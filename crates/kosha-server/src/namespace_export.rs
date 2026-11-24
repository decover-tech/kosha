use crate::{api_error, AppState, ErrorCode};

#[cfg(feature = "s3")]
pub(crate) const URL_EXPIRY_SECONDS: u64 = 900;

#[cfg(any(feature = "s3", test))]
use {
    crate::{namespace_not_found, unindexed::Consistency, ApiError},
    kosha_core::{Document, KoshaError, NamespaceId},
    kosha_segment::{PageSpans, SegmentReader},
    std::io::Write,
};

pub(crate) fn handle(name: &str, tenant: &str, body: &[u8], state: &AppState) -> String {
    if !body.is_empty()
        && !serde_json::from_slice::<serde_json::Value>(body).is_ok_and(|request| {
            request.is_object()
                && request
                    .get("format")
                    .is_none_or(|format| format.as_str() == Some("jsonl"))
        })
    {
        return api_error(ErrorCode::InvalidArgument, "export format must be 'jsonl'");
    }
    #[cfg(feature = "s3")]
    if let Some(storage) = &state.s3_storage {
        let ns = NamespaceId(crate::tenant_namespace(tenant, name));
        let result = (|| {
            state.ensure_writable(&ns)?;
            let lock = state.publication_lock(&ns);
            let _publication = lock.lock().unwrap();
            if state.indexer.manifest_version(&ns).is_none() {
                return Err(namespace_not_found(name));
            }
            let directory = state.data_dir.join(&ns.0);
            std::fs::create_dir_all(&directory).map_err(export_error)?;
            let mut file = tempfile::Builder::new()
                .prefix(".export-")
                .suffix(".jsonl")
                .rand_bytes(24)
                .tempfile_in(directory)
                .map_err(export_error)?;
            let document_count = {
                let mut writer = std::io::BufWriter::new(file.as_file_mut());
                let count = write_documents(state, &ns, name, &mut writer)?;
                writer.flush().map_err(export_error)?;
                count
            };
            let export_name = file
                .path()
                .file_name()
                .expect("tempfile has a name")
                .to_string_lossy();
            // Use the namespace prefix so hard delete also erases exports.
            let path = format!("{}/_exports/{export_name}", ns.0);
            let url = storage
                .upload_export(file.path(), &path)
                .map_err(export_error)?;
            let response = crate::json_ok(&serde_json::json!({
                "namespace": name,
                "document_count": document_count,
                "format": "jsonl",
                "download_url": url,
                "expires_in_seconds": URL_EXPIRY_SECONDS,
            }));
            Ok(crate::with_header(response, "cache-control", "no-store"))
        })();
        return result.unwrap_or_else(|response| response);
    }
    let _ = (name, tenant, state);
    api_error(
        ErrorCode::NotImplemented,
        "namespace export requires S3 storage",
    )
}

#[cfg(any(feature = "s3", test))]
fn export_error(error: impl std::fmt::Display) -> String {
    ApiError::new(
        ErrorCode::Internal,
        format!("namespace export failed: {error}"),
    )
    .redact("namespace export failed")
    .into_raw()
}

#[cfg(any(feature = "s3", test))]
fn write_documents(
    state: &AppState,
    ns: &NamespaceId,
    name: &str,
    mut output: impl Write,
) -> Result<u64, String> {
    let view = state
        .search_view(ns, Consistency::Strong, Some(std::time::Duration::ZERO))?
        .ok_or_else(|| namespace_not_found(name))?;
    if view.fallback.is_some() {
        return Err(api_error(
            ErrorCode::Unavailable,
            "the complete namespace snapshot is unavailable; retry the export",
        ));
    }
    let manifest = &view.search;
    let mut count = 0;
    for entry in &manifest.segments {
        let directory = manifest.segment_dir(&state.data_dir, ns, &entry.segment_id);
        #[cfg(feature = "s3")]
        let mut pins = crate::SegmentPins {
            cache: &state.cache,
            data_dir: &state.data_dir,
            keys: Vec::new(),
        };
        #[cfg(feature = "s3")]
        if !kosha_core::is_tail_segment_id(&entry.segment_id.0) {
            let paths = std::slice::from_ref(&directory);
            pins.hold_all(paths);
            let outcome = state.ensure_segments_local(paths, false);
            if let Some(error) = crate::hydration_error_response(&outcome) {
                return Err(error);
            }
            state.ensure_doc_store_local(
                paths,
                &crate::RequestHydration::unlimited(),
                kosha_segment::segment_uses_doc_vectors,
            );
        }
        let reader = SegmentReader::open_with_options(directory, false).map_err(export_error)?;
        if reader.doc_count() != entry.doc_count {
            return Err(export_error(
                "segment document count differs from the manifest",
            ));
        }
        let deleted = manifest.tombstones.get(&entry.segment_id);
        let mut spans = PageSpans::default();
        for seq in 0..entry.doc_count {
            if deleted.is_some_and(|deleted| deleted.contains(&seq)) {
                continue;
            }
            let record = reader
                .page_doc_record(seq, true, &mut spans)
                .and_then(|record| {
                    record.ok_or_else(|| KoshaError::NotFound(format!("missing document {seq}")))
                })
                .map_err(export_error)?;
            serde_json::to_writer(
                &mut output,
                &Document {
                    id: record.doc_id,
                    fields: record.fields,
                },
            )
            .map_err(export_error)?;
            output.write_all(b"\n").map_err(export_error)?;
            count += 1;
        }
    }
    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_exports_shards_with_all_stored_fields_and_unflushed_upserts() {
        let state = AppState::new(crate::tests::test_dir());
        let ns = NamespaceId("acme/docs".into());
        let request = |tenant: &str, action: &str, body: serde_json::Value| {
            let response = crate::route(
                &format!("POST /v1/namespaces/docs/{action} HTTP/1.1"),
                &std::collections::HashMap::new(),
                &serde_json::to_vec(&body).unwrap(),
                tenant,
                &state,
            );
            assert!(response.starts_with("HTTP/1.1 200"), "{response}");
        };
        request("acme", "shards", serde_json::json!({"shards": 3}));
        request(
            "acme",
            "documents",
            serde_json::json!({"documents": [
                {"id": "keep", "fields": [
                    {"name": "vector", "field_type": "Vector", "value": "[1.0,0.0]"},
                    {"name": "score", "field_type": "Integer", "value": "12"}
                ]},
                {"id": "replace", "fields": [{"name": "title", "field_type": "Text", "value": "old"}]},
                {"id": "delete", "fields": [{"name": "title", "field_type": "Keyword", "value": "gone"}]}
            ]}),
        );
        request("acme", "flush", serde_json::json!({}));
        request(
            "acme",
            "delete",
            serde_json::json!({"filter": {"term": {"title": "gone"}}}),
        );
        request(
            "acme",
            "documents",
            serde_json::json!({"documents": [
                {"id": "replace", "fields": [{"name": "title", "field_type": "Text", "value": "new\nline"}]}
            ]}),
        );
        request(
            "other",
            "documents",
            serde_json::json!({"documents": [
                {"id": "other-tenant", "fields": [{"name": "title", "field_type": "Text", "value": "private"}]}
            ]}),
        );

        let mut bytes = Vec::new();
        assert_eq!(write_documents(&state, &ns, "docs", &mut bytes).unwrap(), 2);
        let mut docs: Vec<Document> = std::str::from_utf8(&bytes)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        docs.sort_by(|a, b| a.id.0.cmp(&b.id.0));
        assert_eq!(docs[0].id.0, "keep");
        assert_eq!(docs[0].fields[0].field_type, kosha_core::FieldType::Vector);
        assert_eq!(
            serde_json::from_str::<Vec<f32>>(&docs[0].fields[0].value).unwrap(),
            [1.0, 0.0]
        );
        assert_eq!(docs[0].fields[1].value, "12");
        assert_eq!(docs[1].id.0, "replace");
        assert_eq!(docs[1].fields[0].value, "new\nline");
        std::fs::remove_dir_all(&state.data_dir).unwrap();
    }
}
