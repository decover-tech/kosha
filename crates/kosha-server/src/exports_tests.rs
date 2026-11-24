//! Tests for namespace exports (#347).

use super::*;
use crate::tests::test_dir;
use std::io::Read as _;
use std::net::TcpListener as StdListener;

fn enabled(state: &AppState) {
    state.exports.configure(ExportSettings {
        enabled: true,
        allow_insecure_upload: true,
        ..ExportSettings::default()
    });
}

fn exporting_state() -> AppState {
    let state = AppState::new(test_dir());
    enabled(&state);
    state
}

fn parse(response: &str) -> (u16, serde_json::Value) {
    let status: u16 = response[9..12].parse().unwrap();
    let body = &response[response.find("\r\n\r\n").unwrap() + 4..];
    (
        status,
        serde_json::from_str(body).unwrap_or(serde_json::Value::Null),
    )
}

fn call(
    state: &AppState,
    tenant: &str,
    line: &str,
    body: serde_json::Value,
) -> (u16, serde_json::Value) {
    parse(&crate::route(
        &format!("{line} HTTP/1.1\r\n"),
        &HashMap::new(),
        &serde_json::to_vec(&body).unwrap(),
        tenant,
        state,
    ))
}

fn index(state: &AppState, tenant: &str, ns: &str, ids: &[&str]) {
    let documents: Vec<_> = ids
        .iter()
        .map(|id| {
            serde_json::json!({"id": id, "fields": [
                {"name": "body", "field_type": "Text", "value": format!("common {id}")},
                {"name": "tag", "field_type": "Keyword", "value": id},
                {"name": "emb", "field_type": "Vector", "value": "[1.0, 0.0, 0.5]"}]})
        })
        .collect();
    let (status, body) = call(
        state,
        tenant,
        &format!("POST /v1/namespaces/{ns}/documents"),
        serde_json::json!({ "documents": documents }),
    );
    assert_eq!(status, 200, "{body}");
    let (status, body) = call(
        state,
        tenant,
        &format!("POST /v1/namespaces/{ns}/flush"),
        serde_json::json!({}),
    );
    assert_eq!(status, 200, "{body}");
}

fn delete_doc(state: &AppState, tenant: &str, ns: &str, id: &str) {
    let (status, body) = call(
        state,
        tenant,
        &format!("POST /v1/namespaces/{ns}/delete"),
        serde_json::json!({"filter": {"term": {"tag": id}}}),
    );
    assert_eq!(status, 200, "{body}");
}

/// Start an export; units tests run it inline, so it is finished.
fn export(state: &AppState, tenant: &str, ns: &str, body: serde_json::Value) -> serde_json::Value {
    let (status, created) = call(
        state,
        tenant,
        &format!("POST /v1/namespaces/{ns}/exports"),
        body,
    );
    assert_eq!(status, 202, "{created}");
    let id = created["id"].as_str().unwrap().to_string();
    let (status, record) = call(
        state,
        tenant,
        &format!("GET /v1/namespaces/{ns}/exports/{id}"),
        serde_json::json!(null),
    );
    assert_eq!(status, 200, "{record}");
    record
}

fn gunzip_lines(bytes: &[u8]) -> Vec<serde_json::Value> {
    let mut text = String::new();
    flate2::read::MultiGzDecoder::new(bytes)
        .read_to_string(&mut text)
        .unwrap();
    text.lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

fn download(state: &AppState, tenant: &str, ns: &str, id: &str, part: usize) -> (u16, Vec<u8>) {
    let mut response = download_response(
        &format!("GET /v1/namespaces/{ns}/exports/{id}/download?part={part} HTTP/1.1\r\n"),
        tenant,
        state,
    );
    (response.status(), response.body_bytes())
}

fn ids_of(lines: &[serde_json::Value]) -> Vec<String> {
    let mut ids: Vec<String> = lines
        .iter()
        .map(|line| line["id"].as_str().unwrap().to_string())
        .collect();
    ids.sort();
    ids
}

#[test]
fn upload_urls_must_be_https_and_public() {
    for (url, why) in [
        ("http://93.184.216.34/x", "https"),
        ("ftp://93.184.216.34/x", "https"),
        ("https://user:pw@93.184.216.34/x", "credentials"),
        ("https://127.0.0.1/x", "not a public address"),
        ("https://10.1.2.3/x", "not a public address"),
        ("https://172.16.0.9/x", "not a public address"),
        ("https://192.168.1.1/x", "not a public address"),
        (
            "https://169.254.169.254/latest/meta-data",
            "not a public address",
        ),
        ("https://100.64.0.1/x", "not a public address"),
        ("https://0.0.0.0/x", "not a public address"),
        ("https://[::1]/x", "not a public address"),
        ("https://[fd00::1]/x", "not a public address"),
        ("https://[fe80::1]/x", "not a public address"),
        ("https://[::ffff:10.0.0.1]/x", "not a public address"),
        ("https://[64:ff9b::a9fe:a9fe]/x", "not a public address"),
        ("not a url", "not a URL"),
    ] {
        let error = validate_upload_url(url, false).unwrap_err();
        assert!(error.contains(why), "{url}: {error}");
    }
    let ok = validate_upload_url(
        "https://93.184.216.34:8443/bucket/key?X-Amz-Signature=s",
        false,
    )
    .unwrap();
    assert_eq!(ok.addr, "93.184.216.34:8443".parse().unwrap());
    assert!(validate_upload_url("https://[2606:2800:220:1::1]/x", false).is_ok());
    // Loopback over http only with the test switch.
    assert!(validate_upload_url("http://127.0.0.1:9/x", true).is_ok());
}

#[test]
fn routes_parse_and_only_create_and_delete_are_writes() {
    assert_eq!(
        parse_route("POST", "/v1/namespaces/docs/exports"),
        Some(ExportRoute::Create("docs".into()))
    );
    assert_eq!(
        parse_route("GET", "/v1/namespaces/docs/exports/exp-1/download?part=2"),
        Some(ExportRoute::Download("docs".into(), "exp-1".into()))
    );
    assert_eq!(
        parse_route("GET", "/v1/namespaces/docs/exports/a/b/c"),
        None
    );
    assert_eq!(parse_route("PUT", "/v1/namespaces/docs/exports"), None);
    assert!(is_write_route("POST /v1/namespaces/docs/exports HTTP/1.1"));
    assert!(is_write_route(
        "DELETE /v1/namespaces/docs/exports/x HTTP/1.1"
    ));
    assert!(!is_write_route(
        "GET /v1/namespaces/docs/exports/x HTTP/1.1"
    ));
    assert!(crate::is_write_path(
        "POST /v1/namespaces/docs/exports HTTP/1.1\r\n"
    ));
    assert!(!crate::is_write_path(
        "GET /v1/namespaces/docs/exports HTTP/1.1\r\n"
    ));
}

#[test]
fn exports_are_off_unless_enabled() {
    let state = AppState::new(test_dir());
    let (status, body) = call(
        &state,
        "acme",
        "POST /v1/namespaces/docs/exports",
        serde_json::json!({}),
    );
    assert_eq!(status, 404, "{body}");
    assert!(body["error"].as_str().unwrap().contains("KOSHA_EXPORTS"));
}

#[test]
fn an_export_holds_every_live_document_with_vectors_and_never_leaks_across_tenants() {
    let state = exporting_state();
    index(&state, "acme", "docs", &["a", "b", "c"]);
    delete_doc(&state, "acme", "docs", "b");
    // Written but not flushed: the export flushes first.
    let (status, _) = call(
        &state,
        "acme",
        "POST /v1/namespaces/docs/documents",
        serde_json::json!({"documents": [{"id": "d", "fields": [
            {"name": "body", "field_type": "Text", "value": "late"}]}]}),
    );
    assert_eq!(status, 200);
    // Another tenant, same namespace name.
    index(&state, "other", "docs", &["x"]);

    let record = export(&state, "acme", "docs", serde_json::json!({}));
    assert_eq!(record["state"], "completed", "{record}");
    assert_eq!(record["documents"], 3);
    assert_eq!(record["namespace"], "docs");
    assert!(record.get("pins").is_none() && record.get("upload_url").is_none());
    let id = record["id"].as_str().unwrap();
    let (status, bytes) = download(&state, "acme", "docs", id, 0);
    assert_eq!(status, 200);
    let lines = gunzip_lines(&bytes);
    assert_eq!(ids_of(&lines), vec!["a", "c", "d"]);
    // In the index API's own form, vectors included.
    let a = lines.iter().find(|l| l["id"] == "a").unwrap();
    let fields = a["fields"].as_array().unwrap();
    assert!(fields
        .iter()
        .any(|f| f["name"] == "emb" && f["field_type"] == "Vector"));
    assert!(fields
        .iter()
        .any(|f| f["name"] == "body" && f["value"] == "common a"));

    // Other tenants see nothing of it, under any name.
    for (tenant, ns) in [("other", "docs"), ("acme", "other-ns")] {
        let (status, _) = call(
            &state,
            tenant,
            &format!("GET /v1/namespaces/{ns}/exports/{id}"),
            serde_json::json!(null),
        );
        assert_eq!(status, 404, "{tenant}/{ns}");
        let (status, _) = download(&state, tenant, ns, id, 0);
        assert_eq!(status, 404, "{tenant}/{ns}");
    }
    let (status, listed) = call(
        &state,
        "other",
        "GET /v1/namespaces/docs/exports",
        serde_json::json!(null),
    );
    assert_eq!(status, 200);
    assert!(listed["exports"].as_array().unwrap().is_empty());
    let (status, listed) = call(
        &state,
        "acme",
        "GET /v1/namespaces/docs/exports",
        serde_json::json!(null),
    );
    assert_eq!(status, 200);
    assert_eq!(listed["exports"].as_array().unwrap().len(), 1);

    // Without vectors.
    let lean = export(
        &state,
        "acme",
        "docs",
        serde_json::json!({"include_vectors": false}),
    );
    let (_, bytes) = download(&state, "acme", "docs", lean["id"].as_str().unwrap(), 0);
    for line in gunzip_lines(&bytes) {
        assert!(line["fields"]
            .as_array()
            .unwrap()
            .iter()
            .all(|f| f["field_type"] != "Vector"));
    }
}

#[test]
fn large_exports_split_into_parts_that_concatenate_into_one_stream() {
    let state = exporting_state();
    state.exports.configure(ExportSettings {
        enabled: true,
        part_bytes: 1, // every document closes a part
        ..ExportSettings::default()
    });
    index(&state, "acme", "docs", &["a", "b", "c", "d"]);
    let record = export(&state, "acme", "docs", serde_json::json!({}));
    let parts = record["parts"].as_array().unwrap();
    assert_eq!(parts.len(), 4, "{record}");
    let id = record["id"].as_str().unwrap();
    let mut all = Vec::new();
    for part in 0..parts.len() {
        let (status, bytes) = download(&state, "acme", "docs", id, part);
        assert_eq!(status, 200);
        assert_eq!(gunzip_lines(&bytes).len(), 1);
        all.extend(bytes);
    }
    assert_eq!(ids_of(&gunzip_lines(&all)), vec!["a", "b", "c", "d"]);
    let (status, _) = download(&state, "acme", "docs", id, 9);
    assert_eq!(status, 404);
}

#[test]
fn an_empty_namespace_exports_one_empty_part() {
    let state = exporting_state();
    let (status, _) = call(
        &state,
        "acme",
        "POST /v1/namespaces",
        serde_json::json!({"namespace": "empty"}),
    );
    assert_eq!(status, 200);
    let record = export(&state, "acme", "empty", serde_json::json!({}));
    assert_eq!(record["state"], "completed", "{record}");
    assert_eq!(record["documents"], 0);
    let (status, bytes) = download(&state, "acme", "empty", record["id"].as_str().unwrap(), 0);
    assert_eq!(status, 200);
    assert!(gunzip_lines(&bytes).is_empty());
    let (status, _) = call(
        &state,
        "acme",
        "POST /v1/namespaces/missing/exports",
        serde_json::json!({}),
    );
    assert_eq!(status, 404);
}

#[test]
fn bad_requests_are_refused_before_anything_is_recorded() {
    let state = exporting_state();
    index(&state, "acme", "docs", &["a"]);
    for body in [
        serde_json::json!({"include_vectors": "yes"}),
        serde_json::json!({"upload_url": 7}),
        serde_json::json!({"upload_url": "https://169.254.169.254/latest"}),
        serde_json::json!([1]),
    ] {
        let (status, error) = call(
            &state,
            "acme",
            "POST /v1/namespaces/docs/exports",
            body.clone(),
        );
        assert_eq!(status, 400, "{body}: {error}");
    }
    let listed = state
        .controller
        .lock()
        .unwrap()
        .exports(&ExportFilter::default())
        .unwrap();
    assert!(listed.is_empty());
}

#[test]
fn a_tenant_may_run_only_so_many_exports_at_once() {
    let state = exporting_state();
    state.exports.configure(ExportSettings {
        enabled: true,
        max_active: 1,
        ..ExportSettings::default()
    });
    index(&state, "acme", "docs", &["a"]);
    // An active export another pod holds.
    let ns = NamespaceId("acme/docs".into());
    let manifest = state
        .controller
        .lock()
        .unwrap()
        .manifest_cloned(&ns)
        .unwrap();
    let held = state
        .controller
        .lock()
        .unwrap()
        .create_export(&NewExport {
            tenant_id: "acme".into(),
            namespace: ns.0.clone(),
            include_vectors: true,
            pins: vec![(ns.clone(), manifest)],
            upload_url: None,
            ttl_ms: 1000,
        })
        .unwrap();
    state
        .controller
        .lock()
        .unwrap()
        .claim_export(&held.id, "another-pod", 60_000)
        .unwrap()
        .unwrap();
    let (status, body) = call(
        &state,
        "acme",
        "POST /v1/namespaces/docs/exports",
        serde_json::json!({}),
    );
    assert_eq!(status, 429, "{body}");
}

#[test]
fn pinned_segments_stay_referenced_until_the_export_ends() {
    let state = exporting_state();
    index(&state, "acme", "docs", &["a"]);
    let ns = NamespaceId("acme/docs".into());
    let manifest = state
        .controller
        .lock()
        .unwrap()
        .manifest_cloned(&ns)
        .unwrap();
    let segment = manifest.segments[0].segment_id.clone();
    let rel = format!("{}/{}", ns.0, segment.0);
    let pinned = state
        .controller
        .lock()
        .unwrap()
        .create_export(&NewExport {
            tenant_id: "acme".into(),
            namespace: ns.0.clone(),
            include_vectors: true,
            pins: vec![(ns.clone(), manifest)],
            upload_url: None,
            ttl_ms: 1000,
        })
        .unwrap();
    // A compaction retires the segment from the current manifest...
    index(&state, "acme", "docs", &["b"]);
    let (status, body) = parse(&crate::handle_compact_namespace(
        br#"{"namespace": "acme/docs", "mode": "full"}"#,
        "acme",
        &state,
    ));
    assert_eq!(status, 200, "{body}");
    let current = state
        .controller
        .lock()
        .unwrap()
        .manifest_cloned(&ns)
        .unwrap();
    assert!(current.segments.iter().all(|e| e.segment_id != segment));
    // ...but GC's reference set still holds it while the export is active.
    let refs = state.branch_references().unwrap();
    assert!(refs.holds_segment_dir(&rel));
    // Running the export reads it and drops the pin.
    let finished = state.run_export(&pinned.id).unwrap();
    assert_eq!(finished.state, ExportState::Completed);
    assert_eq!(finished.documents, 1);
    assert!(!state.branch_references().unwrap().holds_segment_dir(&rel));
}

#[test]
fn deleting_the_namespace_or_tenant_deletes_its_exports_and_counts_them() {
    let state = exporting_state();
    index(&state, "acme", "docs", &["a"]);
    index(&state, "acme", "kept", &["k"]);
    let first = export(&state, "acme", "docs", serde_json::json!({}));
    let kept = export(&state, "acme", "kept", serde_json::json!({}));
    let dir = state.data_dir.join(kosha_core::export::export_dir(
        "acme",
        first["id"].as_str().unwrap(),
    ));
    assert!(dir.is_dir());

    let (status, deleted) = call(
        &state,
        "acme",
        "DELETE /v1/namespaces/docs",
        serde_json::json!({}),
    );
    assert_eq!(status, 200, "{deleted}");
    assert_eq!(deleted["deletion"]["exports_deleted"], 1, "{deleted}");
    assert!(!dir.exists(), "the export's files are gone");
    assert!(state
        .controller
        .lock()
        .unwrap()
        .export(first["id"].as_str().unwrap())
        .unwrap()
        .is_none());
    // The other namespace's export is untouched.
    let kept_id = kept["id"].as_str().unwrap();
    assert!(state
        .controller
        .lock()
        .unwrap()
        .export(kept_id)
        .unwrap()
        .is_some());

    // Tenant deletion takes the rest, and the stray prefix.
    let stray = state.data_dir.join("_exports/acme/orphan");
    std::fs::create_dir_all(&stray).unwrap();
    std::fs::write(stray.join("part-00000.jsonl.gz"), b"x").unwrap();
    let response = namespace_deletion::handle_delete_tenant(
        "acme",
        br#"{"confirm": "acme"}"#,
        &state,
        auth::global(),
    );
    let (status, body) = parse(&response);
    assert_eq!(status, 200, "{body}");
    // The namespace's own record counts its export; the stray directory
    // went with the tenant's prefix.
    let kept_record = body["namespaces"][0]["deletion_id"].as_str().unwrap();
    let kept_record = state
        .controller
        .lock()
        .unwrap()
        .deletion(kept_record)
        .unwrap()
        .unwrap();
    assert_eq!(kept_record.exports_deleted, 1);
    assert!(body["deletion"]["state"] == "completed", "{body}");
    assert!(!state.data_dir.join("_exports/acme").exists());
    assert!(state
        .controller
        .lock()
        .unwrap()
        .export(kept_id)
        .unwrap()
        .is_none());
}

#[test]
fn completed_exports_expire_and_lose_their_objects() {
    let state = exporting_state();
    index(&state, "acme", "docs", &["a"]);
    let record = export(&state, "acme", "docs", serde_json::json!({}));
    let id = record["id"].as_str().unwrap();
    let expires = record["expires_at_ms"].as_u64().unwrap();
    assert_eq!(state.run_export_sweep(expires - 1), 0);
    assert_eq!(state.run_export_sweep(expires), 1);
    let after = state
        .controller
        .lock()
        .unwrap()
        .export(id)
        .unwrap()
        .unwrap();
    assert_eq!(after.state, ExportState::Expired);
    assert!(after.parts.is_empty());
    assert!(!state
        .data_dir
        .join(kosha_core::export::export_dir("acme", id))
        .exists());
    let (status, _) = download(&state, "acme", "docs", id, 0);
    assert_eq!(status, 409);
    // Deleting by the owner works the same way.
    let other = export(&state, "acme", "docs", serde_json::json!({}));
    let other_id = other["id"].as_str().unwrap();
    let (status, body) = call(
        &state,
        "acme",
        &format!("DELETE /v1/namespaces/docs/exports/{other_id}"),
        serde_json::json!(null),
    );
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["state"], "expired");
}

#[test]
fn an_export_whose_pod_died_is_claimed_again_and_rerun() {
    let state = exporting_state();
    index(&state, "acme", "docs", &["a", "b"]);
    let ns = NamespaceId("acme/docs".into());
    let manifest = state
        .controller
        .lock()
        .unwrap()
        .manifest_cloned(&ns)
        .unwrap();
    let orphan = state
        .controller
        .lock()
        .unwrap()
        .create_export(&NewExport {
            tenant_id: "acme".into(),
            namespace: ns.0.clone(),
            include_vectors: true,
            pins: vec![(ns.clone(), manifest)],
            upload_url: None,
            ttl_ms: 1000,
        })
        .unwrap();
    // A live claim of another pod is respected...
    state
        .controller
        .lock()
        .unwrap()
        .claim_export(&orphan.id, "dead-pod", 60_000)
        .unwrap()
        .unwrap();
    assert!(state.run_export(&orphan.id).is_none());
    // ...a stale one is taken over.
    std::thread::sleep(Duration::from_millis(5));
    let taken = state
        .controller
        .lock()
        .unwrap()
        .claim_export(&orphan.id, "new-pod", 1)
        .unwrap()
        .unwrap();
    assert_eq!(taken.attempts, 2);
}

#[test]
fn a_customer_upload_url_receives_the_whole_export() {
    let state = exporting_state();
    index(&state, "acme", "docs", &["a", "b"]);
    let listener = StdListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let received = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        let mut raw = Vec::new();
        let mut buf = [0u8; 8192];
        let (head, length) = loop {
            let n = stream.read(&mut buf).unwrap();
            raw.extend_from_slice(&buf[..n]);
            if let Some(at) = raw.windows(4).position(|w| w == b"\r\n\r\n") {
                let head = String::from_utf8_lossy(&raw[..at]).to_string();
                let length: usize = head
                    .lines()
                    .find_map(|l| {
                        let (k, v) = l.split_once(':')?;
                        k.eq_ignore_ascii_case("content-length")
                            .then(|| v.trim().parse().unwrap())
                    })
                    .expect("a content-length, not chunked");
                raw.drain(..at + 4);
                break (head, length);
            }
        };
        while raw.len() < length {
            let n = stream.read(&mut buf).unwrap();
            if n == 0 {
                break;
            }
            raw.extend_from_slice(&buf[..n]);
        }
        use std::io::Write as _;
        stream
            .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n")
            .unwrap();
        (head, raw)
    });
    let url = format!("http://127.0.0.1:{port}/bucket/export.jsonl.gz?X-Amz-Signature=secret");
    let record = export(
        &state,
        "acme",
        "docs",
        serde_json::json!({"upload_url": url}),
    );
    assert_eq!(record["delivery_state"], "delivered", "{record}");
    assert_eq!(record["delivery_host"], format!("127.0.0.1:{port}"));
    assert!(!record.to_string().contains("secret"));
    let (head, body) = received.join().unwrap();
    assert!(
        head.starts_with("PUT /bucket/export.jsonl.gz?X-Amz-Signature=secret"),
        "{head}"
    );
    assert_eq!(ids_of(&gunzip_lines(&body)), vec!["a", "b"]);
    // The stored URL is gone once delivered.
    let stored = state
        .controller
        .lock()
        .unwrap()
        .export(record["id"].as_str().unwrap())
        .unwrap()
        .unwrap();
    assert!(stored.upload_url.is_none());
}

#[test]
fn a_failed_delivery_is_reported_and_the_export_stays_downloadable() {
    let state = exporting_state();
    index(&state, "acme", "docs", &["a"]);
    // Nothing listens there.
    let port = {
        let listener = StdListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap().port()
    };
    let record = export(
        &state,
        "acme",
        "docs",
        serde_json::json!({"upload_url": format!("http://127.0.0.1:{port}/x")}),
    );
    assert_eq!(record["state"], "completed");
    assert_eq!(record["delivery_state"], "failed", "{record}");
    assert!(record["delivery_error"].as_str().is_some());
    let (status, _) = download(&state, "acme", "docs", record["id"].as_str().unwrap(), 0);
    assert_eq!(status, 200);
}

#[test]
fn settings_parse_and_refuse_typos() {
    let lookup = |pairs: &'static [(&'static str, &'static str)]| {
        move |name: &str| {
            pairs
                .iter()
                .find(|(k, _)| *k == name)
                .map(|(_, v)| v.to_string())
        }
    };
    assert!(!ExportSettings::from_lookup(lookup(&[])).unwrap().enabled);
    let on = ExportSettings::from_lookup(lookup(&[
        ("KOSHA_EXPORTS", "on"),
        ("KOSHA_EXPORT_TTL_SECS", "3600"),
        ("KOSHA_EXPORT_PART_BYTES", "2097152"),
        ("KOSHA_EXPORT_PRESIGN_SECS", "99999999"),
    ]))
    .unwrap();
    assert!(on.enabled);
    assert_eq!(on.ttl, Duration::from_secs(3600));
    assert_eq!(on.part_bytes, 2 * 1024 * 1024);
    assert_eq!(on.presign, Duration::from_secs(7 * 24 * 3600));
    assert!(!on.allow_insecure_upload);
    assert!(ExportSettings::from_lookup(lookup(&[("KOSHA_EXPORTS", "yes please")])).is_err());
    assert!(ExportSettings::from_lookup(lookup(&[("KOSHA_EXPORT_PART_BYTES", "10")])).is_err());
}

// ── #347 review fixes ────────────────────────────────────────────────────

/// Object storage whose deletes under `_exports/` can be made to fail.
#[derive(Default)]
struct ExportRemote {
    fail_exports: std::sync::atomic::AtomicBool,
}

impl namespace_deletion::RemotePrefixStore for ExportRemote {
    fn delete_prefix(&self, prefix: &str) -> Result<usize, KoshaError> {
        if prefix.starts_with("_exports") && self.fail_exports.load(Ordering::SeqCst) {
            return Err(KoshaError::NotFound("injected S3 outage".into()));
        }
        Ok(0)
    }

    fn count_prefix(&self, _prefix: &str) -> Result<usize, KoshaError> {
        Ok(0)
    }
}

/// Finding 1: when an export's files cannot be deleted, its row is kept
/// (expired, no pins) so the deletion retry finds it, and the record stays
/// pending until the files are confirmed gone.
#[test]
fn an_export_whose_files_cannot_be_deleted_keeps_its_row_until_they_are() {
    let mut state = AppState::new(test_dir());
    let remote = Arc::new(ExportRemote::default());
    state.remote_prefix_override = Some(remote.clone());
    enabled(&state);
    index(&state, "acme", "docs", &["a"]);
    let record = export(&state, "acme", "docs", serde_json::json!({}));
    let id = record["id"].as_str().unwrap().to_string();

    remote.fail_exports.store(true, Ordering::SeqCst);
    let (status, body) = call(
        &state,
        "acme",
        "DELETE /v1/namespaces/docs",
        serde_json::json!({}),
    );
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["deletion"]["state"], "pending", "{body}");
    assert!(body["deletion"]["last_error"]
        .as_str()
        .unwrap()
        .contains("export"));
    let kept = state
        .controller
        .lock()
        .unwrap()
        .export(&id)
        .unwrap()
        .unwrap();
    assert_eq!(kept.state, ExportState::Expired);
    assert!(kept.pins.is_empty());

    // The retry finds the row, deletes the files, then the row.
    remote.fail_exports.store(false, Ordering::SeqCst);
    let pending = state
        .controller
        .lock()
        .unwrap()
        .deletion(body["deletion"]["id"].as_str().unwrap())
        .unwrap()
        .unwrap();
    let done = state.resume_namespace_deletion(&pending);
    assert!(done.is_completed(), "{done:?}");
    assert_eq!(done.exports_deleted, 1);
    assert!(state
        .controller
        .lock()
        .unwrap()
        .export(&id)
        .unwrap()
        .is_none());
}

/// Finding 2: the claim is refreshed on a timer while a slow delivery runs,
/// so another writer cannot take a live export over; and each attempt
/// writes under its own prefix, so discarding one never touches another.
#[test]
fn a_slow_delivery_keeps_its_claim_and_attempts_never_share_files() {
    let state = exporting_state();
    state.exports.configure(ExportSettings {
        enabled: true,
        allow_insecure_upload: true,
        claim_refresh: Duration::from_millis(50),
        claim_timeout: Duration::from_millis(400),
        ..ExportSettings::default()
    });
    index(&state, "acme", "docs", &["a", "b"]);
    let listener = StdListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let receiver = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        let mut buf = [0u8; 65536];
        let _ = stream.read(&mut buf);
        // Slower than the claim timeout.
        std::thread::sleep(Duration::from_millis(1200));
        use std::io::Write as _;
        let _ = stream.write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n");
    });
    let intruder_won = std::thread::scope(|scope| {
        let intruder = scope.spawn(|| {
            std::thread::sleep(Duration::from_millis(700));
            let active = state
                .controller
                .lock()
                .unwrap()
                .exports(&ExportFilter::active())
                .unwrap();
            active.iter().any(|e| {
                state
                    .controller
                    .lock()
                    .unwrap()
                    .claim_export(&e.id, "intruder", 400)
                    .unwrap()
                    .is_some()
            })
        });
        let record = export(
            &state,
            "acme",
            "docs",
            serde_json::json!({"upload_url": format!("http://127.0.0.1:{port}/x")}),
        );
        assert_eq!(record["state"], "completed", "{record}");
        assert_eq!(record["delivery_state"], "delivered", "{record}");
        intruder.join().unwrap()
    });
    receiver.join().unwrap();
    assert!(!intruder_won, "the claim went stale during the delivery");

    let id = state
        .controller
        .lock()
        .unwrap()
        .exports(&ExportFilter::default())
        .unwrap()[0]
        .id
        .clone();
    let record = state
        .controller
        .lock()
        .unwrap()
        .export(&id)
        .unwrap()
        .unwrap();
    assert!(
        record.parts[0].path.contains("/attempt-1/"),
        "{:?}",
        record.parts
    );
    // An abandoned attempt removes its own prefix only.
    let other = format!("{}/attempt-2", kosha_core::export::export_dir("acme", &id));
    state.discard_run_objects(&record, &other);
    let (status, bytes) = download(&state, "acme", "docs", &id, 0);
    assert_eq!(status, 200);
    assert_eq!(ids_of(&gunzip_lines(&bytes)), vec!["a", "b"]);
}

/// Finding 6: with exports off, active exports are failed (their pins go)
/// and the routes that read or delete exports still answer; and pins are
/// read for every active export, past the first thousand.
#[test]
fn exports_off_releases_pins_and_every_active_pin_is_read() {
    let state = exporting_state();
    index(&state, "acme", "docs", &["a"]);
    let ns = NamespaceId("acme/docs".into());
    let manifest = state
        .controller
        .lock()
        .unwrap()
        .manifest_cloned(&ns)
        .unwrap();
    let rel = format!("{}/{}", ns.0, manifest.segments[0].segment_id.0);
    let other = NamespaceId("acme/other".into());
    for _ in 0..1000 {
        state
            .controller
            .lock()
            .unwrap()
            .create_export(&NewExport {
                tenant_id: "acme".into(),
                namespace: other.0.clone(),
                include_vectors: true,
                pins: vec![(other.clone(), kosha_core::Manifest::empty(1))],
                upload_url: None,
                ttl_ms: 1000,
            })
            .unwrap();
    }
    let newest = state
        .controller
        .lock()
        .unwrap()
        .create_export(&NewExport {
            tenant_id: "acme".into(),
            namespace: ns.0.clone(),
            include_vectors: true,
            pins: vec![(ns.clone(), manifest)],
            upload_url: None,
            ttl_ms: 1000,
        })
        .unwrap();
    let mut refs = kosha_core::branch::References::default();
    state.pin_export_references(&mut refs).unwrap();
    assert!(
        refs.holds_segment_dir(&rel),
        "the 1001st export's pin is read"
    );

    // Turned off: what nothing will run is failed and unpinned.
    state.exports.configure(ExportSettings::default());
    assert_eq!(state.fail_active_exports("exports were turned off"), 1001);
    let mut refs = kosha_core::branch::References::default();
    state.pin_export_references(&mut refs).unwrap();
    assert!(!refs.holds_segment_dir(&rel));
    let (status, body) = call(
        &state,
        "acme",
        &format!("GET /v1/namespaces/docs/exports/{}", newest.id),
        serde_json::json!(null),
    );
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["state"], "failed");
    let (status, body) = call(
        &state,
        "acme",
        &format!("DELETE /v1/namespaces/docs/exports/{}", newest.id),
        serde_json::json!(null),
    );
    assert_eq!(status, 200, "{body}");
    let (status, _) = call(
        &state,
        "acme",
        "POST /v1/namespaces/docs/exports",
        serde_json::json!({}),
    );
    assert_eq!(status, 404, "starting one stays refused");
}

/// Finding 7: the list is newest first and paginated; old failed and
/// expired rows are pruned.
#[test]
fn the_export_list_pages_newest_first_and_old_rows_are_pruned() {
    let state = exporting_state();
    state.exports.configure(ExportSettings {
        enabled: true,
        max_active: 10,
        ..ExportSettings::default()
    });
    index(&state, "acme", "docs", &["a"]);
    let mut ids = Vec::new();
    for _ in 0..3 {
        ids.push(export(&state, "acme", "docs", serde_json::json!({}))["id"].clone());
        std::thread::sleep(Duration::from_millis(3));
    }
    let (status, page) = call(
        &state,
        "acme",
        "GET /v1/namespaces/docs/exports?limit=2",
        serde_json::json!(null),
    );
    assert_eq!(status, 200, "{page}");
    let listed: Vec<serde_json::Value> = page["exports"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["id"].clone())
        .collect();
    assert_eq!(listed, vec![ids[2].clone(), ids[1].clone()]);
    let next = page["next"].as_str().unwrap();
    let (_, rest) = call(
        &state,
        "acme",
        &format!("GET /v1/namespaces/docs/exports?limit=2&before={next}"),
        serde_json::json!(null),
    );
    assert_eq!(rest["exports"][0]["id"], ids[0]);
    assert!(rest["next"].is_null());

    // Expire them all, then let the retention pass.
    let far = unix_millis() + 365 * 24 * 3600 * 1000;
    state.run_export_sweep(far);
    state.run_export_sweep(far + RETENTION.as_millis() as u64 + 1);
    assert!(state
        .controller
        .lock()
        .unwrap()
        .exports(&ExportFilter::default())
        .unwrap()
        .is_empty());
}
