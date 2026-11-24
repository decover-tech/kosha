//! `POST /v1/namespaces/{namespace}/export` (#347): every live document of a
//! namespace, a page at a time, in the shape `POST .../documents` accepts,
//! so a customer can take their data out (or re-index it elsewhere).
//!
//! A page is an ordinary search: `match_all`, sorted by `_id` ascending,
//! continued with `search_after` from the previous page's last id. It goes
//! through the search path, so sharded namespaces, the unindexed tail
//! (`consistency`), deletes and tenant limits behave exactly as for search.
//! Pages are not a snapshot: a document written or deleted during an export
//! appears or not according to where its id falls relative to the cursor.

use crate::api_error::{api_error, ErrorCode};

/// Documents per page when the request names no `limit`.
pub(crate) const DEFAULT_LIMIT: u64 = 1_000;
/// The largest `limit` a page may ask for.
pub(crate) const MAX_LIMIT: u64 = 10_000;

/// A validated export page request.
#[derive(Debug, PartialEq)]
struct ExportPage {
    limit: u64,
    cursor: Option<String>,
    consistency: Option<serde_json::Value>,
}

fn parse_request(body: &[u8]) -> Result<ExportPage, String> {
    let value: serde_json::Value = if body.iter().all(u8::is_ascii_whitespace) {
        serde_json::json!({})
    } else {
        serde_json::from_slice(body).map_err(|e| format!("invalid JSON: {e}"))?
    };
    let obj = value
        .as_object()
        .ok_or_else(|| "export request must be a JSON object".to_string())?;
    for key in obj.keys() {
        if !matches!(
            key.as_str(),
            "namespace" | "limit" | "cursor" | "consistency"
        ) {
            return Err(format!("unknown export field '{key}'"));
        }
    }
    let limit = match obj.get("limit") {
        None | Some(serde_json::Value::Null) => DEFAULT_LIMIT,
        Some(v) => match v.as_u64() {
            Some(n) if (1..=MAX_LIMIT).contains(&n) => n,
            _ => {
                return Err(format!(
                    "'limit' must be an integer in 1..={MAX_LIMIT}, got {v}"
                ))
            }
        },
    };
    let cursor = match obj.get("cursor") {
        None | Some(serde_json::Value::Null) => None,
        Some(serde_json::Value::String(s)) if s.is_empty() => None,
        Some(serde_json::Value::String(s)) => Some(s.clone()),
        Some(other) => return Err(format!("'cursor' must be a string, got {other}")),
    };
    crate::unindexed::Consistency::from_json(obj.get("consistency"))?;
    Ok(ExportPage {
        limit,
        cursor,
        consistency: obj.get("consistency").cloned(),
    })
}

/// The search body that reads one export page.
fn search_body(page: &ExportPage) -> serde_json::Value {
    let mut body = serde_json::json!({
        "query_text": "",
        "filter": {"match_all": {}},
        "sort": [{"_id": {"order": "asc"}}],
        "max_results": page.limit,
        "no_cache": true,
    });
    if let Some(cursor) = &page.cursor {
        body["search_after"] = serde_json::json!([cursor]);
    }
    if let Some(consistency) = &page.consistency {
        body["consistency"] = consistency.clone();
    }
    body
}

/// Turns a search response into an export page; an error response passes
/// through unchanged.
fn export_response(namespace: &str, limit: u64, search: String) -> String {
    if !search.starts_with("HTTP/1.1 200") {
        return search;
    }
    let Some((_, body)) = search.split_once("\r\n\r\n") else {
        return api_error(ErrorCode::Internal, "malformed search response");
    };
    let parsed: serde_json::Value = match serde_json::from_str(body) {
        Ok(v) => v,
        Err(e) => return api_error(ErrorCode::Internal, &format!("search response: {e}")),
    };
    let hits = parsed["results"].as_array().cloned().unwrap_or_default();
    let documents: Vec<serde_json::Value> = hits
        .iter()
        .map(|hit| {
            serde_json::json!({
                "id": hit["doc_id"],
                "fields": hit.get("fields").cloned().unwrap_or_else(|| serde_json::json!([])),
            })
        })
        .collect();
    let next_cursor = if documents.len() as u64 == limit {
        documents.last().map(|d| d["id"].clone())
    } else {
        None
    };
    crate::json_ok(&serde_json::json!({
        "namespace": namespace,
        "documents": documents,
        "next_cursor": next_cursor,
    }))
}

pub(crate) fn handle_export_with_ns(
    namespace: &str,
    tenant: &str,
    body: &[u8],
    state: &crate::AppState,
) -> String {
    let page = match parse_request(body) {
        Ok(page) => page,
        Err(message) => return api_error(ErrorCode::InvalidArgument, &message),
    };
    let search = search_body(&page).to_string();
    let response = crate::handle_search_post_with_ns(namespace, tenant, search.as_bytes(), state);
    export_response(namespace, page.limit, response)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_defaults_and_validation() {
        assert_eq!(
            parse_request(b"").unwrap(),
            ExportPage {
                limit: DEFAULT_LIMIT,
                cursor: None,
                consistency: None
            }
        );
        let page =
            parse_request(br#"{"limit": 2, "cursor": "b", "consistency": "strong"}"#).unwrap();
        assert_eq!(page.limit, 2);
        assert_eq!(page.cursor.as_deref(), Some("b"));
        for bad in [
            &br#"{"limit": 0}"#[..],
            br#"{"limit": 10001}"#,
            br#"{"limit": "5"}"#,
            br#"{"cursor": 3}"#,
            br#"{"consistency": "sometimes"}"#,
            br#"{"query_text": "x"}"#,
            br#"[]"#,
            b"{bad",
        ] {
            assert!(
                parse_request(bad).is_err(),
                "{}",
                String::from_utf8_lossy(bad)
            );
        }
    }

    #[test]
    fn search_body_pages_by_id() {
        let body = search_body(&ExportPage {
            limit: 3,
            cursor: Some("k".into()),
            consistency: None,
        });
        assert_eq!(body["sort"], serde_json::json!([{"_id": {"order": "asc"}}]));
        assert_eq!(body["search_after"], serde_json::json!(["k"]));
        assert_eq!(body["max_results"], 3);
        assert!(body.get("consistency").is_none());
    }

    fn post(state: &crate::AppState, path: &str, body: serde_json::Value) -> serde_json::Value {
        let resp = crate::route(
            &format!("POST {path} HTTP/1.1\r\n"),
            &std::collections::HashMap::new(),
            body.to_string().as_bytes(),
            "t",
            state,
        );
        assert!(resp.starts_with("HTTP/1.1 200"), "{path}: {resp}");
        serde_json::from_str(resp.split_once("\r\n\r\n").unwrap().1).unwrap()
    }

    fn export_all(
        state: &crate::AppState,
        limit: u64,
        consistency: &str,
    ) -> Vec<serde_json::Value> {
        let mut out = Vec::new();
        let mut cursor = serde_json::Value::Null;
        loop {
            let page = post(
                state,
                "/v1/namespaces/docs/export",
                serde_json::json!({"limit": limit, "cursor": cursor, "consistency": consistency}),
            );
            assert_eq!(page["namespace"], "docs");
            let docs = page["documents"].as_array().unwrap();
            assert!(docs.len() as u64 <= limit);
            out.extend(docs.iter().cloned());
            cursor = page["next_cursor"].clone();
            if cursor.is_null() {
                return out;
            }
        }
    }

    fn ids(docs: &[serde_json::Value]) -> Vec<&str> {
        docs.iter().map(|d| d["id"].as_str().unwrap()).collect()
    }

    /// Pages walk every live document once, in id order, with every field,
    /// leave deleted ones out, and re-index into an identical namespace.
    #[test]
    fn export_pages_every_live_document_and_round_trips() {
        let state = crate::AppState::new(crate::tests::test_dir());
        let docs: Vec<serde_json::Value> = ["e", "a", "d", "b", "c"]
            .iter()
            .map(|id| {
                serde_json::json!({"id": id, "fields": [
                    {"name": "title", "field_type": "Text", "value": format!("doc {id}")},
                    {"name": "status", "field_type": "Keyword", "value": if *id == "c" { "closed" } else { "open" }}
                ]})
            })
            .collect();
        post(
            &state,
            "/v1/namespaces/docs/documents",
            serde_json::json!({"documents": docs}),
        );
        // Strong consistency sees acknowledged writes before any flush.
        assert_eq!(
            ids(&export_all(&state, 2, "strong")),
            ["a", "b", "c", "d", "e"]
        );

        post(&state, "/v1/namespaces/docs/flush", serde_json::json!({}));
        post(
            &state,
            "/v1/namespaces/docs/delete",
            serde_json::json!({"filter": {"term": {"status": "closed"}}}),
        );
        post(&state, "/v1/namespaces/docs/flush", serde_json::json!({}));
        for limit in [1, 2, 4, 10] {
            let exported = export_all(&state, limit, "eventual");
            assert_eq!(ids(&exported), ["a", "b", "d", "e"], "limit {limit}");
        }
        let exported = export_all(&state, 10, "eventual");
        let b = &exported[1];
        let mut fields: Vec<(String, String)> = b["fields"]
            .as_array()
            .unwrap()
            .iter()
            .map(|f| {
                (
                    f["name"].as_str().unwrap().to_string(),
                    f["value"].as_str().unwrap().to_string(),
                )
            })
            .collect();
        fields.sort();
        assert_eq!(
            fields,
            [
                ("status".to_string(), "open".to_string()),
                ("title".to_string(), "doc b".to_string())
            ]
        );

        // The pages are valid `documents` bodies.
        post(
            &state,
            "/v1/namespaces/copy/documents",
            serde_json::json!({"documents": exported}),
        );
        post(&state, "/v1/namespaces/copy/flush", serde_json::json!({}));
        let copy = post(
            &state,
            "/v1/namespaces/copy/export",
            serde_json::json!({"consistency": "eventual"}),
        );
        assert_eq!(copy["documents"], serde_json::json!(exported));
        assert!(copy["next_cursor"].is_null());
    }

    #[test]
    fn export_errors() {
        let state = crate::AppState::new(crate::tests::test_dir());
        let resp = crate::route(
            "POST /v1/namespaces/docs/export HTTP/1.1\r\n",
            &std::collections::HashMap::new(),
            br#"{"limit": 0}"#,
            "t",
            &state,
        );
        assert!(resp.starts_with("HTTP/1.1 400"), "{resp}");
        assert!(resp.contains("invalid_argument"), "{resp}");
        let missing = crate::route(
            "POST /v1/namespaces/nope/export HTTP/1.1\r\n",
            &std::collections::HashMap::new(),
            b"{}",
            "t",
            &state,
        );
        assert!(missing.starts_with("HTTP/1.1 404"), "{missing}");
    }
}
