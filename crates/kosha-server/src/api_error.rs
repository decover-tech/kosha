//! One error model for every route (#350, ADR-350 §b).
//!
//! Every non-2xx JSON response carries
//!
//! ```json
//! {"error": "namespace 'docs' not found", "code": "namespace_not_found",
//!  "retryable": false, "request_id": "9f2c4e1ab7d34c08", "details": {...}}
//! ```
//!
//! - `code` is the stable contract ([`ErrorCode`]); `error` is for humans.
//! - `retryable` means the identical request may succeed later and the
//!   server did not apply it. Every retryable response also carries
//!   `retry_after_ms` and a `Retry-After` header (whole seconds, at least 1),
//!   by construction: [`ApiError`] adds both whenever the code is retryable.
//! - `request_id` echoes `X-Request-Id`, which every response carries.
//!
//! `proto/kosha/v1/errors.proto` describes the body; its `code` enum and
//! `docs/api/errors.md` are both checked against [`ErrorCode::ALL`] by
//! tests here.

use std::cell::RefCell;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// The contract revision (`info.version` of the checked-in OpenAPI
/// documents), sent as `Kosha-Api-Version` on every response. A test keeps
/// it equal to the spec.
pub(crate) const API_VERSION: &str = "0.10.0";

/// The delay a retryable error advertises when its call site knows no
/// better one.
pub(crate) const DEFAULT_RETRY_AFTER: Duration = Duration::from_secs(1);

/// A stable, machine-readable error code. Adding a code is an additive
/// change; renaming one, or changing its status or retryability, is not.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum ErrorCode {
    InvalidArgument,
    InvalidQuery,
    Unauthenticated,
    PaymentRequired,
    PermissionDenied,
    NamespaceQuotaExceeded,
    DocQuotaExceeded,
    VectorDimsExceeded,
    NotFound,
    NamespaceNotFound,
    MethodNotAllowed,
    RequestTimeout,
    AlreadyExists,
    Conflict,
    WriteConflict,
    LegacyRouteDisabled,
    PayloadTooLarge,
    TenantQpsExceeded,
    TenantConcurrencyExceeded,
    TenantWriteRateExceeded,
    TenantQueueFull,
    Overloaded,
    Internal,
    NotImplemented,
    UpstreamError,
    Unavailable,
}

impl ErrorCode {
    /// Every code, in the order docs/api/errors.md lists them.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) const ALL: [ErrorCode; 26] = [
        Self::InvalidArgument,
        Self::InvalidQuery,
        Self::Unauthenticated,
        Self::PaymentRequired,
        Self::PermissionDenied,
        Self::NamespaceQuotaExceeded,
        Self::DocQuotaExceeded,
        Self::VectorDimsExceeded,
        Self::NotFound,
        Self::NamespaceNotFound,
        Self::MethodNotAllowed,
        Self::RequestTimeout,
        Self::AlreadyExists,
        Self::Conflict,
        Self::WriteConflict,
        Self::LegacyRouteDisabled,
        Self::PayloadTooLarge,
        Self::TenantQpsExceeded,
        Self::TenantConcurrencyExceeded,
        Self::TenantWriteRateExceeded,
        Self::TenantQueueFull,
        Self::Overloaded,
        Self::Internal,
        Self::NotImplemented,
        Self::UpstreamError,
        Self::Unavailable,
    ];

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::InvalidArgument => "invalid_argument",
            Self::InvalidQuery => "invalid_query",
            Self::Unauthenticated => "unauthenticated",
            Self::PaymentRequired => "payment_required",
            Self::PermissionDenied => "permission_denied",
            Self::NamespaceQuotaExceeded => "namespace_quota_exceeded",
            Self::DocQuotaExceeded => "doc_quota_exceeded",
            Self::VectorDimsExceeded => "vector_dims_exceeded",
            Self::NotFound => "not_found",
            Self::NamespaceNotFound => "namespace_not_found",
            Self::MethodNotAllowed => "method_not_allowed",
            Self::RequestTimeout => "request_timeout",
            Self::AlreadyExists => "already_exists",
            Self::Conflict => "conflict",
            Self::WriteConflict => "write_conflict",
            Self::LegacyRouteDisabled => "legacy_route_disabled",
            Self::PayloadTooLarge => "payload_too_large",
            Self::TenantQpsExceeded => "tenant_qps_exceeded",
            Self::TenantConcurrencyExceeded => "tenant_concurrency_exceeded",
            Self::TenantWriteRateExceeded => "tenant_write_rate_exceeded",
            Self::TenantQueueFull => "tenant_queue_full",
            Self::Overloaded => "overloaded",
            Self::Internal => "internal",
            Self::NotImplemented => "not_implemented",
            Self::UpstreamError => "upstream_error",
            Self::Unavailable => "unavailable",
        }
    }

    pub(crate) fn status(self) -> u16 {
        match self {
            Self::InvalidArgument | Self::InvalidQuery => 400,
            Self::Unauthenticated => 401,
            Self::PaymentRequired => 402,
            Self::PermissionDenied
            | Self::NamespaceQuotaExceeded
            | Self::DocQuotaExceeded
            | Self::VectorDimsExceeded => 403,
            Self::NotFound | Self::NamespaceNotFound => 404,
            Self::MethodNotAllowed => 405,
            Self::RequestTimeout => 408,
            Self::AlreadyExists | Self::Conflict | Self::WriteConflict => 409,
            Self::LegacyRouteDisabled => 410,
            Self::PayloadTooLarge => 413,
            Self::TenantQpsExceeded
            | Self::TenantConcurrencyExceeded
            | Self::TenantWriteRateExceeded
            | Self::TenantQueueFull
            | Self::Overloaded => 429,
            Self::Internal => 500,
            Self::NotImplemented => 501,
            Self::UpstreamError => 502,
            Self::Unavailable => 503,
        }
    }

    /// Whether the identical request may succeed later.
    pub(crate) fn retryable(self) -> bool {
        matches!(
            self,
            Self::RequestTimeout
                | Self::WriteConflict
                | Self::TenantQpsExceeded
                | Self::TenantConcurrencyExceeded
                | Self::TenantWriteRateExceeded
                | Self::TenantQueueFull
                | Self::Overloaded
                | Self::UpstreamError
                | Self::Unavailable
        )
    }

    /// One line for docs/api/errors.md.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn description(self) -> &'static str {
        match self {
            Self::InvalidArgument => {
                "Malformed request: invalid JSON, a missing or invalid namespace, a bad parameter (`details.field` when known)."
            }
            Self::InvalidQuery => "A search or filter the engine cannot run as written.",
            Self::Unauthenticated => {
                "No API key, or an unknown one. The response carries `WWW-Authenticate`."
            }
            Self::PaymentRequired => {
                "Billing (#349) refuses this operation for the tenant's account standing."
            }
            Self::PermissionDenied => {
                "The key may not do this: the admin or internal credential is required, or a scoped key lacks the scope or namespace."
            }
            Self::NamespaceQuotaExceeded => "The tenant is at its namespace quota.",
            Self::DocQuotaExceeded => "The namespace is at its document quota.",
            Self::VectorDimsExceeded => "A vector is wider than the tenant's limit.",
            Self::NotFound => "No such route, branch, job or record.",
            Self::NamespaceNotFound => "The namespace does not exist.",
            Self::MethodNotAllowed => {
                "The path exists but not with this method. The `Allow` header lists the methods it takes."
            }
            Self::RequestTimeout => "The request body did not arrive in time.",
            Self::AlreadyExists => "The namespace (or other named thing) already exists.",
            Self::Conflict => {
                "A precondition does not hold, e.g. deleting a namespace that is not a branch."
            }
            Self::WriteConflict => {
                "A concurrent writer published first. Retrying the identical request is safe."
            }
            Self::LegacyRouteDisabled => {
                "An unversioned legacy route that is off for this tenant. `details.replacement` names the /v1 route."
            }
            Self::PayloadTooLarge => {
                "The body exceeds the pod limit (`details.scope: \"pod\"`) or the tenant's (`\"tenant\"`)."
            }
            Self::TenantQpsExceeded => "The tenant's query rate limit.",
            Self::TenantConcurrencyExceeded => "The tenant's concurrent query limit.",
            Self::TenantWriteRateExceeded => "The tenant's write rate limit.",
            Self::TenantQueueFull => "The tenant's share of the search queue is full.",
            Self::Overloaded => {
                "The pod shed the request (scoring queue, memory, hydration); `details.reason` says which."
            }
            Self::Internal => {
                "An unexpected server error. Quote `request_id` when reporting it."
            }
            Self::NotImplemented => {
                "This server was built or configured without the feature (an embedding provider, s3, postgres)."
            }
            Self::UpstreamError => "A provider the server called (embedding, rerank) failed.",
            Self::Unavailable => {
                "A dependency is unavailable (`details.reason`: control_store, hydration_failed, writer_lease, ingest_unreachable, ...)."
            }
        }
    }

    /// The code for a status from a module's own error type (org admin,
    /// API-key admin, embedding providers), which carries a status rather
    /// than a code. Every status the server sends maps to exactly one code.
    pub(crate) fn from_status(status: u16) -> Self {
        match status {
            400 | 422 => Self::InvalidArgument,
            401 => Self::Unauthenticated,
            402 => Self::PaymentRequired,
            403 => Self::PermissionDenied,
            404 => Self::NotFound,
            405 => Self::MethodNotAllowed,
            408 => Self::RequestTimeout,
            409 => Self::Conflict,
            410 => Self::LegacyRouteDisabled,
            413 => Self::PayloadTooLarge,
            429 => Self::Overloaded,
            501 => Self::NotImplemented,
            502 | 504 => Self::UpstreamError,
            503 => Self::Unavailable,
            _ => Self::Internal,
        }
    }
}

/// The HTTP reason phrase for a status the server sends.
pub(crate) fn reason_phrase(status: u16) -> &'static str {
    match status {
        200 => "OK",
        201 => "Created",
        202 => "Accepted",
        204 => "No Content",
        400 => "Bad Request",
        401 => "Unauthorized",
        402 => "Payment Required",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        408 => "Request Timeout",
        409 => "Conflict",
        410 => "Gone",
        413 => "Payload Too Large",
        422 => "Unprocessable Entity",
        429 => "Too Many Requests",
        500 => "Internal Server Error",
        501 => "Not Implemented",
        502 => "Bad Gateway",
        503 => "Service Unavailable",
        504 => "Gateway Timeout",
        _ => "Unknown",
    }
}

// ─── Request ids ─────────────────────────────────────────────────────────────

thread_local! {
    /// The id of the request this handler thread is serving.
    static REQUEST_ID: RefCell<String> = const { RefCell::new(String::new()) };
}

/// A caller's `X-Request-Id`, kept to ASCII letters, digits and `-`, at most
/// 64 characters; empty if nothing is left.
pub(crate) fn sanitize_request_id(raw: &str) -> String {
    raw.chars()
        .filter(|c| c.is_ascii_alphanumeric() || *c == '-')
        .take(64)
        .collect()
}

/// A fresh 16-hex-character id: the time, the process and a counter mixed,
/// unique within a process and very unlikely to repeat across pods.
pub(crate) fn new_request_id() -> String {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let n = NEXT.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos() as u64);
    let pid = u64::from(std::process::id());
    let mixed = nanos.rotate_left(17) ^ pid.rotate_left(48) ^ n.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    format!("{mixed:016x}")
}

/// Start serving a request on this thread: its id is the caller's
/// `X-Request-Id` (sanitized) or a fresh one. Returns it.
pub(crate) fn begin_request(header: Option<&str>) -> String {
    let id = header
        .map(sanitize_request_id)
        .filter(|id| !id.is_empty())
        .unwrap_or_else(new_request_id);
    REQUEST_ID.with(|r| *r.borrow_mut() = id.clone());
    id
}

/// The id of the request this thread is serving, or a fresh one outside a
/// request (unit tests calling a handler directly).
pub(crate) fn current_request_id() -> String {
    let id = REQUEST_ID.with(|r| r.borrow().clone());
    if id.is_empty() {
        new_request_id()
    } else {
        id
    }
}

/// `" rid=<id>"` for log lines, or empty outside a request.
pub(crate) fn log_rid() -> String {
    REQUEST_ID.with(|r| {
        let id = r.borrow();
        if id.is_empty() {
            String::new()
        } else {
            format!(" rid={id}")
        }
    })
}

// ─── Building error responses ────────────────────────────────────────────────

/// One error response, built up and then rendered.
#[derive(Debug, Clone)]
pub(crate) struct ApiError {
    code: ErrorCode,
    message: String,
    details: serde_json::Map<String, serde_json::Value>,
    retry_after: Option<Duration>,
    /// Deprecated top-level fields kept for old clients (`reason`, `limit`).
    aliases: Vec<(&'static str, serde_json::Value)>,
    headers: Vec<(String, String)>,
}

impl ApiError {
    pub(crate) fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            details: serde_json::Map::new(),
            retry_after: None,
            aliases: Vec::new(),
            headers: Vec::new(),
        }
    }

    /// Add one `details` key.
    pub(crate) fn detail(mut self, key: &str, value: impl Into<serde_json::Value>) -> Self {
        self.details.insert(key.to_string(), value.into());
        self
    }

    /// The delay to advertise. Ignored for a code that is not retryable:
    /// only a retryable response tells the client when to retry.
    pub(crate) fn retry_after(mut self, delay: Duration) -> Self {
        self.retry_after = Some(delay);
        self
    }

    /// A deprecated top-level field (ADR-350 §b "Deprecated aliases").
    pub(crate) fn alias(mut self, key: &'static str, value: impl Into<serde_json::Value>) -> Self {
        self.aliases.push((key, value.into()));
        self
    }

    pub(crate) fn header(mut self, name: &str, value: &str) -> Self {
        self.headers
            .push((name.to_ascii_lowercase(), value.to_string()));
        self
    }

    pub(crate) fn status(&self) -> u16 {
        self.code.status()
    }

    fn retry_after_ms(&self) -> Option<u64> {
        self.code
            .retryable()
            .then(|| self.retry_after.unwrap_or(DEFAULT_RETRY_AFTER))
            .map(|d| (d.as_millis() as u64).max(1))
    }

    /// The JSON body.
    pub(crate) fn body(&self, request_id: &str) -> serde_json::Value {
        let mut body = serde_json::Map::new();
        body.insert("error".into(), self.message.clone().into());
        body.insert("code".into(), self.code.as_str().into());
        body.insert("retryable".into(), self.code.retryable().into());
        if let Some(ms) = self.retry_after_ms() {
            body.insert("retry_after_ms".into(), ms.into());
        }
        body.insert("request_id".into(), request_id.into());
        if !self.details.is_empty() {
            body.insert("details".into(), self.details.clone().into());
        }
        for (key, value) in &self.aliases {
            body.insert((*key).into(), value.clone());
        }
        serde_json::Value::Object(body)
    }

    /// Every header the response carries besides framing: content type,
    /// `Retry-After` when retryable, `X-Request-Id`, `Kosha-Api-Version`,
    /// and any the call site added.
    pub(crate) fn response_headers(&self, request_id: &str) -> Vec<(String, String)> {
        let mut headers = vec![("content-type".to_string(), "application/json".to_string())];
        if let Some(ms) = self.retry_after_ms() {
            headers.push(("retry-after".into(), ms.div_ceil(1000).max(1).to_string()));
        }
        headers.push(("x-request-id".into(), request_id.to_string()));
        headers.push(("kosha-api-version".into(), API_VERSION.into()));
        headers.extend(self.headers.iter().cloned());
        headers
    }

    /// The complete raw HTTP/1.1 response the `String`-returning handlers
    /// in `main.rs` produce. Logs the failure like every error response.
    pub(crate) fn into_raw(self) -> String {
        let request_id = current_request_id();
        let status = self.code.status();
        eprintln!(
            "WARN: request failed: status={status} code={} {}{}",
            self.code.as_str(),
            self.message,
            log_rid()
        );
        let body = self.body(&request_id).to_string();
        let mut out = format!("HTTP/1.1 {status} {}\r\n", reason_phrase(status));
        for (name, value) in self.response_headers(&request_id) {
            out.push_str(&format!("{name}: {value}\r\n"));
        }
        out.push_str(&format!("content-length: {}\r\n\r\n{body}", body.len()));
        out
    }
}

/// The raw response for `code` with `message` and nothing else.
pub(crate) fn api_error(code: ErrorCode, message: &str) -> String {
    ApiError::new(code, message).into_raw()
}

/// The raw response for a module error that carries a status, not a code.
pub(crate) fn api_error_status(status: u16, message: &str) -> String {
    api_error(ErrorCode::from_status(status), message)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parts(raw: &str) -> (u16, Vec<(String, String)>, serde_json::Value) {
        let (head, body) = raw.split_once("\r\n\r\n").unwrap();
        let mut lines = head.split("\r\n");
        let status = lines.next().unwrap()[9..12].parse().unwrap();
        let headers = lines
            .map(|l| {
                let (k, v) = l.split_once(": ").unwrap();
                (k.to_string(), v.to_string())
            })
            .collect();
        (status, headers, serde_json::from_str(body).unwrap())
    }

    fn header<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
        headers
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
    }

    #[test]
    fn every_code_has_one_status_and_a_stable_name() {
        let mut names = std::collections::HashSet::new();
        for code in ErrorCode::ALL {
            assert!(names.insert(code.as_str()), "{code:?} repeats a name");
            assert!(code
                .as_str()
                .chars()
                .all(|c| c.is_ascii_lowercase() || c == '_'));
            assert!((400..600).contains(&code.status()));
            assert!(!code.description().is_empty());
        }
    }

    /// Plan acceptance 6: each body has `error`, `code`, `retryable` and
    /// `request_id`; every retryable response carries `Retry-After` and
    /// `retry_after_ms`, and only those do.
    #[test]
    fn every_code_renders_the_contract() {
        begin_request(Some("rid-under-test"));
        for code in ErrorCode::ALL {
            let (status, headers, body) = parts(&api_error(code, "boom"));
            assert_eq!(status, code.status(), "{code:?}");
            assert_eq!(body["error"], "boom");
            assert_eq!(body["code"], code.as_str());
            assert_eq!(body["retryable"], code.retryable());
            assert_eq!(body["request_id"], "rid-under-test");
            assert_eq!(header(&headers, "x-request-id"), Some("rid-under-test"));
            assert_eq!(header(&headers, "kosha-api-version"), Some(API_VERSION));
            if code.retryable() {
                assert_eq!(header(&headers, "retry-after"), Some("1"), "{code:?}");
                assert_eq!(body["retry_after_ms"], 1000);
            } else {
                assert_eq!(header(&headers, "retry-after"), None, "{code:?}");
                assert!(body.get("retry_after_ms").is_none());
            }
        }
        begin_request(None);
    }

    #[test]
    fn retry_after_rounds_up_to_whole_seconds() {
        let raw = ApiError::new(ErrorCode::Unavailable, "lease")
            .retry_after(Duration::from_millis(2_500))
            .into_raw();
        let (_, headers, body) = parts(&raw);
        assert_eq!(header(&headers, "retry-after"), Some("3"));
        assert_eq!(body["retry_after_ms"], 2500);
        // A non-retryable code never advertises a delay.
        let raw = ApiError::new(ErrorCode::DocQuotaExceeded, "full")
            .retry_after(Duration::from_secs(60))
            .into_raw();
        let (_, headers, body) = parts(&raw);
        assert_eq!(header(&headers, "retry-after"), None);
        assert!(body.get("retry_after_ms").is_none());
    }

    #[test]
    fn request_ids_are_sanitized_or_generated() {
        assert_eq!(begin_request(Some("abc-123\r\nx: y")), "abc-123xy");
        assert_eq!(current_request_id(), "abc-123xy");
        let generated = begin_request(Some("!!!"));
        assert_eq!(generated.len(), 16);
        assert_ne!(new_request_id(), new_request_id());
        begin_request(None);
    }

    #[test]
    fn from_status_covers_every_status_a_code_has() {
        for code in ErrorCode::ALL {
            assert_eq!(
                ErrorCode::from_status(code.status()).status(),
                code.status(),
                "{code:?}"
            );
        }
    }

    const PUBLIC_SPEC: &str = include_str!("../../../api/openapi/v1/kosha.openapi.json");
    const ADMIN_SPEC: &str = include_str!("../../../api/openapi/v1/kosha-admin.openapi.json");

    /// `ErrorResponse.code` in both documents enumerates exactly
    /// [`ErrorCode::ALL`], and `info.version` is [`API_VERSION`].
    #[test]
    fn error_codes_match_spec() {
        let want: Vec<&str> = ErrorCode::ALL.iter().map(|c| c.as_str()).collect();
        for spec in [PUBLIC_SPEC, ADMIN_SPEC] {
            let doc: serde_json::Value = serde_json::from_str(spec).unwrap();
            assert_eq!(doc["info"]["version"], API_VERSION);
            let codes =
                &doc["components"]["schemas"]["v1ErrorResponse"]["properties"]["code"]["enum"];
            assert_eq!(
                codes,
                &serde_json::json!(want),
                "proto/kosha/v1/errors.proto ErrorResponse.code enum differs from ErrorCode::ALL"
            );
        }
    }

    /// `docs/api/errors.md` is generated from [`ErrorCode`]:
    /// `UPDATE_GOLDEN=1 cargo test -p kosha-server error_docs` rewrites it.
    #[test]
    fn error_docs() {
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../../docs/api/errors.md");
        let want = render_error_docs();
        if std::env::var_os("UPDATE_GOLDEN").is_some() {
            std::fs::write(path, &want).unwrap();
            return;
        }
        let got = std::fs::read_to_string(path).unwrap_or_default();
        assert!(
            got == want,
            "docs/api/errors.md is stale: run `UPDATE_GOLDEN=1 cargo test -p kosha-server error_docs`"
        );
    }

    fn render_error_docs() -> String {
        let mut out = String::from(
            "<!-- Generated by `UPDATE_GOLDEN=1 cargo test -p kosha-server error_docs` \
             from crates/kosha-server/src/api_error.rs. Do not edit. -->\n\n\
             # Errors\n\n\
             Every non-2xx JSON response from the Kosha API, on every route, has this body \
             (`ErrorResponse` in `api/openapi/v1/kosha.openapi.json`):\n\n\
             ```json\n\
             {\"error\": \"namespace 'docs' not found\", \"code\": \"namespace_not_found\",\n \
             \"retryable\": false, \"request_id\": \"9f2c4e1ab7d34c08\", \"details\": {}}\n\
             ```\n\n\
             | field | meaning |\n|---|---|\n\
             | `error` | Human-readable. Not stable: never parse it. |\n\
             | `code` | The stable machine contract (table below). |\n\
             | `retryable` | The identical request may succeed later, and the server did not apply it. A forwarded write whose outcome is unknown says so in `details.outcome: \"unknown\"`. |\n\
             | `retry_after_ms` | Present exactly when `retryable` is true. The `Retry-After` header carries the same delay in whole seconds (at least 1). |\n\
             | `request_id` | Equals the `X-Request-Id` response header (the caller's, sanitized, or a generated one). Quote it in bug reports. |\n\
             | `details` | Code-specific; keys are only ever added. |\n\
             | `reason`, `limit` | Deprecated aliases on tenant-limit rejections (`reason` is the pre-0.10 reason string). |\n\n\
             Every response, success or error, carries `X-Request-Id` and \
             `Kosha-Api-Version` (the contract revision).\n\n\
             ## Client rules\n\n\
             - Branch on `code`, never on `error`.\n\
             - Retry only when `retryable` is true, after `Retry-After` (with jitter); \
             give up after a bounded number of attempts.\n\
             - Treat an unknown `code` by its HTTP status and `retryable`: new codes are \
             added without a new API version.\n\n\
             ## Codes\n\n\
             | code | HTTP | retryable | meaning |\n|---|---|---|---|\n",
        );
        for code in ErrorCode::ALL {
            out.push_str(&format!(
                "| `{}` | {} | {} | {} |\n",
                code.as_str(),
                code.status(),
                if code.retryable() { "yes" } else { "no" },
                code.description()
            ));
        }
        out
    }
}
