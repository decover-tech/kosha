//! Built-in, read-only operator dashboard (#214), behind `KOSHA_DASHBOARD`
//! (default off).
//!
//! Two parts:
//!
//! * **Static assets** under `crates/kosha-server/dashboard/`, compiled into
//!   the binary and served at `GET /dashboard` and `GET /dashboard/*`. They
//!   hold no data, so they are served without a key. Every data call the page
//!   makes goes from the browser to the ordinary authenticated API with the
//!   key the operator pastes in, so tenant scoping is enforced by the same
//!   auth as any other client and the page cannot get around it. Responses
//!   carry a strict `Content-Security-Policy` (`default-src 'self'`) and
//!   `X-Frame-Options: DENY`.
//! * **A recent-query ring per namespace**: the last [`RING_CAPACITY`]
//!   searches (total, hydrate and score time, hits, whether kNN degraded),
//!   for at most [`MAX_TRACKED_NAMESPACES`] namespaces (least recently
//!   searched evicted first). It is reported as `recent_queries` in
//!   `GET /v1/namespaces/{ns}/stats`, only while the dashboard is enabled,
//!   so stats output is unchanged by default. In memory, per process.

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

/// Searches kept per namespace.
pub const RING_CAPACITY: usize = 200;
/// Namespaces with a ring at once; the least recently searched is dropped.
pub const MAX_TRACKED_NAMESPACES: usize = 256;

const INDEX_HTML: &str = include_str!("../dashboard/index.html");
const APP_JS: &str = include_str!("../dashboard/app.js");
const STYLE_CSS: &str = include_str!("../dashboard/style.css");

/// Sent on every `/dashboard` response, including 404s.
const SECURITY_HEADERS: &str = "content-security-policy: default-src 'self'; \
     frame-ancestors 'none'; base-uri 'none'; form-action 'none'\r\n\
     x-frame-options: DENY\r\n\
     x-content-type-options: nosniff\r\n\
     referrer-policy: no-referrer\r\n\
     cache-control: no-cache\r\n";

/// `KOSHA_DASHBOARD`: `on`, `true`, `1`, `yes` or `enabled` (any case) turn
/// it on; anything else, or unset, leaves it off.
fn flag_enabled(value: Option<&str>) -> bool {
    value.is_some_and(|v| {
        matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "on" | "true" | "1" | "yes" | "enabled"
        )
    })
}

/// The per-phase timing of one search, in milliseconds. Returned as
/// `timings` on a search response that asked for it (`"timings": true`)
/// while the dashboard is on.
#[derive(Debug, Clone, Copy, Default, serde::Serialize)]
pub struct SearchTimings {
    pub total_ms: f64,
    pub hydrate_ms: f64,
    pub queue_ms: f64,
    pub admit_ms: f64,
    pub bm25_stats_ms: f64,
    pub score_ms: f64,
    pub materialize_ms: f64,
    /// Served from the whole-response result cache: no phase ran.
    pub cached: bool,
    /// Sequential S3 round trips this search made (scoring scaffolding,
    /// postings, kNN postings, page documents). Same figure as `s3_rounds`
    /// on the `search timing:` log line.
    pub s3_rounds: u64,
    /// Bytes this search pulled from S3. Same figure as `s3_bytes` on the
    /// `search timing:` log line.
    pub s3_bytes: u64,
    /// Everything this search read was already local: no S3 request of any
    /// kind, nothing missing or shed, and no kNN segment degraded or BM25
    /// fallback to per-segment statistics. Kosha's counterpart to
    /// turbopuffer's `cache_hit_ratio == 1.0`, which its benchmark polls
    /// before measuring a hot namespace. A result-cache hit is fully cached.
    pub fully_cached: bool,
}

/// One search in a namespace's recent-query ring.
#[derive(Debug, Clone, Copy, serde::Serialize)]
pub struct QuerySample {
    /// Unix milliseconds when the search finished.
    pub at_ms: u64,
    pub total_ms: f64,
    pub hydrate_ms: f64,
    pub score_ms: f64,
    pub hits: usize,
    /// kNN skipped at least one segment (`knn_degraded_segments > 0`).
    pub degraded: bool,
    pub cached: bool,
}

impl QuerySample {
    pub fn new(timings: &SearchTimings, hits: usize, degraded: bool) -> Self {
        Self {
            at_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_or(0, |d| d.as_millis() as u64),
            total_ms: timings.total_ms,
            hydrate_ms: timings.hydrate_ms,
            score_ms: timings.score_ms,
            hits,
            degraded,
            cached: timings.cached,
        }
    }
}

#[derive(Default)]
struct Rings {
    /// Logical clock for LRU eviction: bumped on every record.
    tick: u64,
    by_namespace: HashMap<String, (u64, VecDeque<QuerySample>)>,
}

impl Rings {
    fn record(&mut self, namespace: &str, sample: QuerySample) {
        self.tick += 1;
        let tick = self.tick;
        if let Some((last, ring)) = self.by_namespace.get_mut(namespace) {
            *last = tick;
            if ring.len() == RING_CAPACITY {
                ring.pop_front();
            }
            ring.push_back(sample);
            return;
        }
        if self.by_namespace.len() >= MAX_TRACKED_NAMESPACES {
            // Linear scan over at most MAX_TRACKED_NAMESPACES entries, only
            // when a new namespace arrives at the cap.
            if let Some(oldest) = self
                .by_namespace
                .iter()
                .min_by_key(|(_, (last, _))| *last)
                .map(|(ns, _)| ns.clone())
            {
                self.by_namespace.remove(&oldest);
            }
        }
        let mut ring = VecDeque::with_capacity(RING_CAPACITY);
        ring.push_back(sample);
        self.by_namespace
            .insert(namespace.to_string(), (tick, ring));
    }
}

/// Dashboard switch plus the recent-query rings. One per process, on
/// `AppState`.
pub struct Dashboard {
    enabled: bool,
    rings: Mutex<Rings>,
}

impl Dashboard {
    pub fn new(enabled: bool) -> Self {
        Self {
            enabled,
            rings: Mutex::new(Rings::default()),
        }
    }

    pub fn from_env() -> Self {
        let enabled = flag_enabled(std::env::var("KOSHA_DASHBOARD").ok().as_deref());
        if enabled {
            println!("dashboard: enabled at /dashboard (KOSHA_DASHBOARD)");
        }
        Self::new(enabled)
    }

    pub fn enabled(&self) -> bool {
        self.enabled
    }

    /// Add a finished search to `namespace`'s ring (the tenant-scoped id).
    /// No-op while the dashboard is off.
    pub fn record(&self, namespace: &str, sample: QuerySample) {
        if self.enabled {
            self.rings.lock().unwrap().record(namespace, sample);
        }
    }

    /// `recent_queries` for `GET /v1/namespaces/{ns}/stats`: `None` while
    /// the dashboard is off (the key is then left out), otherwise the
    /// samples oldest first plus a few summary numbers.
    pub fn recent_queries_json(&self, namespace: &str) -> Option<serde_json::Value> {
        if !self.enabled {
            return None;
        }
        let samples: Vec<QuerySample> = self
            .rings
            .lock()
            .unwrap()
            .by_namespace
            .get(namespace)
            .map(|(_, ring)| ring.iter().copied().collect())
            .unwrap_or_default();
        let mut totals: Vec<f64> = samples.iter().map(|s| s.total_ms).collect();
        totals.sort_by(f64::total_cmp);
        let percentile = |p: f64| -> Option<f64> {
            if totals.is_empty() {
                return None;
            }
            let rank = ((p * totals.len() as f64).ceil() as usize).clamp(1, totals.len());
            Some(totals[rank - 1])
        };
        Some(serde_json::json!({
            "capacity": RING_CAPACITY,
            "count": samples.len(),
            "degraded": samples.iter().filter(|s| s.degraded).count(),
            "p50_ms": percentile(0.50),
            "p95_ms": percentile(0.95),
            "max_ms": totals.last(),
            "samples": samples,
        }))
    }
}

/// The response for a `/dashboard` request, or `None` when `request_line`
/// is not one (the request then goes through auth and routing as usual).
/// Answered before authentication: the assets hold no data. While the
/// dashboard is off every `/dashboard` path is a 404.
pub fn static_response(request_line: &str, enabled: bool) -> Option<String> {
    let mut parts = request_line.split_whitespace();
    let method = parts.next()?;
    let target = parts.next()?;
    let path = target.split(['?', '#']).next().unwrap_or(target);
    let rest = if path == "/dashboard" {
        ""
    } else {
        path.strip_prefix("/dashboard/")?
    };
    if !enabled || method != "GET" {
        return Some(not_found());
    }
    let (content_type, body) = match rest {
        "" | "index.html" => ("text/html; charset=utf-8", INDEX_HTML),
        "app.js" => ("text/javascript; charset=utf-8", APP_JS),
        "style.css" => ("text/css; charset=utf-8", STYLE_CSS),
        _ => return Some(not_found()),
    };
    Some(response("200 OK", content_type, body))
}

fn not_found() -> String {
    response("404 Not Found", "text/plain; charset=utf-8", "not found\n")
}

fn response(status: &str, content_type: &str, body: &str) -> String {
    format!(
        "HTTP/1.1 {status}\r\ncontent-length: {}\r\ncontent-type: {content_type}\r\n\
         {SECURITY_HEADERS}\r\n{body}",
        body.len()
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn header<'a>(response: &'a str, name: &str) -> Option<&'a str> {
        let head = response.split("\r\n\r\n").next()?;
        head.lines().skip(1).find_map(|line| {
            let (k, v) = line.split_once(':')?;
            k.trim().eq_ignore_ascii_case(name).then(|| v.trim())
        })
    }

    fn sample(total_ms: f64, degraded: bool) -> QuerySample {
        QuerySample::new(
            &SearchTimings {
                total_ms,
                hydrate_ms: 1.0,
                score_ms: 2.0,
                ..Default::default()
            },
            3,
            degraded,
        )
    }

    #[test]
    fn flag_parsing_is_default_off() {
        assert!(!flag_enabled(None));
        for off in ["", "off", "false", "0", "no", "typo"] {
            assert!(!flag_enabled(Some(off)), "{off}");
        }
        for on in ["on", "ON", " true ", "1", "yes", "enabled"] {
            assert!(flag_enabled(Some(on)), "{on}");
        }
    }

    #[test]
    fn static_assets_are_404_when_off() {
        for line in [
            "GET /dashboard HTTP/1.1",
            "GET /dashboard/ HTTP/1.1",
            "GET /dashboard/app.js HTTP/1.1",
        ] {
            let resp = static_response(line, false).expect("dashboard path");
            assert!(resp.starts_with("HTTP/1.1 404"), "{line}: {resp}");
        }
    }

    #[test]
    fn static_assets_are_served_with_csp_when_on() {
        for (line, content_type) in [
            ("GET /dashboard HTTP/1.1", "text/html"),
            ("GET /dashboard/?x=1 HTTP/1.1", "text/html"),
            ("GET /dashboard/index.html HTTP/1.1", "text/html"),
            ("GET /dashboard/app.js HTTP/1.1", "text/javascript"),
            ("GET /dashboard/style.css HTTP/1.1", "text/css"),
        ] {
            let resp = static_response(line, true).expect("dashboard path");
            assert!(resp.starts_with("HTTP/1.1 200"), "{line}: {resp}");
            assert!(
                header(&resp, "content-type")
                    .unwrap()
                    .starts_with(content_type),
                "{line}"
            );
            let csp = header(&resp, "content-security-policy").unwrap();
            assert!(csp.starts_with("default-src 'self'"), "{csp}");
            assert_eq!(header(&resp, "x-frame-options"), Some("DENY"));
            let (head, body) = resp.split_once("\r\n\r\n").unwrap();
            let len: usize = header(head, "content-length").unwrap().parse().unwrap();
            assert_eq!(len, body.len());
        }
        let missing = static_response("GET /dashboard/../secret HTTP/1.1", true).unwrap();
        assert!(missing.starts_with("HTTP/1.1 404"));
        assert!(header(&missing, "content-security-policy").is_some());
        let post = static_response("POST /dashboard HTTP/1.1", true).unwrap();
        assert!(post.starts_with("HTTP/1.1 404"));
    }

    #[test]
    fn other_paths_are_not_dashboard_requests() {
        for line in [
            "GET /dashboards HTTP/1.1",
            "GET /v1/namespaces HTTP/1.1",
            "GET /healthz HTTP/1.1",
            "",
        ] {
            assert!(static_response(line, true).is_none(), "{line}");
        }
    }

    #[test]
    fn assets_reference_no_external_origin() {
        for asset in [INDEX_HTML, APP_JS, STYLE_CSS] {
            // The SVG XML namespace is an identifier, not a fetch.
            let asset = asset.replace("\"http://www.w3.org/2000/svg\"", "");
            assert!(!asset.contains("http://"), "external URL in asset");
            assert!(!asset.contains("https://"), "external URL in asset");
        }
        // CSP `default-src 'self'` blocks inline code, so the page must
        // load its script and style from files.
        assert!(!INDEX_HTML.contains("<script>"));
        assert!(!INDEX_HTML.contains("<style"));
        assert!(!INDEX_HTML.contains(" style="));
    }

    #[test]
    fn ring_keeps_the_last_capacity_samples() {
        let dashboard = Dashboard::new(true);
        for i in 0..(RING_CAPACITY + 50) {
            dashboard.record("t/ns", sample(i as f64, i % 10 == 0));
        }
        let recent = dashboard.recent_queries_json("t/ns").unwrap();
        assert_eq!(recent["count"], RING_CAPACITY);
        let samples = recent["samples"].as_array().unwrap();
        assert_eq!(samples[0]["total_ms"], 50.0);
        assert_eq!(samples[RING_CAPACITY - 1]["total_ms"], 249.0);
        assert_eq!(recent["max_ms"], 249.0);
        assert_eq!(recent["degraded"], 20);
        let p50 = recent["p50_ms"].as_f64().unwrap();
        assert!((149.0..=150.0).contains(&p50), "{p50}");
    }

    #[test]
    fn ring_is_empty_for_an_unsearched_namespace_and_absent_when_off() {
        let on = Dashboard::new(true);
        let recent = on.recent_queries_json("t/none").unwrap();
        assert_eq!(recent["count"], 0);
        assert!(recent["p50_ms"].is_null());

        let off = Dashboard::new(false);
        off.record("t/ns", sample(1.0, false));
        assert!(off.recent_queries_json("t/ns").is_none());
        assert!(off.rings.lock().unwrap().by_namespace.is_empty());
    }

    #[test]
    fn tracked_namespaces_are_capped_lru() {
        let dashboard = Dashboard::new(true);
        for i in 0..MAX_TRACKED_NAMESPACES {
            dashboard.record(&format!("t/ns{i}"), sample(1.0, false));
        }
        // Touch ns0 so ns1 becomes the least recently searched.
        dashboard.record("t/ns0", sample(2.0, false));
        dashboard.record("t/new", sample(3.0, false));
        let rings = dashboard.rings.lock().unwrap();
        assert_eq!(rings.by_namespace.len(), MAX_TRACKED_NAMESPACES);
        assert!(rings.by_namespace.contains_key("t/ns0"));
        assert!(!rings.by_namespace.contains_key("t/ns1"));
        assert!(rings.by_namespace.contains_key("t/new"));
    }
}
