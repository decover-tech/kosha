//! Tenant API-key loading, fail-closed startup, key refresh, and
//! auth-related request limits (#235).
//!
//! Tenant keys come from, merged (env wins on a collision):
//!   - Postgres `kosha.api_keys` (when `DATABASE_URL` is set and the
//!     `postgres` feature is on), otherwise an in-memory store that only
//!     the admin API on this pod fills. Re-read every
//!     `KOSHA_API_KEY_REFRESH_SECS` (default 30) by a background thread, so
//!     keys created through `POST /v1/admin/api-keys` start working and
//!     revoked keys stop working without a restart.
//!   - `KOSHA_API_KEYS` (`key1=tenant1,key2=tenant2`).
//!   - `KOSHA_API_KEY` (single key, tenant `default`).
//!
//! Scoped keys (#346): a stored key carries scopes (`read`, `write`,
//! `admin`), an optional namespace list and an optional expiry, and
//! authenticates as a [`Principal`]; `main.rs` (`route`) checks the scope a
//! route needs. Env keys are full-scope. Keys are held and looked up by
//! SHA-256 only (`kosha_control::api_keys`). Expiry is checked on every
//! request; when a key was last used is collected in memory and written on
//! the refresh tick.
//!
//! Admin and peer access is not a property of tenant keys: it is the
//! dedicated `KOSHA_ADMIN_API_KEY` / `KOSHA_INTERNAL_API_KEY` credentials,
//! checked in `main.rs` (`service_authorized`).
//!
//! Fail closed: if no credential of any kind is configured at startup, the
//! server refuses to start, unless `KOSHA_AUTH=disabled` is set explicitly.
//! Then every request is tenant `dev` (local development only). Before this,
//! an empty key table or a failed Postgres connect silently turned auth off.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::time::{Duration, Instant};

use kosha_control::api_keys::{hash_key, now_unix};
use kosha_control::{ApiKeyStore, KeyRecord, MemoryApiKeyStore, NewKey, Scope, Scopes};

/// Parse the env-var tenant key sources into one map. Empty keys and
/// tenants are ignored: an empty `KOSHA_API_KEY=""` must never make the
/// empty string a valid key.
pub fn parse_env_keys(api_keys: Option<&str>, api_key: Option<&str>) -> HashMap<String, String> {
    let mut map = HashMap::new();
    if let Some(list) = api_keys {
        for pair in list.split(',') {
            if let Some((k, t)) = pair.split_once('=') {
                let (k, t) = (k.trim(), t.trim());
                if !k.is_empty() && !t.is_empty() {
                    map.insert(k.to_string(), t.to_string());
                }
            }
        }
    }
    if let Some(k) = api_key.map(str::trim).filter(|k| !k.is_empty()) {
        map.insert(k.to_string(), "default".to_string());
    }
    map
}

/// Who a request authenticated as, and what it may do (#346).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Principal {
    pub tenant: String,
    pub scopes: Scopes,
    /// `None`: every namespace of the tenant.
    pub namespaces: Option<Vec<String>>,
    /// Unix seconds; `None` never expires.
    pub expires_at: Option<i64>,
    /// The stored key's id; `None` for env keys and service credentials.
    pub key_id: Option<String>,
}

impl Principal {
    /// Every scope on every namespace of `tenant`: env keys, the service
    /// credentials and `KOSHA_AUTH=disabled`.
    pub fn full(tenant: &str) -> Self {
        Self {
            tenant: tenant.to_string(),
            scopes: Scopes::ALL,
            namespaces: None,
            expires_at: None,
            key_id: None,
        }
    }

    fn from_record(r: &KeyRecord) -> Self {
        Self {
            tenant: r.tenant_id.clone(),
            scopes: r.scopes,
            namespaces: r.namespaces.clone(),
            expires_at: r.expires_at,
            key_id: Some(r.id.clone()),
        }
    }

    pub fn has_scope(&self, scope: Scope) -> bool {
        self.scopes.contains(scope)
    }

    /// Whether this key may touch the tenant namespace `ns` (unqualified).
    pub fn allows_namespace(&self, ns: &str) -> bool {
        self.namespaces
            .as_ref()
            .is_none_or(|list| list.iter().any(|n| n == ns))
    }
}

/// Why a presented key was refused. Both are 401.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LookupError {
    Unknown,
    Expired,
}

/// Tenant API keys by SHA-256 of the key (#346): fixed env keys plus a
/// periodically refreshed snapshot of the stored keys. No plaintext key is
/// kept past the request that presented it.
pub struct KeyStore {
    /// sha256(key) → principal, from `KOSHA_API_KEYS`/`KOSHA_API_KEY`.
    env: HashMap<String, Principal>,
    /// sha256(key) → principal, from the key store. Replaced wholesale on
    /// each refresh.
    db: RwLock<HashMap<String, Principal>>,
    /// key id → last time it authenticated a request on this pod. Written
    /// to the key store on the refresh tick.
    used: Mutex<HashMap<String, i64>>,
    /// The key store when it is not Postgres: in memory (env-only setups
    /// and tests).
    memory: Arc<dyn ApiKeyStore>,
    #[cfg(feature = "postgres")]
    database_url: Option<String>,
    /// Shared Postgres handle for key reads and the admin API (one pool
    /// per process instead of a fresh connection per call).
    #[cfg(feature = "postgres")]
    pg: Mutex<Option<std::sync::Arc<kosha_control::PgStore>>>,
}

impl KeyStore {
    /// A store with only the given env keys (`key → tenant`) and an
    /// in-memory key store (tests, env-only).
    pub fn with_keys(env: HashMap<String, String>) -> Self {
        Self {
            env: env
                .into_iter()
                .map(|(key, tenant)| (hash_key(&key), Principal::full(&tenant)))
                .collect(),
            db: RwLock::new(HashMap::new()),
            used: Mutex::new(HashMap::new()),
            memory: Arc::new(MemoryApiKeyStore::new()),
            #[cfg(feature = "postgres")]
            database_url: None,
            #[cfg(feature = "postgres")]
            pg: Mutex::new(None),
        }
    }

    /// The principal a presented key authenticates as. Expiry is checked
    /// against the clock now, not only at the last refresh.
    pub fn lookup(&self, key: &str) -> Result<Principal, LookupError> {
        self.lookup_at(key, now_unix())
    }

    /// [`lookup`](Self::lookup) at `now` (Unix seconds).
    pub fn lookup_at(&self, key: &str, now: i64) -> Result<Principal, LookupError> {
        if key.is_empty() {
            return Err(LookupError::Unknown);
        }
        let hash = hash_key(key);
        if let Some(p) = self.env.get(&hash) {
            return Ok(p.clone());
        }
        let principal = self
            .db
            .read()
            .unwrap()
            .get(&hash)
            .cloned()
            .ok_or(LookupError::Unknown)?;
        if principal.expires_at.is_some_and(|t| t <= now) {
            return Err(LookupError::Expired);
        }
        if let Some(id) = &principal.key_id {
            let mut used = self.used.lock().unwrap();
            let at = used.entry(id.clone()).or_insert(now);
            *at = (*at).max(now);
        }
        Ok(principal)
    }

    pub fn len(&self) -> usize {
        let db = self.db.read().unwrap();
        self.env.len() + db.keys().filter(|k| !self.env.contains_key(*k)).count()
    }

    /// Replace the stored-key snapshot with these (active) records. Also
    /// used by tests to simulate a refresh.
    pub fn replace_db_records(&self, records: &[KeyRecord]) {
        let map = records
            .iter()
            .map(|r| (r.key_hash.clone(), Principal::from_record(r)))
            .collect();
        *self.db.write().unwrap() = map;
    }

    /// Make a just-created key usable on this pod immediately. Other pods
    /// pick it up on their next refresh.
    fn insert_record(&self, record: &KeyRecord) {
        self.db
            .write()
            .unwrap()
            .insert(record.key_hash.clone(), Principal::from_record(record));
    }

    /// The shared Postgres handle, connecting on first use.
    #[cfg(feature = "postgres")]
    pub fn pg(&self) -> Option<std::sync::Arc<kosha_control::PgStore>> {
        let mut guard = self.pg.lock().unwrap();
        if guard.is_none() {
            let url = self.database_url.as_ref()?;
            match kosha_control::PgStore::new(url) {
                Ok(store) => *guard = Some(std::sync::Arc::new(store)),
                Err(e) => {
                    eprintln!("api keys: failed to connect to postgres: {e}");
                    return None;
                }
            }
        }
        guard.clone()
    }

    /// Where stored keys live: Postgres when `DATABASE_URL` is set,
    /// otherwise the in-memory store.
    fn backend(&self) -> Result<Arc<dyn ApiKeyStore>, String> {
        #[cfg(feature = "postgres")]
        if self.database_url.is_some() {
            return self
                .pg()
                .map(|pg| pg as Arc<dyn ApiKeyStore>)
                .ok_or_else(|| "postgres unavailable".to_string());
        }
        Ok(self.memory.clone())
    }

    /// Write the last-used times collected since the previous tick, then
    /// re-read the active keys. On error the previous snapshot is kept, so
    /// a transient DB blip doesn't lock every tenant out, and unwritten
    /// last-used times are retried on the next tick.
    pub fn refresh(&self) -> Result<usize, String> {
        let backend = self.backend()?;
        let used: Vec<(String, i64)> = std::mem::take(&mut *self.used.lock().unwrap())
            .into_iter()
            .collect();
        if let Err(e) = backend.touch_last_used(&used) {
            eprintln!("api keys: last-used update failed: {e}");
            let mut pending = self.used.lock().unwrap();
            for (id, at) in used {
                let slot = pending.entry(id).or_insert(at);
                *slot = (*slot).max(at);
            }
        }
        let records = backend.list_active(None).map_err(|e| e.to_string())?;
        self.replace_db_records(&records);
        Ok(records.len())
    }

    /// `/v1/admin/api-keys[/{id}/rotate|revoke]` (#346). `path` excludes
    /// the query string, which is `query`. `valid_tenant` checks a new
    /// key's tenant id. `Ok` is a 200 JSON body; `Err` is (status, message).
    pub fn admin(
        &self,
        method: &str,
        path: &str,
        query: &str,
        body: &[u8],
        valid_tenant: fn(&str) -> bool,
    ) -> Result<serde_json::Value, (u16, String)> {
        let not_found = || (404, "not found".to_string());
        let rest = path
            .strip_prefix("/v1/admin/api-keys")
            .ok_or_else(not_found)?;
        let fail = |e: String| (500, e);
        match (method, rest) {
            ("POST", "") => {
                let req = parse_body(body)?;
                let tenant_id = req
                    .get("tenant_id")
                    .and_then(|v| v.as_str())
                    .ok_or((400, "missing 'tenant_id'".to_string()))?;
                if !valid_tenant(tenant_id) {
                    return Err((
                        400,
                        "invalid 'tenant_id' (must be non-empty, without '/', '\\' or control \
                         characters, and not start with '_' or '.')"
                            .into(),
                    ));
                }
                let new = NewKey {
                    tenant_id: tenant_id.to_string(),
                    scopes: parse_scopes(&req)?,
                    namespaces: parse_namespaces(&req)?,
                    description: req
                        .get("description")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string(),
                    expires_at: parse_expiry(&req)?,
                };
                let backend = self.backend().map_err(fail)?;
                let (record, key) = backend
                    .create(new)
                    .map_err(|e| fail(format!("failed to create key: {e}")))?;
                self.insert_record(&record);
                let mut out = record_json(&record);
                out["api_key"] = serde_json::json!(key);
                Ok(out)
            }
            ("GET", "") => {
                let tenant = query
                    .split('&')
                    .find_map(|p| p.strip_prefix("tenant_id="))
                    .filter(|t| !t.is_empty());
                let backend = self.backend().map_err(fail)?;
                let records = backend
                    .list_active(tenant)
                    .map_err(|e| fail(e.to_string()))?;
                Ok(serde_json::json!({
                    "api_keys": records.iter().map(record_json).collect::<Vec<_>>(),
                }))
            }
            ("POST", rest) => {
                let (id, action) = rest
                    .strip_prefix('/')
                    .and_then(|r| r.split_once('/'))
                    .filter(|(id, _)| valid_key_id(id))
                    .ok_or_else(not_found)?;
                match action {
                    "revoke" => {
                        let backend = self.backend().map_err(fail)?;
                        if !backend.revoke(id).map_err(|e| fail(e.to_string()))? {
                            return Err((404, format!("no active API key '{id}'")));
                        }
                        // Gone on this pod now; other pods drop it on their
                        // next refresh.
                        self.db
                            .write()
                            .unwrap()
                            .retain(|_, p| p.key_id.as_deref() != Some(id));
                        Ok(serde_json::json!({ "id": id, "revoked": true }))
                    }
                    "rotate" => {
                        let req = if body.iter().all(u8::is_ascii_whitespace) {
                            serde_json::json!({})
                        } else {
                            parse_body(body)?
                        };
                        let overlap = match req.get("overlap_secs") {
                            None | Some(serde_json::Value::Null) => DEFAULT_ROTATION_OVERLAP_SECS,
                            Some(v) => v.as_u64().ok_or((
                                400,
                                "'overlap_secs' must be a non-negative integer".to_string(),
                            ))?,
                        }
                        .min(i64::MAX as u64 / 2);
                        let expires_at = parse_expiry(&req)?;
                        let backend = self.backend().map_err(fail)?;
                        let (record, key) = backend
                            .rotate(id, Duration::from_secs(overlap), expires_at)
                            .map_err(|e| fail(e.to_string()))?
                            .ok_or((404, format!("no active API key '{id}'")))?;
                        // The new key, and the old key's shortened expiry,
                        // on this pod now.
                        self.insert_record(&record);
                        if let Err(e) = self.refresh() {
                            eprintln!("api keys: refresh after rotate failed: {e}");
                        }
                        let mut out = record_json(&record);
                        out["api_key"] = serde_json::json!(key);
                        out["overlap_secs"] = serde_json::json!(overlap);
                        Ok(out)
                    }
                    _ => Err(not_found()),
                }
            }
            _ => Err(not_found()),
        }
    }
}

/// How long a rotated key keeps working when the rotate request names no
/// `overlap_secs`.
const DEFAULT_ROTATION_OVERLAP_SECS: u64 = 3600;

fn parse_body(body: &[u8]) -> Result<serde_json::Value, (u16, String)> {
    serde_json::from_slice(body).map_err(|_| (400, "invalid JSON".to_string()))
}

fn string_list(v: &serde_json::Value) -> Option<Vec<String>> {
    v.as_array()?
        .iter()
        .map(|s| s.as_str().map(String::from))
        .collect()
}

/// `scopes`: a non-empty list of `read`/`write`/`admin`. Absent: all.
fn parse_scopes(req: &serde_json::Value) -> Result<Scopes, (u16, String)> {
    let Some(v) = req.get("scopes").filter(|v| !v.is_null()) else {
        return Ok(Scopes::ALL);
    };
    let names = string_list(v).ok_or((400, "'scopes' must be a list of strings".to_string()))?;
    let scopes = Scopes::from_names(&names).map_err(|e| (400, e))?;
    if scopes.is_empty() {
        return Err((400, "'scopes' must name at least one scope".into()));
    }
    Ok(scopes)
}

/// `namespaces`: a non-empty list of unqualified namespace names. Absent:
/// every namespace of the tenant.
fn parse_namespaces(req: &serde_json::Value) -> Result<Option<Vec<String>>, (u16, String)> {
    let Some(v) = req.get("namespaces").filter(|v| !v.is_null()) else {
        return Ok(None);
    };
    let list = string_list(v).ok_or((400, "'namespaces' must be a list of strings".to_string()))?;
    if list.is_empty() {
        return Err((
            400,
            "'namespaces' must name at least one namespace (omit it for all)".into(),
        ));
    }
    if let Some(bad) = list.iter().find(|n| !valid_namespace_name(n)) {
        return Err((400, format!("invalid namespace '{bad}'")));
    }
    Ok(Some(list))
}

/// `expires_at` (Unix seconds) or `ttl_secs`: at most one, in the future.
fn parse_expiry(req: &serde_json::Value) -> Result<Option<i64>, (u16, String)> {
    let get = |k: &str| req.get(k).filter(|v| !v.is_null());
    let now = now_unix();
    let at = match (get("expires_at"), get("ttl_secs")) {
        (Some(_), Some(_)) => {
            return Err((400, "give 'expires_at' or 'ttl_secs', not both".into()));
        }
        (Some(v), None) => v.as_i64().ok_or((
            400,
            "'expires_at' must be an integer (Unix seconds)".to_string(),
        ))?,
        (None, Some(v)) => {
            let ttl = v
                .as_u64()
                .filter(|t| *t > 0)
                .ok_or((400, "'ttl_secs' must be a positive integer".to_string()))?;
            now.saturating_add(ttl.min(i64::MAX as u64) as i64)
        }
        (None, None) => return Ok(None),
    };
    if at <= now {
        return Err((400, "'expires_at' must be in the future".into()));
    }
    Ok(Some(at))
}

fn valid_namespace_name(ns: &str) -> bool {
    !ns.is_empty()
        && ns != "."
        && ns != ".."
        && !ns.chars().any(|c| c == '/' || c == '\\' || c.is_control())
}

/// A key id as the stores mint them: a UUID.
fn valid_key_id(id: &str) -> bool {
    id.len() == 36
        && id.char_indices().all(|(i, c)| match i {
            8 | 13 | 18 | 23 => c == '-',
            _ => c.is_ascii_hexdigit(),
        })
}

/// A key's metadata for the admin API: never the plaintext or the hash.
fn record_json(r: &KeyRecord) -> serde_json::Value {
    serde_json::json!({
        "id": r.id,
        "tenant_id": r.tenant_id,
        "key_prefix": r.key_prefix,
        "description": r.description,
        "scopes": r.scopes.names(),
        "namespaces": r.namespaces,
        "created_at": r.created_at,
        "expires_at": r.expires_at,
        "last_used_at": r.last_used_at,
        "rotated_from": r.rotated_from,
    })
}

pub enum Auth {
    /// `KOSHA_AUTH=disabled`: every request is tenant `dev`.
    Disabled,
    Keys(Box<KeyStore>),
}

static AUTH: OnceLock<Auth> = OnceLock::new();

/// The process-wide auth config. `main` initializes it (`init_global`)
/// before binding the listener. Unit tests that drive `serve()` without
/// initializing it get `Disabled`, matching their pre-auth behavior. A
/// non-test binary that somehow skipped init fails closed instead.
pub fn global() -> &'static Auth {
    AUTH.get_or_init(|| {
        if cfg!(test) {
            Auth::Disabled
        } else {
            Auth::Keys(Box::new(KeyStore::with_keys(HashMap::new())))
        }
    })
}

fn env_flag_disabled(var: &str) -> bool {
    std::env::var(var)
        .map(|v| {
            matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "disabled" | "off" | "false" | "0" | "none"
            )
        })
        .unwrap_or(false)
}

fn env_nonempty(var: &str) -> bool {
    std::env::var(var).is_ok_and(|v| !v.trim().is_empty())
}

/// Build the auth config from the environment, load keys, start the
/// refresh thread, and install it globally. Exits the process if no
/// credential is configured and auth wasn't explicitly disabled.
pub fn init_global() {
    if env_flag_disabled("KOSHA_AUTH") {
        eprintln!(
            "WARNING: ******************************************************************\n\
             WARNING: KOSHA_AUTH=disabled — API key authentication is OFF. Every request\n\
             WARNING: is treated as tenant 'dev'. Local development only; never set\n\
             WARNING: this on a reachable deployment.\n\
             WARNING: ******************************************************************"
        );
        let _ = AUTH.set(Auth::Disabled);
        return;
    }

    let env = parse_env_keys(
        std::env::var("KOSHA_API_KEYS").ok().as_deref(),
        std::env::var("KOSHA_API_KEY").ok().as_deref(),
    );
    println!("api keys: {} tenant key(s) from env", env.len());
    // #342: namespaces under a tenant id starting with `_` or `.` are
    // accounted to the shared `_legacy` tenant. Such keys keep working, but
    // the operator is told.
    let mut reserved: Vec<&str> = env
        .values()
        .map(String::as_str)
        .filter(|t| t.starts_with('_') || t.starts_with('.'))
        .collect();
    reserved.sort_unstable();
    reserved.dedup();
    if !reserved.is_empty() {
        eprintln!(
            "WARN: api keys: tenant id(s) {reserved:?} start with '_' or '.'; their \
             cache and hydration usage is accounted to the shared _legacy tenant"
        );
    }
    #[allow(unused_mut)]
    let mut store = KeyStore::with_keys(env);

    let database_url = std::env::var("DATABASE_URL").ok().filter(|u| !u.is_empty());
    #[cfg(feature = "postgres")]
    {
        store.database_url = database_url.clone();
    }
    #[cfg(not(feature = "postgres"))]
    if database_url.is_some() {
        println!(
            "api keys: DATABASE_URL set but postgres feature disabled; env keys and \
             in-memory admin-API keys only"
        );
    }

    if database_url.is_some() {
        match store.refresh() {
            Ok(n) => println!("api keys: loaded {n} key(s) from postgres"),
            Err(e) => eprintln!("api keys: initial postgres load failed: {e}"),
        }
    }

    let service_keys =
        env_nonempty("KOSHA_ADMIN_API_KEY") || env_nonempty("KOSHA_INTERNAL_API_KEY");
    if store.len() == 0 && !service_keys {
        eprintln!(
            "FATAL: no API keys configured (postgres kosha.api_keys empty or unreachable; \
             KOSHA_API_KEY, KOSHA_API_KEYS, KOSHA_ADMIN_API_KEY and KOSHA_INTERNAL_API_KEY \
             unset). Refusing to start with authentication silently open. Configure a key, \
             or set KOSHA_AUTH=disabled explicitly for local development."
        );
        std::process::exit(1);
    }

    let _ = AUTH.set(Auth::Keys(Box::new(store)));

    if database_url.is_some() {
        let ttl = refresh_interval();
        std::thread::Builder::new()
            .name("api-key-refresh".into())
            .spawn(move || loop {
                std::thread::sleep(ttl);
                if let Auth::Keys(store) = global() {
                    if let Err(e) = store.refresh() {
                        eprintln!("api keys: refresh failed, keeping previous snapshot: {e}");
                    }
                }
            })
            .ok();
    }
}

fn refresh_interval() -> Duration {
    let secs = std::env::var("KOSHA_API_KEY_REFRESH_SECS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|s| *s > 0)
        .unwrap_or(30);
    Duration::from_secs(secs)
}

/// Maximum accepted request body (`KOSHA_MAX_BODY_BYTES`, default 64 MiB).
pub fn max_body_bytes() -> usize {
    static MAX: OnceLock<usize> = OnceLock::new();
    *MAX.get_or_init(|| {
        std::env::var("KOSHA_MAX_BODY_BYTES")
            .ok()
            .and_then(|v| v.trim().parse::<usize>().ok())
            .filter(|n| *n > 0)
            .unwrap_or(64 * 1024 * 1024)
    })
}

/// `KOSHA_LEGACY_ROUTES=off` disables the unversioned Phase 1 routes.
pub fn legacy_routes_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| !env_flag_disabled("KOSHA_LEGACY_ROUTES"))
}

/// Requests counted since the last log line, and when that line was written.
type UsageWindow = (u64, Option<Instant>);

/// Per-tenant legacy route usage, so the cutover to `/v1` can be tracked
/// from logs. Logs the first use of each (tenant, route), then at most once
/// a minute with the count since the last line, so hot search traffic
/// doesn't flood the log.
pub fn record_legacy_use(tenant: &str, route: &str) {
    static USAGE: OnceLock<Mutex<HashMap<(String, String), UsageWindow>>> = OnceLock::new();
    let mut usage = USAGE.get_or_init(Default::default).lock().unwrap();
    let entry = usage
        .entry((tenant.to_string(), route.to_string()))
        .or_insert((0, None));
    entry.0 += 1;
    let due = entry
        .1
        .is_none_or(|t| t.elapsed() >= Duration::from_secs(60));
    if due {
        println!(
            "legacy route usage: tenant={tenant} route={route} requests={} \
             (unversioned routes are deprecated; migrate to /v1/namespaces/{{ns}}/...)",
            entry.0
        );
        *entry = (0, Some(Instant::now()));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn env_keys_parse_tenants() {
        let m = parse_env_keys(Some("ka=acme, kb=beta,bad,=x,kc="), Some("boot"));
        assert_eq!(m.get("ka").map(String::as_str), Some("acme"));
        assert_eq!(m.get("kb").map(String::as_str), Some("beta"));
        assert_eq!(m.get("boot").map(String::as_str), Some("default"));
        assert!(!m.contains_key("") && !m.contains_key("bad") && !m.contains_key("kc"));
    }

    #[test]
    fn empty_bootstrap_key_is_not_a_valid_key() {
        let m = parse_env_keys(None, Some("  "));
        assert!(m.is_empty());
        let store = KeyStore::with_keys(m);
        assert_eq!(store.lookup(""), Err(LookupError::Unknown));
    }

    fn valid_tenant(t: &str) -> bool {
        !t.is_empty() && !t.starts_with('_')
    }

    /// Two pods' key stores over one shared key store.
    fn two_pods() -> (KeyStore, KeyStore) {
        let shared: Arc<dyn ApiKeyStore> = Arc::new(MemoryApiKeyStore::new());
        let pod = || {
            let mut s = KeyStore::with_keys(parse_env_keys(None, Some("boot")));
            s.memory = shared.clone();
            s
        };
        (pod(), pod())
    }

    fn create(store: &KeyStore, body: serde_json::Value) -> (String, String) {
        let out = store
            .admin(
                "POST",
                "/v1/admin/api-keys",
                "",
                body.to_string().as_bytes(),
                valid_tenant,
            )
            .unwrap();
        (
            out["id"].as_str().unwrap().to_string(),
            out["api_key"].as_str().unwrap().to_string(),
        )
    }

    #[test]
    fn env_keys_are_full_scope_principals() {
        let store = KeyStore::with_keys(parse_env_keys(Some("ka=acme"), Some("boot")));
        let p = store.lookup("ka").unwrap();
        assert_eq!(p, Principal::full("acme"));
        assert!(Scope::ALL.iter().all(|s| p.has_scope(*s)));
        assert!(p.allows_namespace("anything"));
        assert_eq!(store.lookup("boot").unwrap().tenant, "default");
        assert_eq!(store.lookup("nope"), Err(LookupError::Unknown));
    }

    #[test]
    fn created_key_carries_its_scopes_and_namespaces() {
        let store = KeyStore::with_keys(HashMap::new());
        let (id, key) = create(
            &store,
            serde_json::json!({"tenant_id": "acme", "scopes": ["read"], "namespaces": ["docs"]}),
        );
        let p = store.lookup(&key).unwrap();
        assert_eq!(p.tenant, "acme");
        assert_eq!(p.key_id.as_deref(), Some(id.as_str()));
        assert!(p.has_scope(Scope::Read));
        assert!(!p.has_scope(Scope::Write) && !p.has_scope(Scope::Admin));
        assert!(p.allows_namespace("docs") && !p.allows_namespace("other"));
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn admin_create_rejects_bad_requests() {
        let store = KeyStore::with_keys(HashMap::new());
        for body in [
            serde_json::json!({}),
            serde_json::json!({"tenant_id": "_x"}),
            serde_json::json!({"tenant_id": "acme", "scopes": ["root"]}),
            serde_json::json!({"tenant_id": "acme", "scopes": []}),
            serde_json::json!({"tenant_id": "acme", "namespaces": []}),
            serde_json::json!({"tenant_id": "acme", "namespaces": ["a/b"]}),
            serde_json::json!({"tenant_id": "acme", "expires_at": 1}),
            serde_json::json!({"tenant_id": "acme", "ttl_secs": 0}),
            serde_json::json!({"tenant_id": "acme", "ttl_secs": 5, "expires_at": 9999999999i64}),
        ] {
            let r = store.admin(
                "POST",
                "/v1/admin/api-keys",
                "",
                body.to_string().as_bytes(),
                valid_tenant,
            );
            assert_eq!(r.unwrap_err().0, 400, "{body}");
        }
        let r = store.admin(
            "POST",
            "/v1/admin/api-keys/not-a-uuid/revoke",
            "",
            b"",
            valid_tenant,
        );
        assert_eq!(r.unwrap_err().0, 404);
    }

    #[test]
    fn expired_key_is_refused_at_request_time() {
        let store = KeyStore::with_keys(HashMap::new());
        let (_, key) = create(
            &store,
            serde_json::json!({"tenant_id": "acme", "ttl_secs": 60}),
        );
        let now = now_unix();
        assert!(store.lookup_at(&key, now).is_ok());
        // No refresh in between: the expiry is checked on the request.
        assert_eq!(store.lookup_at(&key, now + 61), Err(LookupError::Expired));
    }

    #[test]
    fn rotation_overlaps_then_the_old_key_stops() {
        let (a, b) = two_pods();
        let (old_id, old_key) = create(&a, serde_json::json!({"tenant_id": "acme"}));
        b.refresh().unwrap();
        let out = a
            .admin(
                "POST",
                &format!("/v1/admin/api-keys/{old_id}/rotate"),
                "",
                br#"{"overlap_secs": 120}"#,
                valid_tenant,
            )
            .unwrap();
        let new_key = out["api_key"].as_str().unwrap().to_string();
        assert_eq!(out["rotated_from"], serde_json::json!(old_id));
        b.refresh().unwrap();
        let now = now_unix();
        for pod in [&a, &b] {
            // Both work during the overlap...
            assert!(pod.lookup_at(&old_key, now).is_ok());
            assert!(pod.lookup_at(&new_key, now).is_ok());
            // ...and only the new one after it.
            assert_eq!(
                pod.lookup_at(&old_key, now + 121),
                Err(LookupError::Expired)
            );
            assert!(pod.lookup_at(&new_key, now + 121).is_ok());
        }
        // A key that is no longer active cannot be rotated again.
        let missing = a.admin(
            "POST",
            "/v1/admin/api-keys/00000000-0000-4000-8000-000000000000/rotate",
            "",
            b"",
            valid_tenant,
        );
        assert_eq!(missing.unwrap_err().0, 404);
    }

    #[test]
    fn revocation_reaches_other_pods_on_refresh() {
        let (a, b) = two_pods();
        let (id, key) = create(&a, serde_json::json!({"tenant_id": "acme"}));
        assert!(b.lookup(&key).is_err());
        b.refresh().unwrap();
        assert_eq!(b.lookup(&key).unwrap().tenant, "acme");

        let out = a
            .admin(
                "POST",
                &format!("/v1/admin/api-keys/{id}/revoke"),
                "",
                b"",
                valid_tenant,
            )
            .unwrap();
        assert_eq!(out["revoked"], serde_json::json!(true));
        // Immediately on the pod that revoked it...
        assert_eq!(a.lookup(&key), Err(LookupError::Unknown));
        // ...and on the other pod after its next refresh.
        assert!(b.lookup(&key).is_ok());
        b.refresh().unwrap();
        assert_eq!(b.lookup(&key), Err(LookupError::Unknown));
        // Env keys are unaffected by refreshes.
        assert_eq!(b.lookup("boot").unwrap().tenant, "default");
        // Revoking twice is a 404.
        let again = a.admin(
            "POST",
            &format!("/v1/admin/api-keys/{id}/revoke"),
            "",
            b"",
            valid_tenant,
        );
        assert_eq!(again.unwrap_err().0, 404);
    }

    #[test]
    fn last_used_is_flushed_on_refresh() {
        let (a, _) = two_pods();
        let (id, key) = create(&a, serde_json::json!({"tenant_id": "acme"}));
        a.lookup_at(&key, 1_000).unwrap();
        a.lookup_at(&key, 2_000).unwrap();
        a.refresh().unwrap();
        let listed = a
            .admin(
                "GET",
                "/v1/admin/api-keys",
                "tenant_id=acme",
                b"",
                valid_tenant,
            )
            .unwrap();
        let row = &listed["api_keys"][0];
        assert_eq!(row["id"], serde_json::json!(id));
        assert_eq!(row["last_used_at"], serde_json::json!(2_000));
        assert!(a.used.lock().unwrap().is_empty());
    }

    #[test]
    fn plaintext_keys_are_never_stored_or_listed() {
        let (a, _) = two_pods();
        let store = KeyStore::with_keys(parse_env_keys(Some("env-secret=acme"), None));
        let (_, key) = create(
            &a,
            serde_json::json!({"tenant_id": "acme", "scopes": ["read"]}),
        );
        let secret = key.split_once('_').unwrap().1.to_string();
        a.refresh().unwrap();
        let held = format!(
            "{:?} {:?} {:?}",
            a.env,
            a.db.read().unwrap(),
            a.memory.list_active(None).unwrap()
        );
        assert!(!held.contains(&secret), "{held}");
        let env_held = format!("{:?}", store.env);
        assert!(!env_held.contains("env-secret"), "{env_held}");
        let listed = a
            .admin("GET", "/v1/admin/api-keys", "", b"", valid_tenant)
            .unwrap()
            .to_string();
        assert!(!listed.contains(&secret), "{listed}");
        assert!(!listed.contains(&hash_key(&key)), "{listed}");
        assert!(listed.contains("key_prefix"));
    }
}
