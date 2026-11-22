//! Tenant API-key loading, fail-closed startup, key refresh, and
//! auth-related request limits (#235).
//!
//! Tenant keys (key → tenant) come from, merged (env wins on a collision):
//!   - Postgres `kosha.api_keys` (when `DATABASE_URL` is set and the
//!     `postgres` feature is on). Re-read every `KOSHA_API_KEY_REFRESH_SECS`
//!     (default 30) by a background thread, so keys created through
//!     `POST /v1/admin/api-keys` start working and deleted keys stop working
//!     without a restart.
//!   - `KOSHA_API_KEYS` (`key1=tenant1,key2=tenant2`).
//!   - `KOSHA_API_KEY` (single key, tenant `default`).
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
use std::sync::{Mutex, OnceLock, RwLock};
use std::time::{Duration, Instant};

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

/// Tenant API keys: fixed env keys plus a periodically refreshed snapshot
/// of the Postgres `kosha.api_keys` table.
pub struct KeyStore {
    env: HashMap<String, String>,
    /// api_key → tenant, from Postgres. Replaced wholesale on each refresh.
    db: RwLock<HashMap<String, String>>,
    #[cfg(feature = "postgres")]
    database_url: Option<String>,
    /// Shared Postgres handle for key reads and `POST /v1/admin/api-keys`
    /// (one pool per process instead of a fresh connection per call).
    #[cfg(feature = "postgres")]
    pg: Mutex<Option<std::sync::Arc<kosha_control::PgStore>>>,
}

impl KeyStore {
    /// A store with only the given keys and no database (tests, env-only).
    pub fn with_keys(env: HashMap<String, String>) -> Self {
        Self {
            env,
            db: RwLock::new(HashMap::new()),
            #[cfg(feature = "postgres")]
            database_url: None,
            #[cfg(feature = "postgres")]
            pg: Mutex::new(None),
        }
    }

    /// The tenant a key belongs to.
    pub fn lookup(&self, key: &str) -> Option<String> {
        if key.is_empty() {
            return None;
        }
        if let Some(t) = self.env.get(key) {
            return Some(t.clone());
        }
        self.db.read().unwrap().get(key).cloned()
    }

    pub fn len(&self) -> usize {
        let db = self.db.read().unwrap();
        self.env.len() + db.keys().filter(|k| !self.env.contains_key(*k)).count()
    }

    /// Replace the database snapshot (also used by tests to simulate a
    /// Postgres refresh that added or removed keys).
    #[cfg_attr(not(feature = "postgres"), allow(dead_code))]
    pub fn replace_db_keys(&self, keys: HashMap<String, String>) {
        *self.db.write().unwrap() = keys;
    }

    /// Make a just-created key usable on this pod immediately. Other pods
    /// pick it up on their next refresh.
    #[cfg_attr(not(feature = "postgres"), allow(dead_code))]
    pub fn insert_db_key(&self, key: String, tenant: String) {
        self.db.write().unwrap().insert(key, tenant);
    }

    /// How many env-configured keys map to `tenant`. They cannot be
    /// removed at runtime.
    pub fn env_key_count(&self, tenant: &str) -> usize {
        self.env.values().filter(|t| t.as_str() == tenant).count()
    }

    /// Drop every database key of `tenant` from this pod's snapshot.
    /// Returns how many went.
    pub fn remove_db_tenant(&self, tenant: &str) -> usize {
        let mut db = self.db.write().unwrap();
        let before = db.len();
        db.retain(|_, t| t != tenant);
        before - db.len()
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

    /// Re-read active keys from Postgres. On error the previous snapshot is
    /// kept, so a transient DB blip doesn't lock every tenant out.
    #[cfg(feature = "postgres")]
    pub fn refresh(&self) -> Result<usize, String> {
        if self.database_url.is_none() {
            return Ok(0);
        }
        let store = self.pg().ok_or("postgres unavailable")?;
        let rows = store.list_api_keys(None).map_err(|e| e.to_string())?;
        let map: HashMap<String, String> = rows
            .into_iter()
            .filter(|(k, _, _)| !k.is_empty())
            .map(|(k, t, _)| (k, t))
            .collect();
        let n = map.len();
        self.replace_db_keys(map);
        Ok(n)
    }

    #[cfg(not(feature = "postgres"))]
    pub fn refresh(&self) -> Result<usize, String> {
        Ok(0)
    }
}

/// What [`purge_tenant_keys`] did.
#[derive(Debug, PartialEq, Eq)]
pub struct KeyPurge {
    /// Keys deleted (rows in `kosha.api_keys`, or snapshot entries when
    /// there is no database).
    pub deleted: u64,
    /// Keys of the tenant configured through `KOSHA_API_KEYS` /
    /// `KOSHA_API_KEY`: they still authenticate until the deployment's
    /// environment changes.
    pub static_remaining: usize,
}

/// Delete a tenant's API keys (tenant deletion, #347): the rows in
/// `kosha.api_keys`, and this pod's snapshot of them so they stop working
/// here at once. Other pods drop them on their next key refresh
/// (`KOSHA_API_KEY_REFRESH_SECS`).
pub fn purge_tenant_keys(auth: &Auth, tenant: &str) -> Result<KeyPurge, String> {
    let Auth::Keys(store) = auth else {
        return Ok(KeyPurge {
            deleted: 0,
            static_remaining: 0,
        });
    };
    // The rows first: if that fails the snapshot is left alone, since the
    // next refresh would bring the keys back anyway.
    #[cfg(feature = "postgres")]
    let deleted_rows = match store.database_url {
        Some(_) => {
            let pg = store.pg().ok_or("postgres unavailable")?;
            pg.delete_api_keys(tenant).map_err(|e| e.to_string())?
        }
        None => 0,
    };
    #[cfg(not(feature = "postgres"))]
    let deleted_rows = 0;
    let dropped = store.remove_db_tenant(tenant) as u64;
    Ok(KeyPurge {
        deleted: deleted_rows.max(dropped),
        static_remaining: store.env_key_count(tenant),
    })
}

pub enum Auth {
    /// `KOSHA_AUTH=disabled`: every request is tenant `dev`.
    Disabled,
    Keys(KeyStore),
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
            Auth::Keys(KeyStore::with_keys(HashMap::new()))
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
        println!("api keys: DATABASE_URL set but postgres feature disabled; env keys only");
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

    let _ = AUTH.set(Auth::Keys(store));

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
        assert_eq!(store.lookup(""), None);
    }

    #[test]
    fn refreshed_db_keys_are_added_and_revoked_without_restart() {
        let store = KeyStore::with_keys(parse_env_keys(None, Some("boot")));
        assert_eq!(store.lookup("sk-new"), None);

        // A refresh that sees a key created via the admin API.
        store.replace_db_keys(HashMap::from([("sk-new".into(), "acme".into())]));
        assert_eq!(store.lookup("sk-new").as_deref(), Some("acme"));

        // A later refresh after the key was deleted.
        store.replace_db_keys(HashMap::new());
        assert_eq!(store.lookup("sk-new"), None);
        // Env keys are unaffected by DB refreshes.
        assert_eq!(store.lookup("boot").as_deref(), Some("default"));
    }

    #[test]
    fn inserted_key_is_usable_immediately() {
        let store = KeyStore::with_keys(HashMap::new());
        store.insert_db_key("sk-x".into(), "acme".into());
        assert_eq!(store.lookup("sk-x").as_deref(), Some("acme"));
        assert_eq!(store.len(), 1);
    }
}
