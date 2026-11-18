//! API-key authentication, the admin role, and auth-related server limits.
//!
//! Key sources (all merged; env keys win on a key collision):
//!   - Postgres `kosha.api_keys` (when `DATABASE_URL` is set and the
//!     `postgres` feature is on): tenant keys, never admin. Re-read every
//!     `KOSHA_API_KEY_REFRESH_SECS` (default 30) by a background thread, so
//!     keys created through `POST /v1/admin/api-keys` start working and
//!     revoked/deleted keys stop working without a restart.
//!   - `KOSHA_API_KEYS` (`key1=tenant1,key2=tenant2`): tenant keys, not admin.
//!   - `KOSHA_API_KEY` (single key, tenant `default`): the bootstrap key.
//!     It is an **admin** key — it is what operators, the compaction
//!     CronJob, and peer replicas (`/internal/segment/*`) already present.
//!   - `KOSHA_ADMIN_API_KEYS` (`key` or `key=tenant`, comma-separated):
//!     additional admin keys (tenant defaults to `default`).
//!
//! Fail closed: if no key is configured anywhere at startup the server
//! refuses to start, unless `KOSHA_AUTH=disabled` is set explicitly — which
//! turns every request into an admin request for tenant `dev` (local
//! development only).

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock, RwLock};
use std::time::{Duration, Instant};

/// Who a request is acting as, after authentication.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Principal {
    pub tenant: String,
    /// May call `/v1/admin/*` and `/internal/*`, sees every namespace in
    /// `/stats`, and addresses namespaces by their raw (unprefixed) name on
    /// the legacy unversioned routes.
    pub admin: bool,
}

impl Principal {
    /// The principal used for every request when `KOSHA_AUTH=disabled`.
    pub fn dev() -> Self {
        Self {
            tenant: "dev".to_string(),
            admin: true,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeyEntry {
    pub tenant: String,
    pub admin: bool,
}

/// Parse the env-var key sources into one map. Empty keys are ignored (an
/// empty `KOSHA_API_KEY=""` must never make the empty string a valid key).
pub fn parse_env_keys(
    api_keys: Option<&str>,
    api_key: Option<&str>,
    admin_keys: Option<&str>,
) -> HashMap<String, KeyEntry> {
    let mut map = HashMap::new();
    if let Some(list) = api_keys {
        for pair in list.split(',') {
            if let Some((k, t)) = pair.split_once('=') {
                let (k, t) = (k.trim(), t.trim());
                if !k.is_empty() && !t.is_empty() {
                    map.insert(
                        k.to_string(),
                        KeyEntry {
                            tenant: t.to_string(),
                            admin: false,
                        },
                    );
                }
            }
        }
    }
    if let Some(k) = api_key.map(str::trim).filter(|k| !k.is_empty()) {
        map.insert(
            k.to_string(),
            KeyEntry {
                tenant: "default".to_string(),
                admin: true,
            },
        );
    }
    if let Some(list) = admin_keys {
        for item in list.split(',') {
            let (k, t) = match item.split_once('=') {
                Some((k, t)) => (k.trim(), Some(t.trim()).filter(|t| !t.is_empty())),
                None => (item.trim(), None),
            };
            if k.is_empty() {
                continue;
            }
            let tenant = t
                .map(str::to_string)
                .or_else(|| map.get(k).map(|e: &KeyEntry| e.tenant.clone()))
                .unwrap_or_else(|| "default".to_string());
            map.insert(
                k.to_string(),
                KeyEntry {
                    tenant,
                    admin: true,
                },
            );
        }
    }
    map
}

/// Validated API keys: fixed env keys plus a periodically refreshed
/// snapshot of the Postgres `kosha.api_keys` table.
pub struct KeyStore {
    env: HashMap<String, KeyEntry>,
    /// api_key → tenant, from Postgres. Replaced wholesale on each refresh.
    db: RwLock<HashMap<String, String>>,
    #[cfg(feature = "postgres")]
    database_url: Option<String>,
    /// Shared Postgres handle for key reads and `POST /v1/admin/api-keys`
    /// (one pool for the process instead of a fresh connection per call).
    #[cfg(feature = "postgres")]
    pg: Mutex<Option<std::sync::Arc<kosha_control::PgStore>>>,
}

impl KeyStore {
    /// A store with only the given keys and no database (tests, env-only).
    pub fn with_keys(env: HashMap<String, KeyEntry>) -> Self {
        Self {
            env,
            db: RwLock::new(HashMap::new()),
            #[cfg(feature = "postgres")]
            database_url: None,
            #[cfg(feature = "postgres")]
            pg: Mutex::new(None),
        }
    }

    pub fn lookup(&self, key: &str) -> Option<Principal> {
        if key.is_empty() {
            return None;
        }
        if let Some(e) = self.env.get(key) {
            return Some(Principal {
                tenant: e.tenant.clone(),
                admin: e.admin,
            });
        }
        self.db.read().unwrap().get(key).map(|tenant| Principal {
            tenant: tenant.clone(),
            admin: false,
        })
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

    /// Make a just-created key usable on this pod immediately; other pods
    /// pick it up on their next refresh.
    #[cfg_attr(not(feature = "postgres"), allow(dead_code))]
    pub fn insert_db_key(&self, key: String, tenant: String) {
        self.db.write().unwrap().insert(key, tenant);
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
    /// kept (a transient DB blip must not lock every tenant out).
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

pub enum Auth {
    /// `KOSHA_AUTH=disabled`: every request is `Principal::dev()`.
    Disabled,
    Keys(KeyStore),
}

static AUTH: OnceLock<Auth> = OnceLock::new();

/// The process-wide auth config. `main` initializes it (`init_global`)
/// before binding the listener. Unit tests that drive `serve()` without
/// initializing it get `Disabled`, matching their pre-auth behavior; a
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

/// Build the auth config from the environment, load keys, start the
/// refresh thread, and install it globally. Exits the process if no keys
/// are configured and auth wasn't explicitly disabled.
pub fn init_global() {
    if env_flag_disabled("KOSHA_AUTH") {
        eprintln!(
            "WARNING: ******************************************************************\n\
             WARNING: KOSHA_AUTH=disabled — API key authentication is OFF. Every request\n\
             WARNING: is treated as an ADMIN request for tenant 'dev'. Local development\n\
             WARNING: only; never set this on a reachable deployment.\n\
             WARNING: ******************************************************************"
        );
        let _ = AUTH.set(Auth::Disabled);
        return;
    }

    let env = parse_env_keys(
        std::env::var("KOSHA_API_KEYS").ok().as_deref(),
        std::env::var("KOSHA_API_KEY").ok().as_deref(),
        std::env::var("KOSHA_ADMIN_API_KEYS").ok().as_deref(),
    );
    let admin_count = env.values().filter(|e| e.admin).count();
    println!("api keys: {} env key(s) ({} admin)", env.len(), admin_count);
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

    if store.len() == 0 {
        eprintln!(
            "FATAL: no API keys configured (postgres kosha.api_keys empty or unreachable, \
             KOSHA_API_KEY / KOSHA_API_KEYS / KOSHA_ADMIN_API_KEYS unset). Refusing to start \
             with authentication silently open. Configure a key, or set KOSHA_AUTH=disabled \
             explicitly for local development."
        );
        std::process::exit(1);
    }
    if admin_count == 0 {
        println!(
            "api keys: no admin key configured (KOSHA_API_KEY / KOSHA_ADMIN_API_KEYS) — \
             /v1/admin/* and /internal/* will return 403 for every caller"
        );
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

/// Extract the presented API key: `Authorization: Bearer <key>` or
/// `X-Api-Key: <key>` (header names are lower-cased by the request parser).
pub fn presented_key(headers: &HashMap<String, String>) -> Option<&str> {
    headers
        .get("authorization")
        .and_then(|v| v.strip_prefix("Bearer "))
        .or_else(|| headers.get("x-api-key").map(|v| v.as_str()))
        .or_else(|| {
            headers
                .iter()
                .find_map(|(k, v)| k.eq_ignore_ascii_case("x-api-key").then_some(v.as_str()))
        })
        .map(str::trim)
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
/// from logs. Logs the first use of each (tenant, route) and then at most
/// once a minute with the count since the last line, so hot search traffic
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
    fn env_keys_parse_tenants_and_admin_roles() {
        let m = parse_env_keys(
            Some("ka=acme, kb=beta,bad,=x,kc="),
            Some("boot"),
            Some("ka,kadmin=ops,"),
        );
        assert_eq!(
            m.get("ka"),
            Some(&KeyEntry {
                tenant: "acme".into(),
                admin: true
            })
        );
        assert_eq!(
            m.get("kb"),
            Some(&KeyEntry {
                tenant: "beta".into(),
                admin: false
            })
        );
        assert_eq!(
            m.get("boot"),
            Some(&KeyEntry {
                tenant: "default".into(),
                admin: true
            })
        );
        assert_eq!(m.get("kadmin").map(|e| e.tenant.as_str()), Some("ops"));
        assert!(!m.contains_key("") && !m.contains_key("bad") && !m.contains_key("kc"));
    }

    #[test]
    fn empty_bootstrap_key_is_not_a_valid_key() {
        let m = parse_env_keys(None, Some("  "), None);
        assert!(m.is_empty());
        let store = KeyStore::with_keys(m);
        assert_eq!(store.lookup(""), None);
    }

    #[test]
    fn refreshed_db_keys_are_added_and_revoked_without_restart() {
        let store = KeyStore::with_keys(parse_env_keys(None, Some("boot"), None));
        assert_eq!(store.lookup("sk-new"), None);

        // A refresh that sees a key created via the admin API.
        store.replace_db_keys(HashMap::from([("sk-new".into(), "acme".into())]));
        assert_eq!(
            store.lookup("sk-new"),
            Some(Principal {
                tenant: "acme".into(),
                admin: false
            })
        );

        // A later refresh after the key was revoked/deleted.
        store.replace_db_keys(HashMap::new());
        assert_eq!(store.lookup("sk-new"), None);
        // Env keys are unaffected by DB refreshes.
        assert!(store.lookup("boot").is_some_and(|p| p.admin));
    }

    #[test]
    fn db_keys_are_never_admin_even_if_colliding_names() {
        let store = KeyStore::with_keys(HashMap::new());
        store.insert_db_key("sk-x".into(), "acme".into());
        assert_eq!(store.lookup("sk-x").map(|p| p.admin), Some(false));
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn presented_key_reads_bearer_and_x_api_key() {
        let h = HashMap::from([("authorization".to_string(), "Bearer sk-1".to_string())]);
        assert_eq!(presented_key(&h), Some("sk-1"));
        let h = HashMap::from([("x-api-key".to_string(), "sk-2".to_string())]);
        assert_eq!(presented_key(&h), Some("sk-2"));
        assert_eq!(presented_key(&HashMap::new()), None);
    }
}
