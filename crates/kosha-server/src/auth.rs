//! Tenant API-key loading, fail-closed startup, key refresh, and
//! auth-related request limits (#235).
//!
//! Tenant keys (key → tenant) come from, merged (env wins on a collision):
//!   - Postgres `kosha.api_keys` (when `DATABASE_URL` is set and the
//!     `postgres` feature is on). Re-read every `KOSHA_API_KEY_REFRESH_SECS`
//!     (default 30) by a background thread, so keys created through
//!     `POST /v1/admin/api-keys` start working and revoked keys stop working
//!     without a restart. These keys are stored and matched by hash, and
//!     carry scopes, an optional namespace allow-list and an optional
//!     expiry (#346); `authz.rs` enforces them per request.
//!   - `KOSHA_API_KEYS` (`key1=tenant1,key2=tenant2`).
//!   - `KOSHA_API_KEY` (single key, tenant `default`).
//!
//! Env keys are operator configuration: they have every scope, no namespace
//! restriction and no expiry.
//!
//! Admin and peer access is not a property of tenant keys: it is the
//! dedicated `KOSHA_ADMIN_API_KEY` / `KOSHA_INTERNAL_API_KEY` credentials,
//! checked in `main.rs` (`service_authorized`).
//!
//! Operators can also hold their own named admin credentials (#346,
//! `operator_admin.rs`). They are rows of the same Postgres registry, so
//! they are loaded, refreshed, expired and revoked exactly like tenant keys,
//! but [`KeyStore::lookup`] never accepts one as a tenant key: only
//! [`KeyStore::lookup_operator`] does. `KOSHA_ADMIN_API_KEY` stays the
//! bootstrap and break-glass credential until `KOSHA_ADMIN_BOOTSTRAP_KEY`
//! switches it off ([`bootstrap_admin_key`]).
//!
//! Fail closed: if no credential of any kind is configured at startup, the
//! server refuses to start, unless `KOSHA_AUTH=disabled` is set explicitly.
//! Then every request is tenant `dev` (local development only). Before this,
//! an empty key table or a failed Postgres connect silently turned auth off.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::time::{Duration, Instant};

use kosha_control::api_keys::{is_operator_tenant_id, operator_name};
use kosha_control::{hash_api_key, ApiKeyRecord, Scopes};

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

/// Who a request authenticated as, and what its key may do (#346).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Principal {
    pub tenant: String,
    pub scopes: Scopes,
    /// `None`: every namespace of the tenant. Otherwise the unqualified
    /// namespace names the key may touch.
    pub namespaces: Option<Arc<[String]>>,
}

impl Principal {
    /// Everything within `tenant`: what every key could do before #346,
    /// and what env keys and `KOSHA_AUTH=disabled` still get.
    pub fn full(tenant: impl Into<String>) -> Self {
        Self {
            tenant: tenant.into(),
            scopes: Scopes::ALL,
            namespaces: None,
        }
    }

    /// True when no scope or namespace check can ever reject this key.
    pub fn unrestricted(&self) -> bool {
        self.scopes == Scopes::ALL && self.namespaces.is_none()
    }
}

/// Why a presented key was not accepted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Rejected {
    /// Not a key this server knows (never issued, revoked, or mistyped).
    Unknown,
    /// A known key past its `expires_at`.
    Expired,
}

/// One Postgres key in the in-memory snapshot, held under its hash.
struct DbKey {
    principal: Principal,
    /// Unix seconds; `None` never expires.
    expires_at: Option<i64>,
    /// Unix seconds of the latest use not yet written to Postgres; 0 when
    /// there is none.
    last_used: AtomicI64,
}

impl DbKey {
    fn new(record: ApiKeyRecord) -> (String, Self) {
        (
            record.key_hash,
            Self {
                principal: Principal {
                    tenant: record.tenant_id,
                    scopes: record.scopes,
                    namespaces: record.namespaces.map(Arc::from),
                },
                expires_at: record.expires_at,
                last_used: AtomicI64::new(0),
            },
        )
    }
}

pub(crate) fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Tenant API keys: fixed env keys plus a periodically refreshed snapshot
/// of the Postgres `kosha.api_keys` table.
///
/// Postgres keys are held and matched by SHA-256 hash (#346): neither the
/// table (for keys created since) nor this snapshot holds a usable key.
/// Env keys are operator configuration and always have full access.
pub struct KeyStore {
    env: HashMap<String, String>,
    /// key hash → key, from Postgres. Replaced wholesale on each refresh.
    db: RwLock<HashMap<String, DbKey>>,
    /// Orders registry changes against refreshes; see [`Self::registry_guard`].
    registry: Mutex<()>,
    #[cfg(feature = "postgres")]
    database_url: Option<String>,
    /// Shared Postgres handle for key reads and the `/v1/admin/api-keys`
    /// routes (one pool per process instead of a fresh connection per call).
    #[cfg(feature = "postgres")]
    pg: Mutex<Option<std::sync::Arc<kosha_control::PgStore>>>,
}

impl KeyStore {
    /// A store with only the given keys and no database (tests, env-only).
    pub fn with_keys(env: HashMap<String, String>) -> Self {
        Self {
            env,
            db: RwLock::new(HashMap::new()),
            registry: Mutex::new(()),
            #[cfg(feature = "postgres")]
            database_url: None,
            #[cfg(feature = "postgres")]
            pg: Mutex::new(None),
        }
    }

    /// Who a key authenticates as.
    pub fn lookup(&self, key: &str) -> Result<Principal, Rejected> {
        self.lookup_at(key, unix_now())
    }

    /// [`Self::lookup`] at an explicit time (unix seconds). A key is
    /// rejected from the second it expires, without waiting for a refresh.
    pub fn lookup_at(&self, key: &str, now: i64) -> Result<Principal, Rejected> {
        if key.is_empty() {
            return Err(Rejected::Unknown);
        }
        if let Some(t) = self.env.get(key) {
            return Ok(Principal::full(t.clone()));
        }
        let db = self.db.read().unwrap();
        let found = db.get(&hash_api_key(key)).ok_or(Rejected::Unknown)?;
        if found.expires_at.is_some_and(|at| now >= at) {
            return Err(Rejected::Expired);
        }
        // An operator admin credential is never a tenant key.
        if is_operator_tenant_id(&found.principal.tenant) {
            return Err(Rejected::Unknown);
        }
        found.last_used.fetch_max(now, Ordering::Relaxed);
        Ok(found.principal.clone())
    }

    /// The operator a named admin credential (#346) belongs to.
    pub fn lookup_operator(&self, key: &str) -> Result<String, Rejected> {
        self.lookup_operator_at(key, unix_now())
    }

    /// [`Self::lookup_operator`] at an explicit time (unix seconds). Like a
    /// tenant key, the credential is rejected from the second it expires,
    /// and from this pod's next refresh after it is revoked. A tenant key
    /// is never an operator credential, whatever its scopes.
    pub fn lookup_operator_at(&self, key: &str, now: i64) -> Result<String, Rejected> {
        if key.is_empty() {
            return Err(Rejected::Unknown);
        }
        let db = self.db.read().unwrap();
        let found = db.get(&hash_api_key(key)).ok_or(Rejected::Unknown)?;
        let name = operator_name(&found.principal.tenant).ok_or(Rejected::Unknown)?;
        if found.expires_at.is_some_and(|at| now >= at) {
            return Err(Rejected::Expired);
        }
        found.last_used.fetch_max(now, Ordering::Relaxed);
        Ok(name.to_string())
    }

    /// How many operator admin credentials in the snapshot are usable at
    /// `now` (unix seconds).
    pub fn active_operator_credentials(&self, now: i64) -> usize {
        self.db
            .read()
            .unwrap()
            .values()
            .filter(|key| operator_name(&key.principal.tenant).is_some())
            .filter(|key| key.expires_at.is_none_or(|at| now < at))
            .count()
    }

    pub fn len(&self) -> usize {
        self.env.len() + self.db.read().unwrap().len()
    }

    /// Hold this from before a registry change is written to Postgres
    /// until it is applied to the snapshot (`upsert_db_key`,
    /// `remove_db_key`). [`Self::refresh`] holds it from its read of the
    /// table until it swaps the snapshot in, so a refresh that read the
    /// table just before the change cannot overwrite the change with what
    /// it read: without this, a key revoked through this pod could work
    /// again here, and a key just created here be refused, until the next
    /// refresh.
    #[cfg_attr(not(feature = "postgres"), allow(dead_code))]
    pub fn registry_guard(&self) -> std::sync::MutexGuard<'_, ()> {
        self.registry.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Replace the database snapshot (also used by tests to simulate a
    /// Postgres refresh that added, changed or removed keys).
    #[cfg_attr(not(feature = "postgres"), allow(dead_code))]
    pub fn replace_db_keys(&self, keys: Vec<ApiKeyRecord>) {
        let map = keys
            .into_iter()
            .filter(|k| !k.key_hash.is_empty() && !k.tenant_id.is_empty())
            .map(DbKey::new)
            .collect();
        *self.db.write().unwrap() = map;
    }

    /// Apply a just-made change to one key (created, or given an earlier
    /// expiry by a rotation) on this pod immediately. Other pods pick it up
    /// on their next refresh.
    #[cfg_attr(not(feature = "postgres"), allow(dead_code))]
    pub fn upsert_db_key(&self, record: ApiKeyRecord) {
        let (hash, key) = DbKey::new(record);
        self.db.write().unwrap().insert(hash, key);
    }

    /// Stop accepting a just-revoked key on this pod immediately.
    #[cfg_attr(not(feature = "postgres"), allow(dead_code))]
    pub fn remove_db_key(&self, key_hash: &str) {
        self.db.write().unwrap().remove(key_hash);
    }

    /// Take the `(key hash, unix seconds)` of every Postgres key used since
    /// the last call, for `last_used_at`.
    #[cfg_attr(not(feature = "postgres"), allow(dead_code))]
    pub fn take_last_used(&self) -> Vec<(String, i64)> {
        self.db
            .read()
            .unwrap()
            .iter()
            .filter_map(|(hash, key)| {
                let at = key.last_used.swap(0, Ordering::Relaxed);
                (at > 0).then(|| (hash.clone(), at))
            })
            .collect()
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
        db.retain(|_, key| key.principal.tenant != tenant);
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

    /// Write `last_used_at` for the keys used since the last refresh, then
    /// re-read active keys from Postgres. On error the previous snapshot is
    /// kept, so a transient DB blip doesn't lock every tenant out.
    #[cfg(feature = "postgres")]
    pub fn refresh(&self) -> Result<usize, String> {
        if self.database_url.is_none() {
            return Ok(0);
        }
        let store = self.pg().ok_or("postgres unavailable")?;
        // Best effort: a lost `last_used_at` update must not stop keys
        // from being refreshed (revocations arrive through this path).
        if let Err(e) = store.touch_api_keys(&self.take_last_used()) {
            eprintln!("api keys: failed to record last_used_at: {e}");
        }
        let _registry = self.registry_guard();
        // Tenant keys and operator admin credentials alike.
        let rows = store.list_all_credentials().map_err(|e| e.to_string())?;
        self.replace_db_keys(rows);
        Ok(self.db.read().unwrap().len())
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

/// Whether `KOSHA_ADMIN_BOOTSTRAP_KEY` leaves the shared
/// `KOSHA_ADMIN_API_KEY` credential in force. Unset or empty is `enabled`,
/// which is how every deployment ran before operator credentials (#346).
/// An unrecognized value is an error: a typo must neither keep a credential
/// alive that the operator meant to switch off, nor switch one off silently.
pub fn parse_bootstrap_admin_key_flag(value: Option<&str>) -> Result<bool, String> {
    let value = value.map(str::trim).unwrap_or("");
    match value.to_ascii_lowercase().as_str() {
        "" | "enabled" | "on" | "true" | "1" => Ok(true),
        "disabled" | "off" | "false" | "0" => Ok(false),
        // The value is not echoed: the likeliest mistake is putting the
        // shared admin key itself here, and this message is logged.
        _ => {
            Err("invalid KOSHA_ADMIN_BOOTSTRAP_KEY (expected 'enabled' or 'disabled')".to_string())
        }
    }
}

/// The shared admin credential a request may present: `admin_api_key`
/// (`KOSHA_ADMIN_API_KEY`), unless it is empty or `flag`
/// (`KOSHA_ADMIN_BOOTSTRAP_KEY`) disables it. An invalid flag yields no
/// credential, so nothing is accepted by mistake; `init_global` refuses to
/// start on one.
pub fn bootstrap_admin_key_from(admin_api_key: Option<&str>, flag: Option<&str>) -> Option<String> {
    if !parse_bootstrap_admin_key_flag(flag).unwrap_or(false) {
        return None;
    }
    admin_api_key.filter(|k| !k.is_empty()).map(str::to_string)
}

/// [`bootstrap_admin_key_from`] the process environment.
pub fn bootstrap_admin_key() -> Option<String> {
    bootstrap_admin_key_from(
        std::env::var("KOSHA_ADMIN_API_KEY").ok().as_deref(),
        std::env::var("KOSHA_ADMIN_BOOTSTRAP_KEY").ok().as_deref(),
    )
}

/// Build the auth config from the environment, load keys, start the
/// refresh thread, and install it globally. Exits the process if no
/// credential is configured and auth wasn't explicitly disabled.
pub fn init_global() {
    let bootstrap_flag = std::env::var("KOSHA_ADMIN_BOOTSTRAP_KEY").ok();
    let bootstrap_enabled = match parse_bootstrap_admin_key_flag(bootstrap_flag.as_deref()) {
        Ok(enabled) => enabled,
        Err(e) => {
            eprintln!("FATAL: {e}. Refusing to start.");
            std::process::exit(1);
        }
    };
    if !bootstrap_enabled {
        println!(
            "admin credentials: KOSHA_ADMIN_BOOTSTRAP_KEY=disabled — KOSHA_ADMIN_API_KEY is \
             not accepted; only operator credentials reach /v1/admin/*"
        );
    }

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
        // The plaintext scrub (#346) stays a manual step, so say when it
        // is due.
        #[cfg(feature = "postgres")]
        if let Some(count) = store.pg().map(|pg| pg.count_plaintext_api_keys()) {
            match count {
                Ok(0) => println!("api keys: no row holds a plaintext key"),
                Ok(n) => println!(
                    "api keys: {n} row(s) still hold a plaintext key; once every pod runs a \
                     build with hashed keys, remove them with \
                     POST /v1/admin/api-keys/scrub-plaintext"
                ),
                Err(e) => eprintln!("api keys: could not count plaintext rows: {e}"),
            }
        }
    }
    if !bootstrap_enabled && store.active_operator_credentials(unix_now()) == 0 {
        eprintln!(
            "WARN: admin credentials: KOSHA_ADMIN_BOOTSTRAP_KEY=disabled and no usable \
             operator credential was loaded; /v1/admin/* is unreachable until one is, or \
             until the bootstrap key is enabled again"
        );
    }

    // A switched-off bootstrap key is not a configured credential.
    let service_keys = (bootstrap_enabled && env_nonempty("KOSHA_ADMIN_API_KEY"))
        || env_nonempty("KOSHA_INTERNAL_API_KEY");
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

/// `KOSHA_LEGACY_ROUTES=off` disables the unversioned Phase 1 routes: the
/// pod default (ADR-350 §c).
pub fn legacy_routes_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| !env_flag_disabled("KOSHA_LEGACY_ROUTES"))
}

/// `KOSHA_LEGACY_ROUTES_TENANTS`: tenants that keep the legacy routes while
/// the pod default is off. Read once at startup.
fn legacy_routes_tenants() -> &'static HashSet<String> {
    static TENANTS: OnceLock<HashSet<String>> = OnceLock::new();
    TENANTS.get_or_init(|| {
        parse_tenant_list(std::env::var("KOSHA_LEGACY_ROUTES_TENANTS").ok().as_deref())
    })
}

/// A comma-separated tenant list; blanks are ignored.
pub fn parse_tenant_list(raw: Option<&str>) -> HashSet<String> {
    raw.unwrap_or("")
        .split(',')
        .map(str::trim)
        .filter(|t| !t.is_empty())
        .map(str::to_string)
        .collect()
}

/// Whether `tenant` may use the legacy routes: the pod default, or the
/// tenant is on the allowlist. A managed deployment sets the default off
/// and lists only the tenants still migrating.
pub fn legacy_routes_enabled_for(tenant: &str) -> bool {
    #[cfg(test)]
    if let Some(allowed) = LEGACY_POLICY_OVERRIDE.with(|policy| {
        policy
            .borrow()
            .as_ref()
            .map(|(default_on, allowlist)| legacy_routes_allowed(*default_on, allowlist, tenant))
    }) {
        return allowed;
    }
    legacy_routes_allowed(legacy_routes_enabled(), legacy_routes_tenants(), tenant)
}

#[cfg(test)]
thread_local! {
    /// A per-thread stand-in for `KOSHA_LEGACY_ROUTES` and
    /// `KOSHA_LEGACY_ROUTES_TENANTS` (both read once per process), so a
    /// test can drive `route()` with the flag off without touching other
    /// tests.
    static LEGACY_POLICY_OVERRIDE: std::cell::RefCell<Option<(bool, HashSet<String>)>> =
        const { std::cell::RefCell::new(None) };
}

/// Run `f` as if the pod default were `default_on` and the allowlist
/// `allowlist`, on this thread only.
#[cfg(test)]
pub fn with_legacy_policy<T>(default_on: bool, allowlist: &str, f: impl FnOnce() -> T) -> T {
    let policy = (default_on, parse_tenant_list(Some(allowlist)));
    LEGACY_POLICY_OVERRIDE.with(|p| *p.borrow_mut() = Some(policy));
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            LEGACY_POLICY_OVERRIDE.with(|p| *p.borrow_mut() = None);
        }
    }
    let _reset = Reset;
    f()
}

/// The decision behind [`legacy_routes_enabled_for`], for tests.
pub fn legacy_routes_allowed(default_on: bool, allowlist: &HashSet<String>, tenant: &str) -> bool {
    default_on || allowlist.contains(tenant)
}

/// When the legacy routes were deprecated (RFC 9745 `Deprecation`): the
/// release that ships #350's 350c, 2026-12-15T00:00:00Z.
pub const LEGACY_DEPRECATED_AT: u64 = 1_797_292_800;

/// When the legacy routes are removed (RFC 8594 `Sunset`). Pushed later,
/// never earlier; gated on `kosha_legacy_requests_total` reading zero on
/// managed deployments (docs/api/versioning.md).
pub const LEGACY_SUNSET: &str = "Tue, 01 Jun 2027 00:00:00 GMT";

/// Where the deprecation is documented (`Link: <...>; rel="deprecation"`).
pub const LEGACY_DEPRECATION_LINK: &str =
    "https://github.com/decover-tech/kosha/blob/main/docs/api/versioning.md#legacy-routes";

/// The unversioned Phase 1 routes, as `record_legacy_use` counts them.
const LEGACY_ROUTES: [&str; 7] = [
    "/index", "/exists", "/replace", "/search", "/flush", "/delete", "/stats",
];

/// How often the legacy-route usage line is written, at most.
const LEGACY_LOG_INTERVAL_S: u64 = 3600;

/// Legacy route usage counted since the last log line: one atomic counter
/// per route and the time the next line is due, so the per-request cost is
/// an atomic add and a clock read, with no lock and no allocation.
struct LegacyUsage {
    counts: [AtomicU64; LEGACY_ROUTES.len()],
    /// Seconds since `started` at which the next line may be written.
    next_log_s: AtomicU64,
}

impl LegacyUsage {
    const fn new() -> Self {
        Self {
            counts: [const { AtomicU64::new(0) }; LEGACY_ROUTES.len()],
            next_log_s: AtomicU64::new(0),
        }
    }

    /// Count one use of `route` at `now_s`; the log line when one is due
    /// (the first use, then at most once per `LEGACY_LOG_INTERVAL_S`).
    fn record(&self, tenant: &str, route: &str, now_s: u64) -> Option<String> {
        let i = LEGACY_ROUTES.iter().position(|r| *r == route)?;
        self.counts[i].fetch_add(1, Ordering::Relaxed);
        let due = self.next_log_s.load(Ordering::Relaxed);
        if now_s < due
            || self
                .next_log_s
                .compare_exchange(
                    due,
                    now_s + LEGACY_LOG_INTERVAL_S,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                )
                .is_err()
        {
            return None;
        }
        let counts: Vec<String> = LEGACY_ROUTES
            .iter()
            .zip(&self.counts)
            .filter_map(|(route, count)| {
                let n = count.swap(0, Ordering::Relaxed);
                (n > 0).then(|| format!("{route}={n}"))
            })
            .collect();
        Some(format!(
            "legacy route usage since the last line: {} latest_tenant={tenant} \
             (unversioned routes are deprecated; migrate to /v1/namespaces/{{ns}}/...)",
            counts.join(" ")
        ))
    }
}

/// Legacy route usage, so the cutover to `/v1` can be tracked from logs:
/// a line on the first use, then at most once an hour per pod with each
/// route's count since the last line. Runs on every legacy request (the
/// backend's searches among them), so it takes no lock and allocates only
/// when it logs.
pub fn record_legacy_use(tenant: &str, route: &str) {
    static USAGE: LegacyUsage = LegacyUsage::new();
    static STARTED: OnceLock<Instant> = OnceLock::new();
    let now_s = STARTED.get_or_init(Instant::now).elapsed().as_secs();
    if let Some(line) = USAGE.record(tenant, route, now_s) {
        println!("{line}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_usage_logs_first_use_then_at_most_hourly() {
        let usage = LegacyUsage::new();
        let first = usage
            .record("acme", "/search", 0)
            .expect("first use is logged");
        assert!(first.contains("/search=1"), "{first}");
        for s in 1..LEGACY_LOG_INTERVAL_S {
            assert_eq!(usage.record("acme", "/search", s), None);
        }
        assert_eq!(usage.record("beta", "/index", 10), None);
        let next = usage
            .record("acme", "/search", LEGACY_LOG_INTERVAL_S)
            .expect("logged again after the interval");
        assert!(
            next.contains(&format!("/search={LEGACY_LOG_INTERVAL_S}")),
            "{next}"
        );
        assert!(next.contains("/index=1"), "{next}");
        assert_eq!(
            usage.record("acme", "/search", LEGACY_LOG_INTERVAL_S + 1),
            None
        );
        assert_eq!(
            usage.record("acme", "/v1/other", 1 << 40),
            None,
            "not a legacy route"
        );
    }

    #[test]
    fn env_keys_parse_tenants() {
        let m = parse_env_keys(Some("ka=acme, kb=beta,bad,=x,kc="), Some("boot"));
        assert_eq!(m.get("ka").map(String::as_str), Some("acme"));
        assert_eq!(m.get("kb").map(String::as_str), Some("beta"));
        assert_eq!(m.get("boot").map(String::as_str), Some("default"));
        assert!(!m.contains_key("") && !m.contains_key("bad") && !m.contains_key("kc"));
    }

    fn record(key: &str, tenant: &str) -> ApiKeyRecord {
        ApiKeyRecord {
            key_hash: hash_api_key(key),
            tenant_id: tenant.into(),
            description: String::new(),
            scopes: Scopes::ALL,
            namespaces: None,
            expires_at: None,
            last_used_at: None,
            created_at: 0,
        }
    }

    fn tenant_of(store: &KeyStore, key: &str) -> Option<String> {
        store.lookup(key).ok().map(|p| p.tenant)
    }

    #[test]
    fn empty_bootstrap_key_is_not_a_valid_key() {
        let m = parse_env_keys(None, Some("  "));
        assert!(m.is_empty());
        let store = KeyStore::with_keys(m);
        assert_eq!(store.lookup(""), Err(Rejected::Unknown));
    }

    #[test]
    fn refreshed_db_keys_are_added_and_revoked_without_restart() {
        let store = KeyStore::with_keys(parse_env_keys(None, Some("boot")));
        assert_eq!(store.lookup("sk-new"), Err(Rejected::Unknown));

        // A refresh that sees a key created via the admin API.
        store.replace_db_keys(vec![record("sk-new", "acme")]);
        assert_eq!(tenant_of(&store, "sk-new").as_deref(), Some("acme"));

        // A later refresh after the key was revoked.
        store.replace_db_keys(Vec::new());
        assert_eq!(store.lookup("sk-new"), Err(Rejected::Unknown));
        // Env keys are unaffected by DB refreshes, and have full access.
        assert_eq!(store.lookup("boot"), Ok(Principal::full("default")));
    }

    #[test]
    fn inserted_key_is_usable_immediately_and_removable() {
        let store = KeyStore::with_keys(HashMap::new());
        store.upsert_db_key(record("sk-x", "acme"));
        assert_eq!(tenant_of(&store, "sk-x").as_deref(), Some("acme"));
        assert_eq!(store.len(), 1);
        store.remove_db_key(&hash_api_key("sk-x"));
        assert_eq!(store.lookup("sk-x"), Err(Rejected::Unknown));
    }

    #[test]
    fn db_keys_match_by_hash_only() {
        let store = KeyStore::with_keys(HashMap::new());
        store.replace_db_keys(vec![record("sk-secret", "acme")]);
        // Neither the hash nor the at-rest placeholder is a credential.
        let hash = hash_api_key("sk-secret");
        assert_eq!(store.lookup(&hash), Err(Rejected::Unknown));
        assert_eq!(
            store.lookup(&format!(" sha256:{hash}")),
            Err(Rejected::Unknown)
        );
        assert!(store.lookup("sk-secret").is_ok());
        // Rows without a hash or a tenant are never loaded.
        store.replace_db_keys(vec![
            ApiKeyRecord {
                key_hash: String::new(),
                ..record("x", "acme")
            },
            record("sk-no-tenant", ""),
        ]);
        assert_eq!(store.len(), 0);
    }

    #[test]
    fn db_keys_carry_scopes_and_namespaces() {
        use kosha_control::Scope;
        let store = KeyStore::with_keys(HashMap::new());
        store.replace_db_keys(vec![
            record("sk-full", "acme"),
            ApiKeyRecord {
                scopes: Scopes::of(&[Scope::Read]),
                namespaces: Some(vec!["docs".into()]),
                ..record("sk-ro", "acme")
            },
        ]);
        assert!(store.lookup("sk-full").unwrap().unrestricted());
        let ro = store.lookup("sk-ro").unwrap();
        assert!(!ro.unrestricted());
        assert_eq!(ro.tenant, "acme");
        assert_eq!(ro.scopes, Scopes::of(&[Scope::Read]));
        assert_eq!(ro.namespaces.as_deref(), Some(&["docs".to_string()][..]));
    }

    #[test]
    fn keys_expire_at_their_deadline_without_a_refresh() {
        let store = KeyStore::with_keys(HashMap::new());
        store.replace_db_keys(vec![ApiKeyRecord {
            expires_at: Some(1_000),
            ..record("sk-temp", "acme")
        }]);
        assert!(store.lookup_at("sk-temp", 999).is_ok());
        assert_eq!(store.lookup_at("sk-temp", 1_000), Err(Rejected::Expired));
        assert_eq!(store.lookup_at("sk-temp", 5_000), Err(Rejected::Expired));
    }

    #[test]
    fn rotation_overlap_keeps_both_keys_working_until_the_old_one_expires() {
        let store = KeyStore::with_keys(HashMap::new());
        store.replace_db_keys(vec![record("sk-old", "acme")]);
        // What the rotate route applies locally (and a refresh loads
        // elsewhere): a new key, and the old one expiring after the overlap.
        store.upsert_db_key(record("sk-new", "acme"));
        store.upsert_db_key(ApiKeyRecord {
            expires_at: Some(2_000),
            ..record("sk-old", "acme")
        });
        assert!(store.lookup_at("sk-old", 1_999).is_ok());
        assert!(store.lookup_at("sk-new", 1_999).is_ok());
        assert_eq!(store.lookup_at("sk-old", 2_000), Err(Rejected::Expired));
        assert!(store.lookup_at("sk-new", 2_000).is_ok());
    }

    fn operator_record(key: &str, name: &str) -> ApiKeyRecord {
        ApiKeyRecord {
            scopes: Scopes::NONE,
            ..record(key, &kosha_control::api_keys::operator_tenant_id(name))
        }
    }

    #[test]
    fn operator_credentials_identify_an_operator_and_are_never_tenant_keys() {
        let store = KeyStore::with_keys(parse_env_keys(None, Some("boot")));
        store.replace_db_keys(vec![
            operator_record("sk-alice", "alice"),
            record("sk-tenant", "acme"),
        ]);
        assert_eq!(store.lookup_operator("sk-alice").as_deref(), Ok("alice"));
        // Not a tenant key, on any route.
        assert_eq!(store.lookup("sk-alice"), Err(Rejected::Unknown));
        // Tenant keys (full access included) and env keys are not operators.
        assert_eq!(store.lookup_operator("sk-tenant"), Err(Rejected::Unknown));
        assert_eq!(store.lookup_operator("boot"), Err(Rejected::Unknown));
        assert_eq!(store.lookup_operator(""), Err(Rejected::Unknown));
        assert_eq!(store.lookup_operator("sk-nobody"), Err(Rejected::Unknown));
        // Neither the hash nor the row's tenant id is a credential.
        assert_eq!(
            store.lookup_operator(&hash_api_key("sk-alice")),
            Err(Rejected::Unknown)
        );
        assert_eq!(
            store.lookup_operator("_operator/alice"),
            Err(Rejected::Unknown)
        );
        assert!(store.lookup("sk-tenant").is_ok());
    }

    #[test]
    fn a_malformed_operator_row_is_neither_an_operator_nor_a_tenant() {
        let store = KeyStore::with_keys(HashMap::new());
        store.replace_db_keys(vec![
            record("sk-empty", "_operator/"),
            record("sk-nested", "_operator/a/b"),
        ]);
        for key in ["sk-empty", "sk-nested"] {
            assert_eq!(store.lookup_operator(key), Err(Rejected::Unknown), "{key}");
            assert_eq!(store.lookup(key), Err(Rejected::Unknown), "{key}");
        }
        assert_eq!(store.active_operator_credentials(0), 0);
    }

    #[test]
    fn operator_credentials_expire_and_are_revoked_like_keys() {
        let store = KeyStore::with_keys(HashMap::new());
        store.replace_db_keys(vec![
            ApiKeyRecord {
                expires_at: Some(1_000),
                ..operator_record("sk-temp", "alice")
            },
            operator_record("sk-bob", "bob"),
        ]);
        assert_eq!(
            store.lookup_operator_at("sk-temp", 999).as_deref(),
            Ok("alice")
        );
        assert_eq!(store.active_operator_credentials(999), 2);
        assert_eq!(
            store.lookup_operator_at("sk-temp", 1_000),
            Err(Rejected::Expired)
        );
        assert_eq!(store.lookup_at("sk-temp", 1_000), Err(Rejected::Expired));
        assert_eq!(store.active_operator_credentials(1_000), 1);

        // Revoked on this pod at once ...
        store.remove_db_key(&hash_api_key("sk-bob"));
        assert_eq!(store.lookup_operator("sk-bob"), Err(Rejected::Unknown));
        // ... and on the others by the refresh that no longer lists it.
        store.upsert_db_key(operator_record("sk-carol", "carol"));
        assert!(store.lookup_operator("sk-carol").is_ok());
        store.replace_db_keys(Vec::new());
        assert_eq!(store.lookup_operator("sk-carol"), Err(Rejected::Unknown));
    }

    #[test]
    fn operator_credential_use_is_recorded_for_last_used_at() {
        let store = KeyStore::with_keys(HashMap::new());
        store.replace_db_keys(vec![operator_record("sk-alice", "alice")]);
        store.lookup_operator_at("sk-alice", 700).unwrap();
        // A refused tenant-route lookup of it records nothing.
        store.lookup_at("sk-alice", 900).unwrap_err();
        assert_eq!(
            store.take_last_used(),
            vec![(hash_api_key("sk-alice"), 700)]
        );
    }

    #[test]
    fn bootstrap_admin_key_is_on_by_default_and_fails_closed_on_a_bad_flag() {
        // Unset or empty: exactly what every deployment had before.
        for flag in [None, Some(""), Some("  "), Some("enabled"), Some("ON")] {
            assert_eq!(parse_bootstrap_admin_key_flag(flag), Ok(true), "{flag:?}");
            assert_eq!(
                bootstrap_admin_key_from(Some("sk-admin"), flag).as_deref(),
                Some("sk-admin"),
                "{flag:?}"
            );
        }
        for flag in ["disabled", "off", "false", "0", " Disabled "] {
            assert_eq!(
                parse_bootstrap_admin_key_flag(Some(flag)),
                Ok(false),
                "{flag}"
            );
            assert_eq!(bootstrap_admin_key_from(Some("sk-admin"), Some(flag)), None);
        }
        // A typo is an error, and accepts nothing.
        for flag in ["disable", "no", "yes", "2"] {
            let e = parse_bootstrap_admin_key_flag(Some(flag)).unwrap_err();
            assert!(e.contains("KOSHA_ADMIN_BOOTSTRAP_KEY"), "{e}");
            assert!(
                !e.contains(&format!("'{flag}'")),
                "the value is echoed: {e}"
            );
            assert_eq!(bootstrap_admin_key_from(Some("sk-admin"), Some(flag)), None);
        }
        // No key configured is no credential, whatever the flag.
        assert_eq!(bootstrap_admin_key_from(None, None), None);
        assert_eq!(bootstrap_admin_key_from(Some(""), Some("enabled")), None);
    }

    #[test]
    fn last_used_is_collected_once_per_use_window() {
        let store = KeyStore::with_keys(parse_env_keys(None, Some("boot")));
        store.replace_db_keys(vec![
            record("sk-a", "acme"),
            record("sk-idle", "acme"),
            ApiKeyRecord {
                expires_at: Some(100),
                ..record("sk-expired", "acme")
            },
        ]);
        assert!(store.take_last_used().is_empty());

        store.lookup_at("sk-a", 500).unwrap();
        store.lookup_at("sk-a", 700).unwrap();
        store.lookup_at("sk-a", 600).unwrap();
        // Env keys and rejected keys are not recorded.
        store.lookup_at("boot", 700).unwrap();
        store.lookup_at("sk-expired", 700).unwrap_err();

        assert_eq!(store.take_last_used(), vec![(hash_api_key("sk-a"), 700)]);
        assert!(store.take_last_used().is_empty());
    }
}
