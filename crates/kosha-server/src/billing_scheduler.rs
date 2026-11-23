//! The billing scheduler (#349): runs the monthly close, the payment sync
//! and dunning on its own, so no CronJob has to call the admin routes. Off
//! unless `KOSHA_BILLING_SCHEDULER=on`.
//!
//! Every pod with it on wakes every `KOSHA_BILLING_SCHEDULER_INTERVAL_SECS`
//! and tries to take the `billing-scheduler` lease in the billing ledger
//! (Postgres `kosha.billing_leases`; three intervals long, renewed by the
//! holder each tick). Only the holder runs a tick, so in a fleet one pod
//! does the work and another takes over within three intervals of it
//! stopping. Without Postgres the lease is per pod, which is right for a
//! single pod. A tick ([`tick`]):
//!
//! 1. delivers this pod's spooled usage;
//! 2. closes last month once `KOSHA_BILLING_CLOSE_DELAY_SECS` have passed
//!    since it ended (time for every pod to deliver its usage), unless the
//!    ledger records that month closed with no errors. A close with
//!    errors is run again next tick;
//! 3. syncs payments from the provider every
//!    `KOSHA_BILLING_SYNC_INTERVAL_SECS` (the fallback for a missed
//!    webhook);
//! 4. runs a dunning pass;
//! 5. rebuilds the standings.
//!
//! Every step is idempotent (one invoice per tenant and month, statuses
//! only leave `open`, notices are claimed before they are sent), so a tick
//! that runs twice (two pods whose clocks disagree about the lease, a
//! restart mid-tick) does no harm.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use kosha_control::billing::{BillingPeriod, CloseReport, SyncReport};
use kosha_control::dunning::DunningReport;
use serde::Serialize;

use crate::billing::Billing;
use crate::AppState;

const LEASE: &str = "billing-scheduler";

/// Settings of the scheduler.
#[derive(Debug, Clone)]
pub(crate) struct SchedulerConfig {
    pub interval: Duration,
    /// How long after a month ends it is closed.
    pub close_delay_ms: u64,
    pub sync_interval_ms: u64,
    /// This pod's name on the lease.
    pub holder: String,
}

fn env_secs(name: &str, default: u64) -> Result<u64, String> {
    match std::env::var(name).ok().map(|v| v.trim().to_string()) {
        None => Ok(default),
        Some(v) if v.is_empty() => Ok(default),
        Some(v) => v
            .parse::<u64>()
            .map_err(|_| format!("{name} must be a number of seconds")),
    }
}

impl SchedulerConfig {
    /// `None` unless `KOSHA_BILLING_SCHEDULER` is on.
    pub(crate) fn from_env() -> Result<Option<Self>, String> {
        let on = std::env::var("KOSHA_BILLING_SCHEDULER")
            .ok()
            .is_some_and(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "on" | "true" | "1"));
        if !on {
            return Ok(None);
        }
        let interval = env_secs("KOSHA_BILLING_SCHEDULER_INTERVAL_SECS", 900)?.max(10);
        Ok(Some(Self {
            interval: Duration::from_secs(interval),
            close_delay_ms: env_secs("KOSHA_BILLING_CLOSE_DELAY_SECS", 7_200)?
                .saturating_mul(1_000),
            sync_interval_ms: env_secs("KOSHA_BILLING_SYNC_INTERVAL_SECS", 3_600)?
                .saturating_mul(1_000),
            holder: Self::holder_name(),
        }))
    }

    /// The pod's host name, made unique per process.
    pub(crate) fn holder_name() -> String {
        format!(
            "{}-{}-{:x}",
            std::env::var("HOSTNAME").unwrap_or_else(|_| "kosha".into()),
            std::process::id(),
            crate::unix_millis()
        )
    }

    fn lease_ttl_ms(&self) -> u64 {
        (self.interval.as_millis() as u64)
            .saturating_mul(3)
            .max(60_000)
    }
}

/// What one tick did.
#[derive(Debug, Clone, Default, Serialize)]
pub(crate) struct TickReport {
    /// Whether this pod held the lease and ran the tick.
    pub leader: bool,
    pub closed: Option<CloseReport>,
    pub synced: Option<SyncReport>,
    pub dunning: Option<DunningReport>,
    pub errors: Vec<String>,
}

/// One scheduler tick at `billing`'s clock. See the module doc.
pub(crate) fn tick(state: &AppState, billing: &Billing, config: &SchedulerConfig) -> TickReport {
    let mut report = TickReport::default();
    let stats = &billing.stats;
    let now_ms = billing.now_ms();
    match billing
        .ledger
        .try_lease(LEASE, &config.holder, now_ms, config.lease_ttl_ms())
    {
        Ok(true) => report.leader = true,
        Ok(false) => {
            stats.scheduler_not_leader.fetch_add(1, Ordering::Relaxed);
            return report;
        }
        Err(e) => {
            stats.scheduler_failed.fetch_add(1, Ordering::Relaxed);
            report.errors.push(format!("lease: {e}"));
            return report;
        }
    }
    if let Some(meter) = state.usage.as_ref() {
        let _ = meter.flush();
    }

    // 2. Last month, once the delay has passed.
    let catalog = billing.catalog();
    let period = BillingPeriod::containing(now_ms).previous();
    let job = format!("close-period/{period}");
    let due = now_ms >= period.end_ms().saturating_add(config.close_delay_ms);
    match billing.ledger.job_done(&job) {
        Ok(false) if due => match billing.biller(&catalog).close_period(period, now_ms) {
            Ok(closed) => {
                if closed.errors.is_empty() {
                    let detail = serde_json::json!({
                        "created": closed.created.len(),
                        "existing": closed.existing,
                        "submitted": closed.submitted,
                    })
                    .to_string();
                    if let Err(e) = billing.ledger.mark_job_done(&job, now_ms, &detail) {
                        report.errors.push(format!("{job}: {e}"));
                    }
                } else {
                    report
                        .errors
                        .push(format!("{job}: {} tenant(s) failed", closed.errors.len()));
                }
                report.closed = Some(closed);
            }
            Err(e) => report.errors.push(format!("{job}: {e}")),
        },
        Ok(_) => {}
        Err(e) => report.errors.push(format!("{job}: {e}")),
    }

    // 3. Polling fallback for missed webhooks.
    if billing.provider.is_some() {
        let last = stats.last_sync_ms.load(Ordering::Relaxed);
        if last == 0 || now_ms >= last.saturating_add(config.sync_interval_ms) {
            match billing.biller(&catalog).sync_payments(now_ms) {
                Ok(synced) => {
                    stats.last_sync_ms.store(now_ms, Ordering::Relaxed);
                    report.synced = Some(synced);
                }
                Err(e) => report.errors.push(format!("sync: {e}")),
            }
        }
    }

    // 4. Dunning.
    match billing.run_dunning() {
        Ok(dunning) => report.dunning = Some(dunning),
        Err(e) => report.errors.push(format!("dunning: {e}")),
    }

    // 5. Enforce what changed.
    billing.refresh_logged();
    if report.errors.is_empty() {
        stats.scheduler_ran.fetch_add(1, Ordering::Relaxed);
    } else {
        stats.scheduler_failed.fetch_add(1, Ordering::Relaxed);
        for error in &report.errors {
            eprintln!("WARN: billing scheduler: {error}");
        }
    }
    stats
        .scheduler_last_run_ms
        .store(crate::unix_millis(), Ordering::Relaxed);
    report
}

impl AppState {
    /// Run the billing scheduler for the life of the process, when it is
    /// configured.
    pub(crate) fn spawn_billing_scheduler(self: &Arc<Self>) {
        let Some(billing) = self.billing.clone() else {
            return;
        };
        let Some(config) = billing.scheduler.clone() else {
            return;
        };
        let state = Arc::clone(self);
        println!(
            "billing scheduler: on (every {}s, close {}s after a month ends, sync every {}s, \
             lease holder {})",
            config.interval.as_secs(),
            config.close_delay_ms / 1_000,
            config.sync_interval_ms / 1_000,
            config.holder
        );
        let _ = std::thread::Builder::new()
            .name("kosha-billing-scheduler".into())
            .spawn(move || loop {
                tick(&state, &billing, &config);
                std::thread::sleep(config.interval);
            });
    }
}
