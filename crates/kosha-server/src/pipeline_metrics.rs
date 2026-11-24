//! `/metrics` families of the usage pipeline (#348) and billing (#349).
//!
//! Both keep their counters as atomics next to the work
//! (`usage::PipelineStats`, `billing::BillingStats`); the collectors here
//! read them at scrape time, so recording costs nothing more than it did.
//! They are registered only on a pod where metering or billing is on, so
//! the families are absent elsewhere. Documented in `docs/metrics.md`.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use prometheus_client::collector::Collector;
use prometheus_client::encoding::{DescriptorEncoder, EncodeMetric};
use prometheus_client::metrics::counter::ConstCounter;
use prometheus_client::metrics::gauge::ConstGauge;
use prometheus_client::metrics::MetricType;

use crate::billing::{Billing, BillingStats};
use crate::metrics::Metrics;
use crate::usage::{PipelineStats, UsageMeter};

/// The server's metrics, with the usage and billing collectors of this
/// pod registered when they are on.
pub(crate) fn server_metrics(usage: Option<&UsageMeter>, billing: Option<&Billing>) -> Metrics {
    let mut metrics = Metrics::from_env();
    if let Some(usage) = usage {
        metrics.register_collector(Box::new(UsageCollector(usage.stats())));
    }
    if let Some(billing) = billing {
        metrics.register_collector(Box::new(BillingCollector(Arc::clone(&billing.stats))));
    }
    metrics
}

fn load(value: &AtomicU64) -> u64 {
    value.load(Ordering::Relaxed)
}

fn counter(
    encoder: &mut DescriptorEncoder,
    name: &str,
    help: &str,
    value: u64,
) -> Result<(), std::fmt::Error> {
    let metric = ConstCounter::new(value);
    metric.encode(encoder.encode_descriptor(name, help, None, metric.metric_type())?)
}

fn gauge(
    encoder: &mut DescriptorEncoder,
    name: &str,
    help: &str,
    value: u64,
) -> Result<(), std::fmt::Error> {
    let metric = ConstGauge::new(value as i64);
    metric.encode(encoder.encode_descriptor(name, help, None, metric.metric_type())?)
}

/// One counter family with one label.
fn labelled_counter(
    encoder: &mut DescriptorEncoder,
    name: &str,
    help: &str,
    label: &str,
    values: &[(&str, u64)],
) -> Result<(), std::fmt::Error> {
    let mut family = encoder.encode_descriptor(name, help, None, MetricType::Counter)?;
    for (value_label, value) in values {
        let labels = [(label, *value_label)];
        ConstCounter::new(*value).encode(family.encode_family(&labels)?)?;
    }
    Ok(())
}

/// The usage pipeline's counters.
#[derive(Debug)]
pub(crate) struct UsageCollector(pub Arc<PipelineStats>);

impl Collector for UsageCollector {
    fn encode(&self, mut encoder: DescriptorEncoder) -> Result<(), std::fmt::Error> {
        let s = &self.0;
        counter(
            &mut encoder,
            "kosha_usage_events_emitted",
            "Usage events this pod produced (spooled or dropped).",
            load(&s.emitted),
        )?;
        counter(
            &mut encoder,
            "kosha_usage_events_spooled",
            "Usage events appended to the local spool.",
            load(&s.spooled),
        )?;
        counter(
            &mut encoder,
            "kosha_usage_events_delivered",
            "Usage events the usage store accepted (flushed).",
            load(&s.delivered),
        )?;
        counter(
            &mut encoder,
            "kosha_usage_events_dropped",
            "Usage events dropped because the spool was full or unwritable.",
            load(&s.dropped),
        )?;
        counter(
            &mut encoder,
            "kosha_usage_spool_malformed_lines",
            "Spool lines that did not parse when delivered (a write cut short by a crash).",
            load(&s.malformed_lines),
        )?;
        counter(
            &mut encoder,
            "kosha_usage_sink_errors",
            "Spool flushes that failed (usage store unreachable or a spool file error).",
            load(&s.sink_errors),
        )?;
        gauge(
            &mut encoder,
            "kosha_usage_spool_bytes",
            "Bytes of usage events waiting in the spool.",
            load(&s.spool_bytes),
        )?;
        gauge(
            &mut encoder,
            "kosha_usage_last_delivery_timestamp_seconds",
            "Unix time of the last flush that delivered the whole spool; 0 if none yet.",
            load(&s.last_flush_ok_ms) / 1_000,
        )?;
        counter(
            &mut encoder,
            "kosha_usage_snapshot_runs",
            "Runs of the hourly stored-bytes and vector-dimension snapshot tick.",
            load(&s.snapshot_runs),
        )?;
        labelled_counter(
            &mut encoder,
            "kosha_usage_snapshots",
            "Namespace snapshots emitted, by meter.",
            "meter",
            &[
                ("stored_bytes", load(&s.stored_snapshots)),
                ("vector_dimensions", load(&s.vector_snapshots)),
            ],
        )?;
        counter(
            &mut encoder,
            "kosha_usage_snapshot_failures",
            "Namespace snapshots a tick could not take (listing or footer read failed); \
             retried next tick.",
            load(&s.snapshot_failures),
        )?;
        counter(
            &mut encoder,
            "kosha_usage_footer_reads",
            "Segment footers read for the vector-dimensions meter (once per segment).",
            load(&s.footer_reads),
        )?;
        Ok(())
    }
}

/// Billing's counters.
#[derive(Debug)]
pub(crate) struct BillingCollector(pub Arc<BillingStats>);

impl Collector for BillingCollector {
    fn encode(&self, mut encoder: DescriptorEncoder) -> Result<(), std::fmt::Error> {
        let s = &self.0;
        labelled_counter(
            &mut encoder,
            "kosha_billing_webhook_events",
            "Stripe webhook deliveries, by outcome.",
            "outcome",
            &[
                ("processed", load(&s.webhooks_processed)),
                ("duplicate", load(&s.webhooks_duplicate)),
                ("bad_signature", load(&s.webhooks_bad_signature)),
                ("invalid", load(&s.webhooks_invalid)),
                ("failed", load(&s.webhooks_failed)),
            ],
        )?;
        labelled_counter(
            &mut encoder,
            "kosha_billing_scheduler_runs",
            "Billing scheduler ticks, by result.",
            "result",
            &[
                ("ok", load(&s.scheduler_ran)),
                ("not_leader", load(&s.scheduler_not_leader)),
                ("failed", load(&s.scheduler_failed)),
            ],
        )?;
        gauge(
            &mut encoder,
            "kosha_billing_scheduler_last_run_timestamp_seconds",
            "Unix time of the last scheduler tick this pod ran as the leader; 0 if none.",
            load(&s.scheduler_last_run_ms) / 1_000,
        )?;
        labelled_counter(
            &mut encoder,
            "kosha_billing_notices",
            "Dunning notices, by outcome.",
            "outcome",
            &[
                ("sent", load(&s.notices_sent)),
                ("failed", load(&s.notices_failed)),
            ],
        )?;
        counter(
            &mut encoder,
            "kosha_billing_payment_retries",
            "Payments retried by dunning.",
            load(&s.payment_retries),
        )?;
        counter(
            &mut encoder,
            "kosha_billing_payments_collected",
            "Invoices a dunning retry collected.",
            load(&s.payments_collected),
        )?;
        labelled_counter(
            &mut encoder,
            "kosha_billing_catalog_reloads",
            "Plan catalog reloads, by outcome.",
            "outcome",
            &[
                ("applied", load(&s.catalog_reloads)),
                ("rejected", load(&s.catalog_reloads_rejected)),
            ],
        )?;
        counter(
            &mut encoder,
            "kosha_billing_refresh_failures",
            "Standings refreshes that failed (the previous standings stayed in force).",
            load(&s.refresh_failures),
        )?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::tests::{parse_exposition, value};
    use crate::metrics::Scrape;

    #[test]
    fn the_families_render_as_valid_exposition() {
        let mut metrics = Metrics::default();
        let usage = Arc::new(PipelineStats::default());
        usage.spooled.store(7, Ordering::Relaxed);
        usage.dropped.store(2, Ordering::Relaxed);
        usage.spool_bytes.store(1_024, Ordering::Relaxed);
        usage.vector_snapshots.store(3, Ordering::Relaxed);
        let billing = Arc::new(BillingStats::default());
        billing.webhooks_bad_signature.store(4, Ordering::Relaxed);
        billing.notices_sent.store(5, Ordering::Relaxed);
        metrics.register_collector(Box::new(UsageCollector(usage)));
        metrics.register_collector(Box::new(BillingCollector(billing)));
        let text = metrics.render(&Scrape::default());
        let samples = parse_exposition(&text).unwrap();
        let get = |name: &str, labels: &[(&str, &str)]| value(&samples, name, labels);
        assert_eq!(get("kosha_usage_events_spooled_total", &[]), Some(7.0));
        assert_eq!(get("kosha_usage_events_dropped_total", &[]), Some(2.0));
        assert_eq!(get("kosha_usage_spool_bytes", &[]), Some(1_024.0));
        assert_eq!(
            get(
                "kosha_usage_snapshots_total",
                &[("meter", "vector_dimensions")]
            ),
            Some(3.0)
        );
        assert_eq!(
            get(
                "kosha_billing_webhook_events_total",
                &[("outcome", "bad_signature")]
            ),
            Some(4.0)
        );
        assert_eq!(
            get("kosha_billing_notices_total", &[("outcome", "sent")]),
            Some(5.0)
        );
        // Every family documented in docs/metrics.md.
        let doc = include_str!("../../../docs/metrics.md");
        for sample in &samples {
            let family = sample.name.trim_end_matches("_total");
            if family.starts_with("kosha_usage_") || family.starts_with("kosha_billing_") {
                assert!(
                    doc.contains(&sample.name),
                    "{} is not in docs/metrics.md",
                    sample.name
                );
            }
        }
    }
}
