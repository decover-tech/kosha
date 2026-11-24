# Managed-service operations starter kit

This is an operator-only starting point, not a customer SLA. It does not deploy monitoring, set
commercial entitlements, publish a status site, or assign people to on-call.

## Draft search SLOs

Map the deployment's configured [billing plans](../configuration.md#billing) to these draft
classes. Kosha has no built-in plan names. Validate the targets with production-like traffic before
publishing them.

| Plan class | Search availability | Successful searches within latency threshold |
| --- | --- | --- |
| Free / trial | 99.0% | 95% within 1 s |
| Paid | 99.9% | 95% within 250 ms |

Use a rolling 30-day window. Measure at the public gateway, after authentication and plan resolution:

- **Availability:** successful valid searches divided by all valid searches. Include service
  overload, server errors, and gateway timeouts as failures; exclude invalid requests and documented
  customer quota or entitlement refusals. A 429 alone does not distinguish service overload from a
  tenant exceeding its plan limits.
- **Latency:** successful valid searches at or below the plan threshold divided by all successful
  valid searches. Include cold searches and result-cache hits. Put plan thresholds in the gateway's
  histogram bucket boundaries.
- Aggregate request counts by the finite configured plan mapping, not by averaging tenant ratios or
  pod percentiles. No qualifying requests means no observation, not 100% availability. Track
  synthetic public-path checks separately to detect outages with no incoming traffic.

Kosha's [internal metrics](../metrics.md) do not carry plan, and search `outcome="error"` includes
client errors. Do not treat it as a service failure counter or derive contractual budget alerts
from it. Gateway instrumentation, plan dashboards and SLO burn-rate alerts are rollout prerequisites.

## Prometheus and tenant dashboard

Add [alerts.yml](alerts.yml) to Prometheus's `rule_files`; do not load the test fixture as a rule.
Use the existing [pod scrape settings](../metrics.md#access) and attach a unique `cluster` label to
scraped time series for each Kosha deployment before ingestion. A Prometheus external label alone
does not label local queries. Scrape each pod once; don't mix deployments or drop some tenant cache
series when using these cluster aggregates.

Use an internal datasource and restricted operator dashboard. Start with tenant labels disabled.
For a pilot with a known, bounded number of tenants, set `KOSHA_METRICS_TENANT_LABELS=on` and
`KOSHA_METRICS_AUTH=admin` on the pods and configure the scraper's credential via a secret. Keep
`KOSHA_METRICS_NAMESPACE_LABELS` off. Review active-series usage per deployment and set a scrape
sample limit above the measured peak as a last resort; hitting it fails the whole scrape, so monitor
scrape health. Disable tenant labels if the cohort cannot be kept within the series budget.

Create Grafana panels with the following PromQL, or use the same queries in Prometheus with literal
values. Use single-select `cluster` and `tenant` variables from operator-maintained lists, with no
“All” option. These filters are for navigation, not access control. There are no tenant-labelled
latency or hydration counters; compare the tenant's cache pressure against the cluster hydration
signal rather than attributing the latter to a tenant.

| Panel | PromQL |
| --- | --- |
| Cache footprint (bytes) | `sum by (cache) (kosha_cache_bytes{cluster=~"${cluster:regex}",tenant=~"${tenant:regex}",tenant!=""})` |
| NVMe hit and miss rates (two queries) | `sum(rate(kosha_cache_hits_total{cluster=~"${cluster:regex}",tenant=~"${tenant:regex}",tenant!="",cache="nvme"}[5m]))` and `sum(rate(kosha_cache_misses_total{cluster=~"${cluster:regex}",tenant=~"${tenant:regex}",tenant!="",cache="nvme"}[5m]))` |
| Evictions per second | `sum by (cache) (rate(kosha_cache_evictions_total{cluster=~"${cluster:regex}",tenant=~"${tenant:regex}",tenant!=""}[5m]))` |
| Limit refusals per second | `sum by (reason) (rate(kosha_tenant_rejections_total{cluster=~"${cluster:regex}",tenant=~"${tenant:regex}",tenant!=""}[5m]))` |
| Deployment hydration p95 (seconds) | `histogram_quantile(0.95, sum by (le) (rate(kosha_search_phase_seconds_bucket{cluster=~"${cluster:regex}",phase="hydrate"}[5m])))` |

The rules are warning-level starting thresholds, not pages or statements of user impact. Group
notifications by deployment and alert name and use a private destination for any tenant alert.
The tenant churn alert is absent if tenant metrics are disabled. Watch scraper failure, absence
of expected targets and public reachability separately so missing data does not look healthy.

### Compaction backlog

Kosha does not expose a native compaction backlog gauge. For the optional backlog rule, an
operator collector must query the scheduler pod's authenticated `GET /v1/admin/compaction-status`
(the dedicated indexer if deployed, otherwise the scheduler-enabled writer), and publish this
aggregate gauge. Given a successful response body, the transformation is:

```sh
jq -er '
  select(.scheduler.enabled == true and (.namespaces | type) == "array")
  | [.namespaces[] | select(.needs_compaction == true)]
  | "kosha_ops_compaction_namespaces_needing_compaction \(length)"
'
```

Give the collector's scrape target the same `cluster` label. On a failed poll, report collector
failure and stop serving the stale sample; don't write zero. Monitor collector scrape and poll
health independently. The alert means the deployment has continuously had outstanding work
for 30 minutes, not that a specific namespace has been stuck for that long. Tune the alert against
the scheduler interval and normal ingest volume.

## Runbooks

| Signal or symptom | Verify | Mitigate and verify recovery |
| --- | --- | --- |
| Cold-hydration storm / search shedding | Compare hydration p95, S3 fetch rate, hydration lease timeouts and queue rejections with deploys or replica turnover. Check cache misses and pod disk usage. | Pause simultaneous query-pod turnover; retain warm replicas and let controlled warm-up finish. Confirm hydration and search shedding return to baseline before resuming the rollout. |
| Cache churn / inline span writes | Check per-pod disk free space and I/O, cache bytes, miss and eviction rates. Check whether eviction activity is a deletion sweep rather than capacity pressure. | Relieve disk pressure or resize only within available disk capacity. Confirm miss rate, eviction rate and inline writes subside, and search latency recovers. |
| Compaction backlog | On the scheduler pod inspect scheduler ticks and enabled state, namespace `needs_compaction` and segment counts, and recent job failures. Check storage headroom and store connectivity. | Restore the scheduler or dependency first. Don't start concurrent full merges as a first response; assess merge size, disk headroom and existing jobs before considering a tiered pass. Confirm jobs complete and segment counts trend down. |
| S3 throttling | Correlate `SlowDown`/503s in dependency logs or provider metrics with fetches, deployment churn, and compaction. | Reduce competing background work and stagger warm-up or rollouts. Confirm throttling declines and customer read and write probes recover before restoring load. |
| Postgres failover | Check the provider's writer endpoint and failover state, Kosha readiness, connection errors and writer fencing. | Restore connectivity to the elected writer. Don't bypass fencing or promote a second writer. Confirm authenticated search, an isolated write/flush/read-back probe, and compaction job progress before resolving. |

## On-call and public status

Before rollout, the service owner configures a weekly primary and secondary rotation in the team's
paging system, with a handoff covering active incidents, deploys and dependency changes. Exercise
notification delivery and name the escalation contact for cloud/database issues. Suggested practice:
primary acknowledges customer-impacting pages within 15 minutes, secondary takes over on a missed
acknowledgment, and the first responder names an incident commander for a sustained incident.

Host a public status page outside the serving cluster with Search and Indexing components by region.
Use a private incident channel for evidence and coordination. For confirmed customer impact, post
the affected components and regions, known symptoms, UTC start time and next update time. Post an
update every 30 minutes while impact continues; use Investigating, Identified, Monitoring and
Resolved as applicable. Do not post tenant IDs, internal errors, credentials or dashboards. Resolve
once public probes and the relevant workload have recovered and remained stable; record timestamps,
root cause and owned follow-up actions internally.

Validate the bundled alerts from the repository root:

```sh
promtool check rules docs/operations/alerts.yml
promtool test rules docs/operations/alerts.test.yml
```
