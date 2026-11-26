# Operational endpoints

These endpoints are for load balancers, Kubernetes and Prometheus. They are
permanent, unversioned, and deliberately left out of the public OpenAPI
document: their bodies are not part of the API contract (ADR-350 §c).

| Endpoint | Auth | Answer |
|---|---|---|
| `GET /healthz` (also `/v1/healthz`) | none | `200 {"status": "ok"}` while the process serves HTTP. Liveness. `/v1/healthz` is also the public `HealthCheck` operation (SDK `ping`). |
| `GET /readyz` (also `/v1/readyz`) | none | `200 {"status": "ready"}`, or `503 {"status": "warming", ...}` while a query pod prefetches its configured hot namespaces. Readiness. The `warming` body is not an `ErrorResponse`. |
| `GET /metrics` | none, or the admin key with `KOSHA_METRICS_AUTH=admin` | Prometheus text exposition; see [../metrics.md](../metrics.md). |
| `GET /dashboard` | none for the static page; its data calls use the API | Read-only UI with `KOSHA_DASHBOARD=on`; see [../dashboard.md](../dashboard.md). |

All of them are answered before authentication and on every role. They are
never affected by `KOSHA_LEGACY_ROUTES`. The Kubernetes manifests probe
`/v1/healthz` and `/v1/readyz`.
