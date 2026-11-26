# SDK retry policy

Normative for the Python (`kosha-client`), TypeScript (`@decover/kosha`) and
Go (`github.com/decover-tech/kosha/clients/go`) SDKs (ADR-351 §b). The
`retry.*` and `compat.*` cases in `cases/` pin it; a change here changes all
three SDKs and those cases together.

## When to retry

1. **The server says.** If the error body is JSON with a boolean
   `retryable`, retry if and only if it is `true`, for every operation. The
   server only says `true` when it did not apply the request
   (docs/api/errors.md). One exception wins over `retryable`: a body with
   `details.outcome: "unknown"` (a query pod forwarded the write and the
   ingest pod may have applied it) is handled like rule 4, retried only for
   idempotent operations unless the caller opts in. The server says
   `retryable: false` there; older builds said `true`.
2. **No `retryable` field** (an older server, a proxy, the HTTP layer):
   - 429 and 503: retry, for every operation (the request was rejected
     before it was applied);
   - 502 and 504: retry idempotent operations only;
   - anything else: never.
3. **No connection** (DNS failure, connection refused, TLS handshake):
   nothing was sent, so retry every operation.
4. **Timeout or reset after the request was sent:** retry idempotent
   operations only, unless the caller opts in (`retryNonIdempotent` /
   `WithRetryNonIdempotent()` / `retry_non_idempotent=True`). The caller's
   own cancellation (`signal`, `ctx`) is never retried.

## Idempotent operations

Every GET, search, exists, upsert (`POST .../documents`, keyed by the
required id), merge (`POST .../replace`), delete_by_query, flush and
namespace delete.

Not idempotent: namespace create, schema put, branch create and delete,
shards, and any route the SDK does not know (`request()` / `Do`).

## How long to wait

1. The body's `retry_after_ms` (millisecond precision, and it survives the
   query pod's forwarding of writes);
2. else the `Retry-After` header, as delta-seconds or an HTTP-date;
3. else full jitter: `rand(0, min(maxDelay, baseDelay * 2^retry))`, `retry`
   counting from 0.

A server-specified delay is used as given, plus up to 100 ms of jitter.

## When to give up

Stop at once, without sleeping, and surface the last error with its
attempt count when any of these holds:

- the server asks for more than `maxRetryAfter` (so a long quota window is
  reported, not slept through);
- the next sleep would end past `totalBudget`, measured from the first
  attempt;
- `maxAttempts` attempts have been made.

## Defaults

| setting | default | Python | TypeScript | Go |
|---|---|---|---|---|
| attempts, including the first | 3 | `max_retries=3` | `maxAttempts: 3` | `MaxAttempts: 3` |
| backoff base | 250 ms | `base_delay_ms` | `baseDelayMs` | `BaseDelay` |
| backoff cap | 8 s | `max_delay_ms` | `maxDelayMs` | `MaxDelay` |
| longest server delay honoured | 30 s | `max_retry_after_ms` | `maxRetryAfterMs` | `MaxRetryAfter` |
| total budget | 60 s | `total_budget_ms` | `totalBudgetMs` | `TotalBudget` |
| per-attempt timeout | 30 s (Python: 60 s) | `timeout` | `timeoutMs` | `WithTimeout` |

`max_retries` in Python keeps its historical meaning: it counts attempts,
so `max_retries=1` is fail-fast.

## Bodies

Request bodies are serialized once and replayed byte for byte on every
attempt.
