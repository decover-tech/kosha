// The retry policy (clients/conformance/RETRY.md). It is the same in the
// Python, TypeScript and Go SDKs, and the shared conformance cases pin it.
import { isRecord } from "./errors.js";

export interface RetryOptions {
  /** Attempts in total, including the first (default 3). 1 disables retries. */
  maxAttempts?: number;
  /** Backoff base for full jitter (default 250 ms). */
  baseDelayMs?: number;
  /** Backoff ceiling (default 8000 ms). */
  maxDelayMs?: number;
  /** Give up at once when the server asks to wait longer (default 30000 ms). */
  maxRetryAfterMs?: number;
  /** Give up when the next sleep would pass this, from the first attempt (default 60000 ms). */
  totalBudgetMs?: number;
}

export const DEFAULT_RETRY: Required<RetryOptions> = {
  maxAttempts: 3,
  baseDelayMs: 250,
  maxDelayMs: 8000,
  maxRetryAfterMs: 30000,
  totalBudgetMs: 60000,
};

// POST actions under /v1/namespaces/{ns}/ that are safe to replay: upserts
// are keyed by the required id, the rest read or converge.
const IDEMPOTENT_ACTIONS = new Set(["search", "exists", "documents", "replace", "delete", "flush"]);

/**
 * Whether replaying `method path` cannot change the outcome. Idempotent:
 * every GET, search, exists, upsert, merge, delete_by_query, flush and
 * namespace delete. Not: namespace create, schema put, branch create and
 * delete, shards, and any route this table does not know.
 */
export function isIdempotent(method: string, path: string): boolean {
  const m = method.toUpperCase();
  if (m === "GET" || m === "HEAD") return true;
  const parts = path.split("?")[0]!.split("/").filter(Boolean);
  if (parts[0] !== "v1" || parts[1] !== "namespaces") return false;
  if (m === "DELETE") return parts.length === 3;
  if (m === "POST" && parts.length === 4) return IDEMPOTENT_ACTIONS.has(parts[3]!);
  return false;
}

/** Whether an HTTP error response may be retried (RETRY.md rules 1 and 2). */
export function retryableStatus(status: number, body: unknown, idempotent: boolean): boolean {
  if (isRecord(body) && typeof body.retryable === "boolean") return body.retryable;
  if (status === 429 || status === 503) return true;
  if (status === 502 || status === 504) return idempotent;
  return false;
}

/**
 * The delay the server asked for: the body's `retry_after_ms`, else the
 * `Retry-After` header (delta-seconds or an HTTP-date). Undefined when it
 * asked for none.
 */
export function serverDelayMs(body: unknown, headers: Headers, now: number = Date.now()): number | undefined {
  if (isRecord(body) && typeof body.retry_after_ms === "number" && body.retry_after_ms >= 0) {
    return body.retry_after_ms;
  }
  return parseRetryAfter(headers.get("retry-after"), now);
}

export function parseRetryAfter(value: string | null, now: number = Date.now()): number | undefined {
  if (value === null) return undefined;
  const v = value.trim();
  if (/^\d+$/.test(v)) return Number(v) * 1000;
  const at = Date.parse(v);
  if (Number.isNaN(at)) return undefined;
  return Math.max(0, at - now);
}

/** Full jitter: rand(0, min(maxDelayMs, baseDelayMs * 2^retry)), retry counting from 0. */
export function backoffMs(retry: number, policy: Required<RetryOptions>, random: () => number = Math.random): number {
  const cap = Math.min(policy.maxDelayMs, policy.baseDelayMs * 2 ** retry);
  return Math.floor(random() * cap);
}

/** Sleeps `ms`, rejecting with the signal's reason if it aborts first. */
export function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) return reject(signal.reason);
    const timer = setTimeout(() => {
      signal?.removeEventListener("abort", onAbort);
      resolve();
    }, ms);
    const onAbort = () => {
      clearTimeout(timer);
      reject(signal!.reason);
    };
    signal?.addEventListener("abort", onAbort, { once: true });
  });
}
