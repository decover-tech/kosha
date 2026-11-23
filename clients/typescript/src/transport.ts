import { KoshaConnectionError, KoshaError, KoshaTimeoutError } from "./errors.js";
import {
  DEFAULT_RETRY,
  backoffMs,
  isIdempotent,
  retryableStatus,
  serverDelayMs,
  sleep,
  type RetryOptions,
} from "./retry.js";
import { VERSION } from "./version.js";

/** Per-call options; each overrides the client's setting for one call. */
export interface CallOptions {
  /** Cancels the call, including any sleep between attempts. */
  signal?: AbortSignal;
  /** Per-attempt timeout (default 30000 ms). */
  timeoutMs?: number;
  /** Attempts in total for this call. */
  maxAttempts?: number;
  /** Retry a timed-out or reset non-idempotent call (namespace create, schema put, ...). */
  retryNonIdempotent?: boolean;
  /** Extra request headers. */
  headers?: Record<string, string>;
}

export interface TransportOptions {
  baseUrl: string;
  apiKey: string;
  retry?: RetryOptions;
  timeoutMs?: number;
  fetch?: typeof globalThis.fetch;
  headers?: Record<string, string>;
}

// Failures that happen before the request is written: replaying them is
// safe for every operation (RETRY.md rule 3).
const CONNECT_CODES = new Set([
  "ECONNREFUSED",
  "ENOTFOUND",
  "EAI_AGAIN",
  "EHOSTUNREACH",
  "ENETUNREACH",
  "UND_ERR_CONNECT_TIMEOUT",
]);

function errorCode(err: unknown): string | undefined {
  for (let e: unknown = err, depth = 0; e && depth < 4; depth++) {
    const rec = e as { code?: unknown; cause?: unknown; errors?: unknown[] };
    if (typeof rec.code === "string") return rec.code;
    if (Array.isArray(rec.errors) && rec.errors.length > 0) {
      const inner = rec.errors[0] as { code?: unknown };
      if (typeof inner?.code === "string") return inner.code;
    }
    e = rec.cause;
  }
  return undefined;
}

function userAgent(): string {
  const p = (globalThis as { process?: { version?: string; platform?: string; arch?: string } }).process;
  const runtime = p?.version ? `node/${p.version.replace(/^v/, "")}` : "js";
  const os = p?.platform ? `${p.platform}/${p.arch ?? "unknown"}` : "unknown";
  return `kosha-typescript/${VERSION} (${runtime}; ${os})`;
}

/** The single HTTP path every SDK call takes: auth, timeouts, retries, typed errors. */
export class Transport {
  readonly baseUrl: string;
  private readonly apiKey: string;
  private readonly retry: Required<RetryOptions>;
  private readonly timeoutMs: number;
  private readonly fetchImpl: typeof globalThis.fetch;
  private readonly headers: Record<string, string>;
  private readonly ua = userAgent();

  constructor(opts: TransportOptions) {
    this.baseUrl = opts.baseUrl.replace(/\/+$/, "");
    this.apiKey = opts.apiKey;
    this.retry = { ...DEFAULT_RETRY, ...stripUndefined(opts.retry ?? {}) };
    this.timeoutMs = opts.timeoutMs ?? 30000;
    this.fetchImpl = opts.fetch ?? globalThis.fetch.bind(globalThis);
    this.headers = { ...(opts.headers ?? {}) };
  }

  async request<T>(method: string, path: string, body?: unknown, opts: CallOptions = {}): Promise<T> {
    const url = this.baseUrl + (path.startsWith("/") ? path : `/${path}`);
    const idempotent = isIdempotent(method, path);
    // Buffered once so every attempt replays the same bytes.
    const payload = body === undefined ? undefined : JSON.stringify(body);
    const headers: Record<string, string> = {
      ...this.headers,
      ...(opts.headers ?? {}),
      Authorization: `Bearer ${this.apiKey}`,
      Accept: "application/json",
      "User-Agent": this.ua,
    };
    if (payload !== undefined) headers["Content-Type"] = "application/json";
    const maxAttempts = Math.max(1, opts.maxAttempts ?? this.retry.maxAttempts);
    const timeoutMs = opts.timeoutMs ?? this.timeoutMs;
    const started = Date.now();

    for (let attempt = 1; ; attempt++) {
      opts.signal?.throwIfAborted();
      const timeout = AbortSignal.timeout(timeoutMs);
      const signal = opts.signal ? AbortSignal.any([opts.signal, timeout]) : timeout;

      let error: KoshaError;
      let retry: boolean;
      let serverDelay: number | undefined;
      try {
        const res = await this.fetchImpl(url, { method, headers, body: payload, signal });
        const text = await res.text();
        if (res.ok) return (text === "" ? {} : JSON.parse(text)) as T;
        let parsed: unknown = text;
        try {
          parsed = JSON.parse(text);
        } catch {
          // keep the raw text
        }
        serverDelay = serverDelayMs(parsed, res.headers);
        error = KoshaError.fromResponse(res.status, parsed, res.headers, serverDelay);
        retry = retryableStatus(res.status, parsed, idempotent);
      } catch (err) {
        if (opts.signal?.aborted) throw opts.signal.reason;
        if (err instanceof SyntaxError) {
          throw new KoshaError({ status: 200, message: `invalid JSON in response: ${err.message}`, attempts: attempt, cause: err });
        }
        const replaySafe = idempotent || opts.retryNonIdempotent === true;
        if (timeout.aborted) {
          error = new KoshaTimeoutError(`${method} ${path} timed out after ${timeoutMs} ms`, err);
          retry = replaySafe;
        } else {
          const code = errorCode(err);
          error = new KoshaConnectionError(`${method} ${path} failed: ${code ?? String(err)}`, err);
          retry = (code !== undefined && CONNECT_CODES.has(code)) || replaySafe;
        }
      }

      error.attempts = attempt;
      if (!retry || attempt >= maxAttempts) throw error;
      let delay: number;
      if (serverDelay !== undefined) {
        if (serverDelay > this.retry.maxRetryAfterMs) throw error;
        delay = serverDelay + Math.floor(Math.random() * 100);
      } else {
        delay = backoffMs(attempt - 1, this.retry);
      }
      if (Date.now() - started + delay > this.retry.totalBudgetMs) throw error;
      await sleep(delay, opts.signal);
    }
  }
}

function stripUndefined<T extends object>(obj: T): Partial<T> {
  return Object.fromEntries(Object.entries(obj).filter(([, v]) => v !== undefined)) as Partial<T>;
}
