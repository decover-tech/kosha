import type { ErrorCode } from "./types.js";

export interface KoshaErrorInit {
  status: number;
  code?: string;
  message: string;
  retryable?: boolean;
  retryAfterMs?: number;
  requestId?: string;
  details?: Record<string, unknown>;
  attempts?: number;
  body?: unknown;
  cause?: unknown;
}

/**
 * A failed call. `code` is the server's stable error code (docs/api/errors.md),
 * else the deprecated `reason` of a pre-0.10 tenant-limit rejection, else
 * `"unknown"`. Branch on `code` or `status`, never on `message`.
 */
export class KoshaError extends Error {
  /** HTTP status; 0 when no response arrived. */
  readonly status: number;
  /** Open union: codes are added without a new API version. */
  readonly code: ErrorCode | (string & {});
  readonly retryable: boolean;
  /** The delay the server asked for, when it asked for one. */
  readonly retryAfterMs: number | undefined;
  readonly requestId: string | undefined;
  readonly details: Record<string, unknown> | undefined;
  /** How many attempts were made before giving up. */
  attempts: number;
  /** The parsed JSON body, or the raw text when it was not JSON. */
  readonly body: unknown;

  constructor(init: KoshaErrorInit) {
    super(init.message, init.cause === undefined ? undefined : { cause: init.cause });
    this.name = "KoshaError";
    this.status = init.status;
    this.code = init.code ?? "unknown";
    this.retryable = init.retryable ?? false;
    this.retryAfterMs = init.retryAfterMs;
    this.requestId = init.requestId;
    this.details = init.details;
    this.attempts = init.attempts ?? 1;
    this.body = init.body;
  }

  /** Builds the error for a non-2xx response. */
  static fromResponse(
    status: number,
    body: unknown,
    headers: Headers,
    retryAfterMs: number | undefined,
  ): KoshaError {
    const obj = isRecord(body) ? body : undefined;
    const str = (v: unknown) => (typeof v === "string" && v !== "" ? v : undefined);
    const code = str(obj?.code) ?? str(obj?.reason) ?? "unknown";
    const message = str(obj?.error) ?? `HTTP ${status}`;
    return new KoshaError({
      status,
      code,
      message,
      retryable: obj?.retryable === true,
      retryAfterMs,
      requestId: str(obj?.request_id) ?? headers.get("x-request-id") ?? undefined,
      details: isRecord(obj?.details) ? obj.details : undefined,
      body,
    });
  }
}

/** No response: DNS, refused or reset connections, TLS failures. */
export class KoshaConnectionError extends KoshaError {
  constructor(message: string, cause: unknown) {
    super({ status: 0, message, cause });
    this.name = "KoshaConnectionError";
  }
}

/** The per-attempt timeout (`timeoutMs`) elapsed. */
export class KoshaTimeoutError extends KoshaError {
  constructor(message: string, cause?: unknown) {
    super({ status: 0, message, cause });
    this.name = "KoshaTimeoutError";
  }
}

/** Bad SDK configuration or arguments; nothing was sent. */
export class KoshaConfigError extends KoshaError {
  constructor(message: string) {
    super({ status: 0, message, attempts: 0 });
    this.name = "KoshaConfigError";
  }
}

export function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
