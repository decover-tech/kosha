import { afterEach, describe, expect, it } from "vitest";
import { Kosha, KoshaConfigError, KoshaConnectionError, KoshaError, KoshaTimeoutError, isIdempotent } from "../src/index.js";
import { parseRetryAfter, retryableStatus } from "../src/retry.js";
import { errorBody, fakeServer, json, type FakeServer } from "./fake-server.js";

let server: FakeServer | undefined;
afterEach(async () => {
  await server?.close();
  server = undefined;
});

function client(url: string, extra: Record<string, unknown> = {}) {
  return new Kosha({ baseUrl: url, apiKey: "sk-test", retry: { baseDelayMs: 5, maxDelayMs: 20 }, ...extra });
}

async function caught(p: Promise<unknown>): Promise<KoshaError> {
  try {
    await p;
  } catch (e) {
    return e as KoshaError;
  }
  throw new Error("expected the call to fail");
}

describe("retry policy", () => {
  it("replays an outcome-unknown write only when that is safe", () => {
    const unknown = { retryable: true, details: { outcome: "unknown" } };
    expect(retryableStatus(503, unknown, false)).toBe(false);
    expect(retryableStatus(503, unknown, true)).toBe(true);
    expect(retryableStatus(503, { ...unknown, retryable: false }, true)).toBe(true);
    expect(retryableStatus(503, unknown, false, true)).toBe(true);
    expect(retryableStatus(503, { retryable: true }, false)).toBe(true);
  });

  it("honours Retry-After on a 429, then succeeds", async () => {
    let n = 0;
    server = await fakeServer((_req, res) => {
      if (n++ === 0) return json(res, 429, { error: "slow down" }, { "retry-after": "1" });
      json(res, 200, { results: [], total_hits: 0 });
    });
    const started = Date.now();
    const out = await client(server.url).namespace("ns").search({ query_text: "x" });
    expect(out.total_hits).toBe(0);
    expect(server.requests).toHaveLength(2);
    expect(Date.now() - started).toBeGreaterThanOrEqual(950);
  });

  it("does not retry a retryable:false 429 (a quota)", async () => {
    server = await fakeServer((_req, res) => json(res, 429, errorBody("namespace_quota_exceeded", false)));
    const err = await caught(client(server.url).namespace("ns").search({ query_text: "x" }));
    expect(server.requests).toHaveLength(1);
    expect(err.status).toBe(429);
    expect(err.code).toBe("namespace_quota_exceeded");
    expect(err.retryable).toBe(false);
    expect(err.attempts).toBe(1);
  });

  it("backs off on bare 503s and succeeds on the third attempt", async () => {
    let n = 0;
    server = await fakeServer((_req, res) => {
      if (n++ < 2) return json(res, 503, { error: "unavailable" });
      json(res, 200, { status: "flushed" });
    });
    const out = await client(server.url).namespace("ns").flush();
    expect(out.status).toBe("flushed");
    expect(server.requests).toHaveLength(3);
  });

  it("stops after maxAttempts with attempts set", async () => {
    server = await fakeServer((_req, res) => json(res, 503, errorBody("unavailable", true, { retry_after_ms: 10 })));
    const err = await caught(client(server.url).listNamespaces());
    expect(server.requests).toHaveLength(3);
    expect(err.attempts).toBe(3);
    expect(err.code).toBe("unavailable");
  });

  it("gives up at once when the server asks for more than maxRetryAfterMs", async () => {
    server = await fakeServer((_req, res) => json(res, 429, { error: "later" }, { "retry-after": "120" }));
    const started = Date.now();
    const err = await caught(client(server.url).listNamespaces());
    expect(server.requests).toHaveLength(1);
    expect(err.retryAfterMs).toBe(120000);
    expect(Date.now() - started).toBeLessThan(1000);
  });

  it("prefers the body's retry_after_ms over the header", async () => {
    let n = 0;
    server = await fakeServer((_req, res) => {
      if (n++ === 0) return json(res, 429, errorBody("tenant_qps_exceeded", true, { retry_after_ms: 50 }), { "retry-after": "120" });
      json(res, 200, { namespaces: [] });
    });
    const started = Date.now();
    await client(server.url).listNamespaces();
    expect(server.requests).toHaveLength(2);
    expect(Date.now() - started).toBeLessThan(1000);
  });

  it("stops when the next sleep would overrun totalBudgetMs", async () => {
    server = await fakeServer((_req, res) => json(res, 503, errorBody("unavailable", true, { retry_after_ms: 500 })));
    const err = await caught(
      new Kosha({ baseUrl: server.url, apiKey: "k", retry: { totalBudgetMs: 200 } }).listNamespaces(),
    );
    expect(server.requests).toHaveLength(1);
    expect(err.attempts).toBe(1);
  });

  it("retries 502 only for idempotent operations", async () => {
    server = await fakeServer((_req, res) => json(res, 502, { error: "bad gateway" }));
    await caught(client(server.url).createNamespace("a"));
    expect(server.requests).toHaveLength(1);
    await caught(client(server.url).listNamespaces());
    expect(server.requests).toHaveLength(4);
  });

  it("never retries a 4xx without retryable", async () => {
    server = await fakeServer((_req, res) => json(res, 400, errorBody("invalid_argument", false)));
    const err = await caught(client(server.url).namespace("n").search({ query_text: "" }));
    expect(err.status).toBe(400);
    expect(server.requests).toHaveLength(1);
  });

  it("retries a timeout only for idempotent operations", async () => {
    server = await fakeServer(async (_req, res) => {
      await new Promise((r) => setTimeout(r, 300));
      json(res, 200, {});
    });
    const c = client(server.url, { timeoutMs: 50 });
    const create = await caught(c.createNamespace("a"));
    expect(create).toBeInstanceOf(KoshaTimeoutError);
    expect(create.attempts).toBe(1);
    const search = await caught(c.namespace("a").search({ query_text: "" }, { maxAttempts: 2 }));
    expect(search).toBeInstanceOf(KoshaTimeoutError);
    expect(search.attempts).toBe(2);
    const opted = await caught(c.createNamespace("a", undefined, { maxAttempts: 2, retryNonIdempotent: true }));
    expect(opted.attempts).toBe(2);
  });

  it("retries a refused connection for any operation", async () => {
    const s = await fakeServer((_req, res) => json(res, 200, {}));
    const url = s.url;
    await s.close();
    const err = await caught(client(url).createNamespace("a"));
    expect(err).toBeInstanceOf(KoshaConnectionError);
    expect(err.attempts).toBe(3);
  });

  it("aborts a sleep when the caller's signal fires", async () => {
    server = await fakeServer((_req, res) => json(res, 503, errorBody("unavailable", true, { retry_after_ms: 5000 })));
    const ac = new AbortController();
    setTimeout(() => ac.abort(new Error("cancelled")), 100);
    const started = Date.now();
    await expect(client(server.url).listNamespaces({ signal: ac.signal })).rejects.toThrow("cancelled");
    expect(Date.now() - started).toBeLessThan(2000);
  });
});

describe("errors", () => {
  it("falls back to reason, then unknown, and to the X-Request-Id header", async () => {
    let n = 0;
    server = await fakeServer((_req, res) => {
      if (n++ === 0) return json(res, 403, { error: "nope", reason: "body_too_large" }, { "x-request-id": "hdr-1" });
      res.writeHead(500, { "content-type": "text/plain" });
      res.end("plain failure");
    });
    const c = client(server.url, { retry: { maxAttempts: 1 } });
    const a = await caught(c.listNamespaces());
    expect(a.code).toBe("body_too_large");
    expect(a.requestId).toBe("hdr-1");
    const b = await caught(c.listNamespaces());
    expect(b.code).toBe("unknown");
    expect(b.body).toBe("plain failure");
    expect(b.status).toBe(500);
  });

  it("keeps unknown codes verbatim and ignores unknown response fields", async () => {
    let n = 0;
    server = await fakeServer((_req, res) => {
      if (n++ === 0) return json(res, 409, errorBody("brand_new_code", false, { details: { x: 1 } }));
      json(res, 200, { namespaces: [], surprise: true });
    });
    const err = await caught(client(server.url).listNamespaces());
    expect(err.code).toBe("brand_new_code");
    expect(err.details).toEqual({ x: 1 });
    expect(err.requestId).toBe("req-1");
    const ok = await client(server.url).listNamespaces();
    expect(ok.namespaces).toEqual([]);
  });
});

describe("client", () => {
  it("sends the key, a User-Agent and snake_case bodies", async () => {
    server = await fakeServer((_req, res) => json(res, 200, { results: [] }));
    await client(server.url, { headers: { "X-Extra": "1" } })
      .namespace("my ns")
      .search({ query_text: "q", max_results: 5 });
    const req = server.requests[0]!;
    expect(req.path).toBe("/v1/namespaces/my%20ns/search");
    expect(req.headers.authorization).toBe("Bearer sk-test");
    expect(req.headers["user-agent"]).toMatch(/^kosha-typescript\/\d+\.\d+\.\d+ \(/);
    expect(req.headers["x-extra"]).toBe("1");
    expect(req.body).toEqual({ query_text: "q", max_results: 5 });
  });

  it("needs an API key and a base URL", () => {
    const saved = { k: process.env.KOSHA_API_KEY, h: process.env.KOSHA_HOST };
    delete process.env.KOSHA_API_KEY;
    delete process.env.KOSHA_HOST;
    try {
      expect(() => new Kosha({ baseUrl: "http://x" })).toThrow(KoshaConfigError);
      expect(() => new Kosha({ apiKey: "k" })).toThrow(KoshaConfigError);
      process.env.KOSHA_API_KEY = "k";
      process.env.KOSHA_HOST = "http://x";
      expect(() => new Kosha()).not.toThrow();
    } finally {
      for (const [name, v] of [["KOSHA_API_KEY", saved.k], ["KOSHA_HOST", saved.h]] as const) {
        if (v === undefined) delete process.env[name];
        else process.env[name] = v;
      }
    }
  });

  it("classifies idempotency", () => {
    expect(isIdempotent("GET", "/v1/namespaces/a/stats")).toBe(true);
    expect(isIdempotent("POST", "/v1/namespaces/a/search")).toBe(true);
    expect(isIdempotent("POST", "/v1/namespaces/a/documents")).toBe(true);
    expect(isIdempotent("POST", "/v1/namespaces/a/replace")).toBe(true);
    expect(isIdempotent("POST", "/v1/namespaces/a/delete")).toBe(true);
    expect(isIdempotent("DELETE", "/v1/namespaces/a")).toBe(true);
    expect(isIdempotent("POST", "/v1/namespaces")).toBe(false);
    expect(isIdempotent("POST", "/v1/namespaces/a/schema")).toBe(false);
    expect(isIdempotent("POST", "/v1/namespaces/a/branch")).toBe(false);
    expect(isIdempotent("DELETE", "/v1/namespaces/a/branch")).toBe(false);
    expect(isIdempotent("POST", "/v1/namespaces/a/shards")).toBe(false);
  });

  it("parses Retry-After as seconds or an HTTP date", () => {
    expect(parseRetryAfter("2")).toBe(2000);
    const now = Date.parse("Wed, 21 Oct 2026 07:28:00 GMT");
    expect(parseRetryAfter("Wed, 21 Oct 2026 07:28:03 GMT", now)).toBe(3000);
    expect(parseRetryAfter("soon")).toBeUndefined();
    expect(parseRetryAfter(null)).toBeUndefined();
  });
});
