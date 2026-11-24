// The shared SDK conformance suite (clients/conformance), TypeScript runner.
// Skipped unless KOSHA_CONFORMANCE_URL is set; see clients/conformance/README.md.
// The Python and Go runners implement the same ops and matchers.
import { readFileSync, readdirSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import {
  BulkError,
  Kosha,
  KoshaConfigError,
  KoshaError,
  type BulkResult,
  type CallOptions,
  type Document,
} from "../src/index.js";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..", "..", "conformance");
const LANG = "ts";
const URL_ = process.env.KOSHA_CONFORMANCE_URL;
const CONTROL = process.env.KOSHA_CONFORMANCE_CONTROL;
const RUN_ID = process.env.KOSHA_CONFORMANCE_RUN_ID || (Date.now() % 36 ** 6).toString(16);
const KEYS: Record<string, string> = {
  default: process.env.KOSHA_CONFORMANCE_API_KEY ?? "sk-kosha-dev",
  small: process.env.KOSHA_CONFORMANCE_SMALL_KEY ?? "sk-conformance-small",
  limited: process.env.KOSHA_CONFORMANCE_LIMITED_KEY ?? "sk-conformance-limited",
  invalid: "sk-conformance-invalid",
};
const BIG = "x".repeat(5000);

type Json = any;
interface Step {
  op: string;
  args?: Record<string, Json>;
  client?: { timeout_ms?: number; max_attempts?: number; total_budget_ms?: number; retry_non_idempotent?: boolean };
  expect?: Record<string, Json>;
}
interface Case {
  id: string;
  tenant?: string;
  setup?: Step[];
  faults?: Json[];
  steps: Step[];
}
interface Result {
  ids?: string[];
  total_hits?: number;
  count?: number;
  indexed?: number;
  batches?: number;
  failed_ids?: string[];
}

function loadCases(): Case[] {
  return readdirSync(join(ROOT, "cases"))
    .filter((f) => f.endsWith(".json"))
    .sort()
    .flatMap((f) => JSON.parse(readFileSync(join(ROOT, "cases", f), "utf8")) as Case[]);
}
const schema = JSON.parse(readFileSync(join(ROOT, "case.schema.json"), "utf8")).$defs;

// ── Generators and placeholders ──────────────────────────────────────────

export function render(template: string, i: number): string {
  return template.replace(/\{i(?:%(\d+))?(?::0(\d+))?\}/g, (_m, mod?: string, width?: string) => {
    const n = mod ? i % Number(mod) : i;
    return width ? String(n).padStart(Number(width), "0") : String(n);
  });
}

function generate(spec: Json): Document[] {
  return Array.from({ length: spec.count }, (_, i) => ({
    id: render(spec.id, i),
    fields: Object.entries((spec.fields ?? {}) as Record<string, { type: string; value: string }>).map(([name, f]) => ({
      name,
      field_type: f.type as Document["fields"][number]["field_type"],
      value: render(f.value, i),
    })),
  }));
}

function substitute(value: Json, ns: string): Json {
  if (typeof value === "string") return value.replaceAll("$NS", ns).replaceAll("$BIG", BIG);
  if (Array.isArray(value)) return value.map((v) => substitute(v, ns));
  if (value && typeof value === "object") return Object.fromEntries(Object.entries(value).map(([k, v]) => [k, substitute(v, ns)]));
  return value;
}

const documents = (v: Json): Document[] => (v && !Array.isArray(v) && v.generate ? generate(v.generate) : v);
const withCount = (r: Result): Result => (r.ids ? { ...r, count: r.ids.length } : r);

// ── Ops ──────────────────────────────────────────────────────────────────

type Op = (k: Kosha, ns: string, a: Json, o: CallOptions) => Promise<Result>;

const bulkResult = (r: BulkResult): Result => ({
  indexed: r.indexed,
  batches: r.batches,
  failed_ids: r.failures.flatMap((f) => f.ids),
});

const OPS: Record<string, Op> = {
  create_namespace: async (k, ns, a, o) => (await k.createNamespace(ns, a.schema, o), {}),
  list_namespaces: async (k, _ns, _a, o) => withCount({ ids: ((await k.listNamespaces(o)).namespaces ?? []).map((n) => n.name ?? "") }),
  delete_namespace: async (k, ns, _a, o) => (await k.deleteNamespace(ns, o), {}),
  put_schema: async (k, ns, a, o) => (await k.namespace(ns).putSchema(a.schema, o), {}),
  get_schema: async (k, ns, _a, o) => withCount({ ids: Object.keys((await k.namespace(ns).getSchema(o)).schema?.fields ?? {}).sort() }),
  upsert: async (k, ns, a, o) => ({ indexed: Number((await k.namespace(ns).upsert(documents(a.documents), o)).indexed_count) }),
  merge: async (k, ns, a, o) => ({ indexed: Number((await k.namespace(ns).merge(documents(a.documents), o)).indexed_count) }),
  exists: async (k, ns, a, o) => withCount({ ids: (await k.namespace(ns).exists(a.ids, o)).existing ?? [] }),
  search: async (k, ns, a, o) => {
    const r = await k.namespace(ns).search(a.body, o);
    return withCount({ ids: (r.results ?? []).map((h) => h.doc_id ?? ""), ...(r.total_hits !== undefined ? { total_hits: Number(r.total_hits) } : {}) });
  },
  search_all: async (k, ns, a, o) => {
    const ids: string[] = [];
    for await (const h of k.namespace(ns).searchAll(a.body, { ...o, ...(a.page_size ? { pageSize: a.page_size } : {}) })) ids.push(h.doc_id ?? "");
    return withCount({ ids });
  },
  bulk_index: async (k, ns, a, o) =>
    bulkResult(
      await k.namespace(ns).bulkIndex(documents(a.documents), {
        ...o,
        ...(a.max_docs_per_batch ? { maxDocsPerBatch: a.max_docs_per_batch } : {}),
        ...(a.max_bytes_per_batch ? { maxBytesPerBatch: a.max_bytes_per_batch } : {}),
        ...(a.concurrency ? { concurrency: a.concurrency } : {}),
        ...(a.on_error ? { onError: a.on_error } : {}),
      }),
    ),
  delete_by_query: async (k, ns, a, o) => ({ count: Number((await k.namespace(ns).deleteByQuery(a.body, o)).deleted) }),
  flush: async (k, ns, _a, o) => (await k.namespace(ns).flush(o), {}),
  stats: async (k, ns, _a, o) => ({ count: Number((await k.namespace(ns).stats(o)).documents) }),
  raw_request: async (k, _ns, a, o) => (await k.request(a.method, a.path, a.body, o), {}),
};

// ── Matchers ─────────────────────────────────────────────────────────────

interface Meta {
  elapsedMs: number;
  attempts: () => Promise<number>;
}
type Matcher = (want: Json, r: Result, e: KoshaError | undefined, m: Meta) => void | Promise<void>;

function errorKind(e: KoshaError): string {
  if (e instanceof KoshaConfigError) return "config";
  return e.status ? "http" : "transport";
}

const MATCHERS: Record<string, Matcher> = {
  ok: () => {},
  ids: (want, r) => expect(r.ids).toEqual(Array.isArray(want) ? want : generate(want.generate).map((d) => d.id)),
  ids_unordered: (want: string[], r) => expect([...(r.ids ?? [])].sort()).toEqual([...want].sort()),
  ids_include: (want: string[], r) => expect(r.ids ?? []).toEqual(expect.arrayContaining(want)),
  total_hits: (want, r) => expect(r.total_hits).toBe(want),
  count: (want, r) => expect(r.count).toBe(want),
  unique: (_w, r) => expect(new Set(r.ids).size).toBe(r.ids?.length),
  indexed: (want, r) => expect(r.indexed).toBe(want),
  batches: (want, r) => expect(r.batches).toBe(want),
  failed_ids: (want: string[], r) => expect([...(r.failed_ids ?? [])].sort()).toEqual([...want].sort()),
  error: (want, _r, e) => {
    expect(e, "expected an error, the call succeeded").toBeDefined();
    const err = e!;
    if (want.kind !== undefined) expect(errorKind(err)).toBe(want.kind);
    if (want.status !== undefined) expect(err.status).toBe(want.status);
    if (want.code !== undefined) expect(err.code).toBe(want.code);
    if (want.retryable !== undefined) expect(err.retryable).toBe(want.retryable);
    if (want.has_request_id !== undefined) expect(Boolean(err.requestId)).toBe(want.has_request_id);
    if (want.retry_after_ms !== undefined) expect(err.retryAfterMs).toBe(want.retry_after_ms);
  },
  attempts: async (want, _r, _e, m) => expect(await m.attempts()).toBe(want),
  attempts_min: async (want, _r, _e, m) => expect(await m.attempts()).toBeGreaterThanOrEqual(want),
  min_elapsed_ms: (want, _r, _e, m) => expect(m.elapsedMs).toBeGreaterThanOrEqual(want),
  max_elapsed_ms: (want, _r, _e, m) => expect(m.elapsedMs).toBeLessThanOrEqual(want),
};

// ── Runner ───────────────────────────────────────────────────────────────

async function control(method: string, path: string, body?: Json): Promise<Json> {
  const res = await fetch(CONTROL!.replace(/\/+$/, "") + path, {
    method,
    headers: { "content-type": "application/json" },
    ...(body !== undefined ? { body: JSON.stringify(body) } : {}),
  });
  if (!res.ok) throw new Error(`faultproxy ${method} ${path}: ${res.status} ${await res.text()}`);
  return res.json();
}

const needsProxy = (c: Case) =>
  Boolean(c.faults?.length) || c.steps.some((s) => s.expect && ("attempts" in s.expect || "attempts_min" in s.expect));

function makeClient(apiKey: string, settings: Step["client"] = {}): Kosha {
  return new Kosha({
    baseUrl: URL_!,
    apiKey,
    ...(settings.timeout_ms !== undefined ? { timeoutMs: settings.timeout_ms } : {}),
    retry: {
      ...(settings.max_attempts !== undefined ? { maxAttempts: settings.max_attempts } : {}),
      ...(settings.total_budget_ms !== undefined ? { totalBudgetMs: settings.total_budget_ms } : {}),
    },
  });
}

async function runOp(k: Kosha, step: Step, ns: string, tag: string): Promise<{ result: Result; error?: KoshaError }> {
  const args = substitute(step.args ?? {}, ns);
  const target: string = args.namespace ?? ns;
  delete args.namespace;
  const opts: CallOptions = { headers: { "X-Conformance-Case": tag } };
  if (step.client?.retry_non_idempotent) opts.retryNonIdempotent = true;
  try {
    return { result: await OPS[step.op]!(k, target, args, opts) };
  } catch (e) {
    if (e instanceof BulkError) return { result: bulkResult(e.result), error: e };
    if (e instanceof KoshaError) return { result: {}, error: e };
    throw e;
  }
}

describe("conformance runner", () => {
  it("implements every op and matcher in case.schema.json", () => {
    expect(Object.keys(OPS).sort()).toEqual([...schema.op.enum].sort());
    expect(Object.keys(MATCHERS).sort()).toEqual(Object.keys(schema.expect.properties).sort());
  });

  it("renders generator templates", () => {
    expect(render("d{i:04}", 7)).toBe("d0007");
    expect(render("t{i%7}", 23)).toBe("t2");
    expect(render("x{i%7:02}-{i}", 23)).toBe("x02-23");
  });
});

describe("conformance cases", () => {
  for (const c of loadCases()) {
    const skip = !URL_ ? "KOSHA_CONFORMANCE_URL is not set" : needsProxy(c) && !CONTROL ? "needs faultproxy" : "";
    it.skipIf(Boolean(skip))(c.id, { timeout: 120_000 }, async () => {
      const key = `${LANG}-${RUN_ID}-${c.id}`;
      const ns = `conf-${LANG}-${RUN_ID}-${c.id.replace(/[^a-z0-9]+/g, "-")}`;
      const apiKey = KEYS[c.tenant ?? "default"]!;
      if (c.faults?.length) await control("POST", "/faults", { case: key, rules: c.faults });
      try {
        const setup = makeClient(apiKey);
        for (const step of c.setup ?? []) {
          const { error } = await runOp(setup, step, ns, `${key}#setup`);
          expect(error, `setup ${step.op} failed: ${error?.message}`).toBeUndefined();
        }
        for (const [i, step] of c.steps.entries()) {
          const tag = `${key}#${i}`;
          const want = substitute(step.expect ?? {}, ns) as Record<string, Json>;
          const started = performance.now();
          const { result, error } = await runOp(makeClient(apiKey, step.client), step, ns, tag);
          const meta: Meta = {
            elapsedMs: performance.now() - started,
            attempts: async () =>
              ((await control("GET", `/log?case=${encodeURIComponent(key)}`)).requests as { step: string }[]).filter(
                (r) => `${key}#${r.step}` === tag,
              ).length,
          };
          if (!("error" in want)) expect(error, `step ${i} (${step.op}) failed: ${error?.message}`).toBeUndefined();
          for (const [name, value] of Object.entries(want)) {
            const matcher = MATCHERS[name];
            if (!matcher) throw new Error(`unknown matcher ${name}`);
            try {
              await matcher(value, result, error, meta);
            } catch (e) {
              throw new Error(`step ${i} (${step.op}) ${name}: ${(e as Error).message}`);
            }
          }
        }
      } finally {
        if (c.faults?.length) await control("DELETE", `/faults?case=${encodeURIComponent(key)}`);
        await runOp(makeClient(apiKey, { max_attempts: 1 }), { op: "delete_namespace" }, ns, `${key}#cleanup`);
      }
    });
  }
});
