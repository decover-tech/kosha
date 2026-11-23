import { afterEach, describe, expect, it } from "vitest";
import { BulkError, Kosha, KoshaConfigError, type Document, type Hit } from "../src/index.js";
import { errorBody, fakeServer, json, type FakeServer } from "./fake-server.js";

let server: FakeServer | undefined;
afterEach(async () => {
  await server?.close();
  server = undefined;
});

function doc(i: number, tag?: string): Document {
  const fields: Document["fields"] = [{ name: "title", field_type: "Text", value: `title ${i}` }];
  if (tag !== undefined) fields.push({ name: "tag", field_type: "Keyword", value: tag });
  return { id: `d${String(i).padStart(4, "0")}`, fields };
}

/** A tiny search engine: sorts by the requested keys, applies search_after, pages. */
function search(docs: Document[], body: any): { results: Hit[]; total_hits: number } {
  const keys: [string, number][] = (body.sort ?? []).map((e: Record<string, { order?: string }>) => {
    const [k, v] = Object.entries(e)[0]!;
    return [k, v?.order === "desc" ? -1 : 1];
  });
  const value = (d: Document, k: string) => (k === "_id" ? d.id : (d.fields.find((f) => f.name === k)?.value ?? ""));
  const cmp = (a: string[], b: string[]) => {
    for (let i = 0; i < keys.length; i++) {
      if (a[i]! < b[i]!) return -keys[i]![1];
      if (a[i]! > b[i]!) return keys[i]![1];
    }
    return 0;
  };
  let rows = docs.map((d) => ({ d, v: keys.map(([k]) => value(d, k)) })).sort((x, y) => cmp(x.v, y.v));
  if (body.search_after) rows = rows.filter((r) => cmp(r.v, body.search_after) > 0);
  const include: string[] | undefined = body.source_includes;
  const results = rows.slice(0, body.max_results ?? 10).map(({ d }) => ({
    doc_id: d.id,
    score: 1,
    fields: d.fields.filter((f) => !include || include.includes(f.name)),
  }));
  return { results, total_hits: docs.length };
}

async function collect(it: AsyncIterable<Hit>): Promise<string[]> {
  const ids: string[] = [];
  for await (const h of it) ids.push(h.doc_id!);
  return ids;
}

describe("searchAll", () => {
  it("returns every id exactly once, in order, over 2.5 pages", async () => {
    const docs = Array.from({ length: 250 }, (_, i) => doc(i, `t${i % 7}`));
    server = await fakeServer((req, res) => json(res, 200, search(docs, req.body)));
    const ns = new Kosha({ baseUrl: server.url, apiKey: "k" }).namespace("n");

    const byId = await collect(ns.searchAll({ query_text: "" }, { pageSize: 100 }));
    expect(byId).toEqual(docs.map((d) => d.id));
    expect(server.requests).toHaveLength(3);
    expect(server.requests[0]!.body.sort).toEqual([{ _id: { order: "asc" } }]);
    expect(server.requests[1]!.body.search_after).toEqual(["d0099"]);

    const byTag = await collect(
      ns.searchAll({ query_text: "", sort: [{ tag: { order: "asc" } }], source_includes: ["title"] }, { pageSize: 100 }),
    );
    expect(new Set(byTag).size).toBe(250);
    const expected = [...docs]
      .sort((a, b) => (a.fields[1]!.value + a.id < b.fields[1]!.value + b.id ? -1 : 1))
      .map((d) => d.id);
    expect(byTag).toEqual(expected);
    const last = server.requests[server.requests.length - 1]!.body;
    expect(last.sort).toEqual([{ tag: { order: "asc" } }, { _id: { order: "asc" } }]);
    expect(last.source_includes).toEqual(["title", "tag"]);
    expect(last.search_after).toHaveLength(2);
  });

  it("stops on an empty first page", async () => {
    server = await fakeServer((_req, res) => json(res, 200, { results: [] }));
    const ns = new Kosha({ baseUrl: server.url, apiKey: "k" }).namespace("n");
    expect(await collect(ns.searchAll({ query_text: "" }))).toEqual([]);
    expect(server.requests[0]!.body.max_results).toBe(500);
  });

  it("rejects bodies it cannot page", async () => {
    const ns = new Kosha({ baseUrl: "http://127.0.0.1:1", apiKey: "k" }).namespace("n");
    for (const bad of [
      { query_text: "", from: 10 },
      { query_text: "", search_after: ["x"] },
      { query_text: "", rerank: { query: "x" } },
      { query_text: "", knn: { field: "v", vector: [1] } },
      { query_text: "", mode: "hybrid" as const },
    ]) {
      await expect(collect(ns.searchAll(bad as any))).rejects.toBeInstanceOf(KoshaConfigError);
    }
  });

  it("fails instead of looping when the cursor cannot advance", async () => {
    const page = { results: [{ doc_id: "same", fields: [] }] };
    server = await fakeServer((_req, res) => json(res, 200, page));
    const ns = new Kosha({ baseUrl: server.url, apiKey: "k" }).namespace("n");
    await expect(collect(ns.searchAll({ query_text: "" }, { pageSize: 1 }))).rejects.toBeInstanceOf(KoshaConfigError);
  });
});

describe("bulkIndex", () => {
  it("batches by doc count", async () => {
    server = await fakeServer((req, res) => json(res, 200, { indexed_count: req.body.documents.length }));
    const ns = new Kosha({ baseUrl: server.url, apiKey: "k" }).namespace("n");
    const out = await ns.bulkIndex(Array.from({ length: 250 }, (_, i) => doc(i)), { maxDocsPerBatch: 40 });
    expect(out).toEqual({ indexed: 250, batches: 7, failures: [] });
    expect(server.requests.map((r) => r.body.documents.length).sort((a, b) => a - b)).toEqual([10, 40, 40, 40, 40, 40, 40]);
  });

  it("splits a batch that would pass the byte cap and sends an oversized doc alone", async () => {
    server = await fakeServer((req, res) => json(res, 200, { indexed_count: req.body.documents.length }));
    const ns = new Kosha({ baseUrl: server.url, apiKey: "k" }).namespace("n");
    const big: Document = { id: "big", fields: [{ name: "title", field_type: "Text", value: "x".repeat(5000) }] };
    const docs = [doc(1), doc(2), big, doc(3)];
    const one = new TextEncoder().encode(JSON.stringify(doc(1))).length + 1;
    const out = await ns.bulkIndex(docs, { maxBytesPerBatch: one * 2 + 1, concurrency: 1 });
    expect(out.batches).toBe(3);
    expect(server.requests.map((r) => r.body.documents.map((d: Document) => d.id))).toEqual([
      ["d0001", "d0002"],
      ["big"],
      ["d0003"],
    ]);
  });

  it("reports the failed batch's ids and stops by default", async () => {
    server = await fakeServer((req, res) => {
      if (req.body.documents.some((d: Document) => d.id === "d0005")) return json(res, 400, errorBody("invalid_argument", false));
      json(res, 200, { indexed_count: req.body.documents.length });
    });
    const ns = new Kosha({ baseUrl: server.url, apiKey: "k" }).namespace("n");
    const docs = Array.from({ length: 20 }, (_, i) => doc(i));
    let err: unknown;
    try {
      await ns.bulkIndex(docs, { maxDocsPerBatch: 4, concurrency: 1 });
    } catch (e) {
      err = e;
    }
    expect(err).toBeInstanceOf(BulkError);
    const result = (err as BulkError).result;
    expect(result.failures).toHaveLength(1);
    expect(result.failures[0]!.batchIndex).toBe(1);
    expect(result.failures[0]!.ids).toEqual(["d0004", "d0005", "d0006", "d0007"]);
    expect(result.failures[0]!.error.code).toBe("invalid_argument");
    expect(result.indexed).toBe(4);
    expect(server.requests).toHaveLength(2);

    const cont = await ns.bulkIndex(docs, { maxDocsPerBatch: 4, onError: "continue" });
    expect(cont.indexed).toBe(16);
    expect(cont.batches).toBe(5);
    expect(cont.failures.map((f) => f.batchIndex)).toEqual([1]);
  });

  it("lets the last occurrence of an id win, waiting for an in-flight batch", async () => {
    const order: string[] = [];
    server = await fakeServer(async (req, res) => {
      await new Promise((r) => setTimeout(r, req.body.documents[0].id === "d0000" ? 100 : 0));
      for (const d of req.body.documents) order.push(`${d.id}:${d.fields[0].value}`);
      json(res, 200, { indexed_count: req.body.documents.length });
    });
    const ns = new Kosha({ baseUrl: server.url, apiKey: "k" }).namespace("n");
    const v = (i: number, value: string): Document => ({ id: `d${String(i).padStart(4, "0")}`, fields: [{ name: "title", field_type: "Text", value }] });
    const out = await ns.bulkIndex([v(0, "a"), v(1, "a"), v(2, "a"), v(0, "b"), v(3, "a"), v(3, "c")], {
      maxDocsPerBatch: 2,
      concurrency: 4,
    });
    expect(out.failures).toEqual([]);
    expect(order.indexOf("d0000:b")).toBeGreaterThan(order.indexOf("d0000:a"));
    expect(order).toContain("d0003:c");
    expect(order).not.toContain("d0003:a");
  });
});
