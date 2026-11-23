import { KoshaConfigError, isRecord } from "./errors.js";
import type { CallOptions } from "./transport.js";
import type { Hit, SearchRequest, SearchResponse } from "./types.js";

export interface SearchAllOptions extends CallOptions {
  /** Hits per request (`max_results`); default 500. */
  pageSize?: number;
}

interface Searcher {
  search(request: SearchRequest, opts?: CallOptions): Promise<SearchResponse>;
}

const ID_ASC = { _id: { order: "asc" } };

/** The field a sort entry names: `{"date": {"order": "desc"}}` or `"date"`. */
export function sortKey(entry: unknown): string {
  if (typeof entry === "string") return entry;
  if (isRecord(entry)) {
    const keys = Object.keys(entry);
    if (keys.length === 1) return keys[0]!;
  }
  throw new KoshaConfigError(`searchAll: unsupported sort entry ${JSON.stringify(entry)}`);
}

/**
 * The request searchAll sends for each page: the caller's sort with an
 * `_id` tie-break, the sort fields kept in `source_includes`, and
 * `max_results` set to the page size.
 */
export function pagedRequest(request: SearchRequest, pageSize: number): { body: SearchRequest; keys: string[] } {
  if (request.from !== undefined) throw new KoshaConfigError("searchAll pages with search_after; do not set from");
  if (request.search_after !== undefined) throw new KoshaConfigError("searchAll sets search_after itself");
  if (request.rerank !== undefined) throw new KoshaConfigError("searchAll cannot page a reranked search");
  if (request.knn !== undefined || request.rank !== undefined || request.mode === "semantic" || request.mode === "hybrid") {
    throw new KoshaConfigError("searchAll cannot page a kNN or hybrid search");
  }
  if (!Number.isInteger(pageSize) || pageSize < 1) throw new KoshaConfigError("pageSize must be a positive integer");
  const sort: Record<string, unknown>[] = (request.sort ?? []).map((e) =>
    typeof e === "string" ? { [e]: { order: "asc" } } : e,
  );
  const keys = sort.map(sortKey);
  if (keys[keys.length - 1] !== "_id") {
    sort.push(ID_ASC);
    keys.push("_id");
  }
  const body: SearchRequest = { ...request, sort, max_results: pageSize };
  if (request.source_includes !== undefined) {
    const include = new Set(request.source_includes);
    for (const k of keys) if (k !== "_id" && k !== "_score") include.add(k);
    body.source_includes = [...include];
  }
  return { body, keys };
}

/**
 * The cursor after `hit`: `_id` is the document id, `_score` the score
 * (shortest round-trip form), any other key the hit's first stored value
 * for that field, "" when it has none (the kosha-cli rule).
 */
export function cursorFor(hit: Hit, keys: string[]): string[] {
  const sortValues = (hit as { sort?: unknown }).sort;
  if (Array.isArray(sortValues) && sortValues.length === keys.length) return sortValues.map(String);
  return keys.map((k) => {
    if (k === "_id") return hit.doc_id ?? "";
    if (k === "_score") return String(hit.score ?? 0);
    return hit.fields?.find((f) => f.name === k)?.value ?? "";
  });
}

export async function* searchAll(ns: Searcher, request: SearchRequest, opts: SearchAllOptions = {}): AsyncGenerator<Hit> {
  const { pageSize: requested, ...callOpts } = opts;
  const pageSize = requested ?? (request.max_results !== undefined ? Number(request.max_results) : 500);
  const { body, keys } = pagedRequest(request, pageSize);
  let cursor: string[] | undefined;
  for (;;) {
    const page = await ns.search(cursor ? { ...body, search_after: cursor } : body, callOpts);
    const hits = page.results ?? [];
    for (const hit of hits) yield hit;
    if (hits.length < pageSize) return;
    const next = cursorFor(hits[hits.length - 1]!, keys);
    if (cursor && next.every((v, i) => v === cursor![i])) {
      throw new KoshaConfigError("searchAll: the cursor did not advance (is a sort field stored and returned?)");
    }
    cursor = next;
  }
}
