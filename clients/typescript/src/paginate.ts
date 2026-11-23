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

/** Whether a sort entry (already in object form) sorts descending. */
function isDescending(entry: Record<string, unknown>, key: string): boolean {
  const spec = entry[key];
  return isRecord(spec) && spec.order === "desc";
}

/**
 * The request searchAll sends for each page: the caller's sort with an
 * `_id` tie-break, the sort fields kept in `source_includes` and taken out
 * of `source_excludes` (the cursor is read from them), and `max_results`
 * set to the page size. `descending[i]` is whether `keys[i]` sorts desc.
 */
export function pagedRequest(
  request: SearchRequest,
  pageSize: number,
): { body: SearchRequest; keys: string[]; descending: boolean[] } {
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
  const descending = sort.map((e, i) => isDescending(e, keys[i]!));
  if (keys[keys.length - 1] !== "_id") {
    sort.push(ID_ASC);
    keys.push("_id");
    descending.push(false);
  }
  const body: SearchRequest = { ...request, sort, max_results: pageSize };
  if (request.source_includes !== undefined) {
    const include = new Set(request.source_includes);
    for (const k of keys) if (k !== "_id" && k !== "_score") include.add(k);
    body.source_includes = [...include];
  }
  if (request.source_excludes !== undefined) {
    body.source_excludes = request.source_excludes.filter((f) => !keys.includes(f));
  }
  return { body, keys, descending };
}

/**
 * The value the server sorts a stored field value by: a JSON list (a
 * multi-valued keyword) sorts by its smallest element ascending and its
 * largest descending, and an empty list as missing (""). Mirrors the
 * server's `list_elements`.
 */
export function sortValueOf(value: string, descending: boolean): string {
  const trimmed = value.trim();
  if (!value.startsWith("[") || !trimmed.endsWith("]")) return value;
  let parsed: unknown;
  try {
    parsed = JSON.parse(trimmed);
  } catch {
    return value;
  }
  if (!Array.isArray(parsed)) return value;
  const elements = parsed
    .filter((e) => typeof e === "string" || typeof e === "number" || typeof e === "boolean")
    .map(String);
  if (elements.length === 0) return "";
  return elements.reduce((best, e) => ((descending ? e > best : e < best) ? e : best));
}

/**
 * The cursor after `hit`: `_id` is the document id, `_score` the score
 * (shortest round-trip form), any other key the value the server sorted
 * the hit by: its stored value for that field, or for a list the element
 * [`sortValueOf`] picks; "" when it has none.
 */
export function cursorFor(hit: Hit, keys: string[], descending: boolean[] = []): string[] {
  const sortValues = (hit as { sort?: unknown }).sort;
  if (Array.isArray(sortValues) && sortValues.length === keys.length) return sortValues.map(String);
  return keys.map((k, i) => {
    if (k === "_id") return hit.doc_id ?? "";
    if (k === "_score") return String(hit.score ?? 0);
    const value = hit.fields?.find((f) => f.name === k)?.value;
    return value === undefined ? "" : sortValueOf(value, descending[i] === true);
  });
}

export async function* searchAll(ns: Searcher, request: SearchRequest, opts: SearchAllOptions = {}): AsyncGenerator<Hit> {
  const { pageSize: requested, ...callOpts } = opts;
  const pageSize = requested ?? (request.max_results !== undefined ? Number(request.max_results) : 500);
  const { body, keys, descending } = pagedRequest(request, pageSize);
  let cursor: string[] | undefined;
  for (;;) {
    const page = await ns.search(cursor ? { ...body, search_after: cursor } : body, callOpts);
    const hits = page.results ?? [];
    for (const hit of hits) yield hit;
    if (hits.length < pageSize) return;
    const next = cursorFor(hits[hits.length - 1]!, keys, descending);
    if (cursor && next.every((v, i) => v === cursor![i])) {
      throw new KoshaConfigError("searchAll: the cursor did not advance (is a sort field stored and returned?)");
    }
    cursor = next;
  }
}
