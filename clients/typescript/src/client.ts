import { bulkIndex, type BulkOptions, type BulkResult } from "./bulk.js";
import { KoshaConfigError } from "./errors.js";
import { searchAll, type SearchAllOptions } from "./paginate.js";
import type { RetryOptions } from "./retry.js";
import { Transport, type CallOptions } from "./transport.js";
import type {
  CreateNamespaceResponse,
  DeleteByQueryRequest,
  DeleteByQueryResponse,
  DeleteNamespaceResponse,
  Document,
  ExistsResponse,
  FlushResponse,
  GetSchemaResponse,
  Hit,
  IndexDocumentsResponse,
  ListNamespacesResponse,
  NamespaceSchema,
  PutSchemaResponse,
  ReplaceDocumentsResponse,
  SearchRequest,
  SearchResponse,
  StatsResponse,
} from "./types.js";

export interface KoshaOptions {
  /** Tenant API key; defaults to the KOSHA_API_KEY environment variable. */
  apiKey?: string;
  /** Server URL, e.g. https://kosha.example.com; defaults to KOSHA_HOST. */
  baseUrl?: string;
  /** Retry policy (clients/conformance/RETRY.md). */
  retry?: RetryOptions;
  /** Per-attempt timeout (default 30000 ms). */
  timeoutMs?: number;
  /** A fetch implementation, e.g. one with a proxy agent. */
  fetch?: typeof globalThis.fetch;
  /** Headers sent with every request. */
  headers?: Record<string, string>;
}

function env(name: string): string | undefined {
  const p = (globalThis as { process?: { env?: Record<string, string | undefined> } }).process;
  const v = p?.env?.[name];
  return v === undefined || v === "" ? undefined : v;
}

/**
 * A Kosha client for the /v1 API.
 *
 * ```ts
 * const kosha = new Kosha({ baseUrl: "https://kosha.example.com", apiKey });
 * const docs = kosha.namespace("docs");
 * const page = await docs.search({ query_text: "hello", max_results: 10 });
 * ```
 */
export class Kosha {
  private readonly transport: Transport;

  constructor(opts: KoshaOptions = {}) {
    const apiKey = opts.apiKey ?? env("KOSHA_API_KEY");
    const baseUrl = opts.baseUrl ?? env("KOSHA_HOST");
    if (!apiKey) throw new KoshaConfigError("no API key: pass apiKey or set KOSHA_API_KEY");
    if (!baseUrl) throw new KoshaConfigError("no server URL: pass baseUrl or set KOSHA_HOST");
    this.transport = new Transport({
      baseUrl,
      apiKey,
      ...(opts.retry ? { retry: opts.retry } : {}),
      ...(opts.timeoutMs !== undefined ? { timeoutMs: opts.timeoutMs } : {}),
      ...(opts.fetch ? { fetch: opts.fetch } : {}),
      ...(opts.headers ? { headers: opts.headers } : {}),
    });
  }

  /** A handle on one namespace of the key's tenant. Sends nothing. */
  namespace(name: string): Namespace {
    if (!name) throw new KoshaConfigError("namespace name is empty");
    return new Namespace(this.transport, name);
  }

  createNamespace(name: string, schema?: NamespaceSchema, opts?: CallOptions): Promise<CreateNamespaceResponse> {
    const body: Record<string, unknown> = { namespace: name };
    if (schema !== undefined) body.schema = schema;
    return this.transport.request("POST", "/v1/namespaces", body, opts);
  }

  listNamespaces(opts?: CallOptions): Promise<ListNamespacesResponse> {
    return this.transport.request("GET", "/v1/namespaces", undefined, opts);
  }

  deleteNamespace(name: string, opts?: CallOptions): Promise<DeleteNamespaceResponse> {
    return this.transport.request("DELETE", `/v1/namespaces/${encodeURIComponent(name)}`, undefined, opts);
  }

  /**
   * Any route without a typed method, through the same retry and error
   * handling. `path` starts with `/v1/`.
   */
  request<T = unknown>(method: string, path: string, body?: unknown, opts?: CallOptions): Promise<T> {
    return this.transport.request<T>(method, path, body, opts);
  }
}

/** Operations on one namespace. */
export class Namespace {
  constructor(
    private readonly transport: Transport,
    readonly name: string,
  ) {}

  private path(action?: string): string {
    const base = `/v1/namespaces/${encodeURIComponent(this.name)}`;
    return action ? `${base}/${action}` : base;
  }

  putSchema(schema: NamespaceSchema, opts?: CallOptions): Promise<PutSchemaResponse> {
    return this.transport.request("POST", this.path("schema"), schema, opts);
  }

  getSchema(opts?: CallOptions): Promise<GetSchemaResponse> {
    return this.transport.request("GET", this.path("schema"), undefined, opts);
  }

  /** Inserts each document, or replaces the stored one with the same id in full. */
  upsert(documents: Document[], opts?: CallOptions): Promise<IndexDocumentsResponse> {
    return this.transport.request("POST", this.path("documents"), { documents }, opts);
  }

  /** Merges partial documents onto the stored copies by id; unknown ids are inserted. */
  merge(documents: Document[], opts?: CallOptions): Promise<ReplaceDocumentsResponse> {
    return this.transport.request("POST", this.path("replace"), { documents }, opts);
  }

  search(request: SearchRequest, opts?: CallOptions): Promise<SearchResponse> {
    return this.transport.request("POST", this.path("search"), request, opts);
  }

  /** Every hit of `request`, page by page with `search_after`. See README. */
  searchAll(request: SearchRequest, opts?: SearchAllOptions): AsyncIterable<Hit> {
    return searchAll(this, request, opts);
  }

  /** Indexes many documents in batches, with bounded concurrency. See README. */
  bulkIndex(documents: Iterable<Document> | AsyncIterable<Document>, opts?: BulkOptions): Promise<BulkResult> {
    return bulkIndex(this, documents, opts);
  }

  exists(ids: string[], opts?: CallOptions): Promise<ExistsResponse> {
    return this.transport.request("POST", this.path("exists"), { ids }, opts);
  }

  deleteByQuery(request: DeleteByQueryRequest, opts?: CallOptions): Promise<DeleteByQueryResponse> {
    return this.transport.request("POST", this.path("delete"), request, opts);
  }

  flush(opts?: CallOptions): Promise<FlushResponse> {
    return this.transport.request("POST", this.path("flush"), {}, opts);
  }

  stats(opts?: CallOptions): Promise<StatsResponse> {
    return this.transport.request("GET", this.path("stats"), undefined, opts);
  }
}
