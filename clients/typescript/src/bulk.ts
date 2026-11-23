import { KoshaError } from "./errors.js";
import type { CallOptions } from "./transport.js";
import type { Document, IndexDocumentsResponse } from "./types.js";

export interface BulkOptions extends CallOptions {
  /** A batch closes at this many documents (default 500). */
  maxDocsPerBatch?: number;
  /** ... or before its request body (the JSON sent, envelope included) would pass this many bytes (default 8 MiB). */
  maxBytesPerBatch?: number;
  /** Batches in flight at once (default 2). */
  concurrency?: number;
  /** "stop" (default): stop dispatching on the first failed batch and throw BulkError. "continue": report failures in the result. */
  onError?: "stop" | "continue";
}

export interface BulkFailure {
  batchIndex: number;
  ids: string[];
  error: KoshaError;
}

export interface BulkResult {
  indexed: number;
  batches: number;
  failures: BulkFailure[];
}

/** A bulkIndex that stopped on a failed batch; `result` holds what was done. */
export class BulkError extends KoshaError {
  readonly result: BulkResult;

  constructor(result: BulkResult) {
    const first = result.failures[0]!.error;
    super({
      status: first.status,
      code: first.code,
      message: `bulk index stopped: batch ${result.failures[0]!.batchIndex} failed: ${first.message}`,
      retryable: first.retryable,
      ...(first.requestId !== undefined ? { requestId: first.requestId } : {}),
      attempts: first.attempts,
      cause: first,
    });
    this.name = "BulkError";
    this.result = result;
  }
}

interface Upserter {
  upsert(documents: Document[], opts?: CallOptions): Promise<IndexDocumentsResponse>;
}

interface InFlight {
  ids: Set<string>;
  done: Promise<void>;
}

const encoder = new TextEncoder();
// The request body is {"documents":[d1,d2,...]}: 16 bytes of envelope plus
// each document and the commas between them. Sizing each document as its
// bytes plus one counts one comma too many, so the envelope adds 15.
const ENVELOPE_BYTES = '{"documents":[]}'.length - 1;

export async function bulkIndex(
  ns: Upserter,
  documents: Iterable<Document> | AsyncIterable<Document>,
  opts: BulkOptions = {},
): Promise<BulkResult> {
  const { maxDocsPerBatch = 500, maxBytesPerBatch = 8 * 1024 * 1024, concurrency = 2, onError = "stop", ...callOpts } = opts;
  const result: BulkResult = { indexed: 0, batches: 0, failures: [] };
  const inflight = new Set<InFlight>();
  let stopped = false;
  // Insertion-ordered; a repeated id replaces its earlier entry (last wins).
  let current = new Map<string, { doc: Document; bytes: number }>();
  let currentBytes = 0;

  const dispatch = async () => {
    const batch = current;
    current = new Map();
    currentBytes = 0;
    while (inflight.size >= Math.max(1, concurrency)) await Promise.race([...inflight].map((f) => f.done));
    if (stopped || batch.size === 0) return;
    const batchIndex = result.batches++;
    const docs = [...batch.values()].map((e) => e.doc);
    const ids = [...batch.keys()];
    const entry: InFlight = { ids: new Set(ids), done: Promise.resolve() };
    entry.done = ns.upsert(docs, callOpts).then(
      (res) => {
        result.indexed += typeof res.indexed_count === "number" ? res.indexed_count : docs.length;
      },
      (err: unknown) => {
        const error = err instanceof KoshaError ? err : new KoshaError({ status: 0, message: String(err), cause: err });
        result.failures.push({ batchIndex, ids, error });
        if (onError === "stop") stopped = true;
      },
    ).finally(() => {
      inflight.delete(entry);
    });
    inflight.add(entry);
  };

  for await (const doc of documents) {
    if (stopped) break;
    const bytes = encoder.encode(JSON.stringify(doc)).length + 1;
    const existing = current.get(doc.id);
    if (existing === undefined) {
      if (current.size > 0 && (current.size >= maxDocsPerBatch || ENVELOPE_BYTES + currentBytes + bytes > maxBytesPerBatch)) {
        await dispatch();
      }
      // A later write to an id that is still in flight waits for it, so the
      // later occurrence always wins.
      const blockers = [...inflight].filter((f) => f.ids.has(doc.id));
      if (blockers.length > 0) {
        if (current.size > 0) await dispatch();
        await Promise.all(blockers.map((f) => f.done));
        if (stopped) break;
      }
      current.set(doc.id, { doc, bytes });
      currentBytes += bytes;
    } else {
      current.set(doc.id, { doc, bytes });
      currentBytes += bytes - existing.bytes;
    }
  }
  if (!stopped && current.size > 0) await dispatch();
  while (inflight.size > 0) await Promise.all([...inflight].map((f) => f.done));
  result.failures.sort((a, b) => a.batchIndex - b.batchIndex);
  if (result.failures.length > 0 && onError === "stop") throw new BulkError(result);
  return result;
}
