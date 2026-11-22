# #383 Matryoshka and native binary embeddings: feasibility

Status: feasibility study, research track (#260). Nothing in this document changes Kosha's
behaviour. The PR that adds it also adds an offline simulator,
`scripts/bench/bm25_scale/repr_recall_sim.py`, and contains no Rust change.

## Experiment spec

- **Hypothesis.** If the vector field's search representation is the first `d` dims
  (Matryoshka) or one bit per dim, and the final shortlist is rescored with the full vector,
  recall@10 stays within 0.01 of full-precision search with an index at least 4× smaller.
- **Change (this PR).** None in the engine. This PR adds a brute-force simulator that measures
  the representation loss alone, with no ANN index involved, and a design for the schema and
  format change.
- **Flag (future).** The search representation is declared per field in the namespace schema
  (`search_representation`, see below). It is absent by default, and it writes a new vector
  index format version only when it is declared. A server-wide kill switch
  `KOSHA_VECTOR_SEARCH_REPR=off` covers the rollout.
- **Metric.** recall@10 after full-dimension rescoring of a shortlist of R candidates
  (globally, or R per segment as Kosha does it), plus bytes per document for **both** parts
  of the index: the search representation and the rescore store. Once there is an index:
  warm p50/p99, local vector bytes and postings probed per query.
- **Success bar (from #383).** At least 4× smaller index, with recall within 0.01 after
  rescoring. This document keeps that bar and counts it on **total vector bytes per
  document** (`vector.idx` entry + `doc_vectors.bin` entry). A narrower bar, "4× fewer
  *local probe* bytes", is a different claim; section 3c says when it would be the right one.

## TL;DR

1. **Sage embeds with OpenAI `text-embedding-3-small`, 1536 dims, as f32.** OpenAI documents
   that this model family was trained so that embeddings can be shortened. Sage can therefore
   truncate its existing vectors **without re-embedding**: take a prefix and re-normalize. The
   model has **no** native int8 or binary output.
2. **The bench corpus is Cohere `embed-english-v3`, 1024 dims.** That model has native
   int8/binary outputs, but Cohere says `output_dimension` (Matryoshka) is only available on
   embed-v4 and newer models. Truncating the bench vectors is therefore a negative control,
   and bench results for truncation would not transfer to Sage.
3. **No design in this document meets the #383 bar of a 4× smaller index.** Kosha keeps the
   full f32 vector in `doc_vectors.bin` for rescoring, and that file is as large as today's
   f32 `vector.idx`. Shrinking only the search representation therefore saves at most 2× in
   total. At 1536 dims, today's layout is 6144 + 6144 = 12,288 B per document. A 384-dim
   prefix makes it 1536 + 6144 = 7680 B (**1.6× smaller**, not 4×). Sign bits make it
   192 + 6144 = 6336 B (1.94×). Getting to 4× in total needs a smaller rescore store as
   well (section 3c), which is not measured here.
4. **Offline simulation** (brute force, about 100k-doc samples, Apple M5). The
   `text-embedding-3-small` numbers come from **document-to-document neighbours on DBpedia
   entities** (held-out documents used as queries), not from short queries over passages.
   That setting is probably easier for both truncation and sign bits. No public
   query-to-passage `text-embedding-3-small` corpus was found, so none was run.
   - With one global shortlist: a 384-dim prefix (4× smaller search representation) needs
     R=100 for 0.994 recall@10; sign bits scored against the f32 query (32× smaller search
     representation) reach 0.995 at R=40.
   - With the shortlist taken **per shard**, as Kosha does per segment (two shards of about
     50k docs, about the size of a bench segment), both do better: 0.990 and 0.9999 at
     R=40 per shard. How these R values map to `KOSHA_VECTOR_RESCORE_FACTOR` inside the
     engine, with IVF probing, is **not measured**.
   - In every setting simulated, sign bits rank ahead of truncation at a smaller search
     representation. For `text-embedding-3-small` this rests on doc-to-doc data only.
   - On Cohere v3 (real queries over passages), truncation fails as expected: 256 dims gives
     0.795 at global R=200, and 0.863 at R=200 per 50k shard.
5. **Recommendation.**
   - Do not build a Matryoshka-specific search path first.
   - The 1-bit code path (#381, RaBitQ v5 already in the tree) shrinks the search
     representation more than truncation does, with no API change and no re-embedding. It
     has the same total-bytes limit as item 3.
   - Whether the 4× bar can be met depends on the rescore store, not on the search
     representation. The options are storing a 768-dim prefix (or int8) in `doc_vectors.bin`, or keeping it cold and reading
     it by range from S3 as #381 plans (section 3c). Matryoshka could help with the first of
     these and with centroid scoring.
   - All of this needs measurement on Sage-like query-to-passage data first (plan below).

## 1. What produces `contentEmbedding`

The Kosha repo does not say. `DESIGN.md` §13 says "Kosha never generates embeddings itself …
`query_embedding` computed by Sage/Pandora". The clients and tests use toy vectors. The answer
comes from the Sage backend repo (`decover-tech/backend`, read locally, 2026-11-21):

| Fact | Source (backend repo) |
|---|---|
| Model `text-embedding-3-small` | `common/settings.py`: `OPENAI_EMBEDDING_MODEL = "text-embedding-3-small"`; OpenRouter route `openai/{OPENAI_EMBEDDING_MODEL}` |
| Provider: OpenRouter first, OpenAI direct as fallback, same model, "cosine 1.000000" between them | `common/settings.py` (`EMBEDDING_PROVIDER` comment), `common/llm/client.py` `_embeddings_create` |
| Called as `embeddings.create(input=…, model=…)`: **no `dimensions`, default `encoding_format`** (float), so full 1536 dims | `common/llm/client.py` ~L827 |
| Index mapping `contentEmbedding = KnnVector(dimension=1536)` (paragraph, case, page, completion indices; findings uses `content_embedding`) | `common/index/paragraph_repository.py`, `case_repository.py`, `page_repository.py`, `findings_repository.py` |
| Written by the ingest sink `contentEmbedding=chunk.embeddings` | `hulk/sink_processors/paragraph_repository_processor.py` |
| Price $0.02 per 1M input tokens | `common/usage/price_seed.py` |

Consequences:

- **Sage vectors are 1536-dim, not 1024.** That is 6 KB per vector in f32, 1.5× the bench
  corpus. Bench latencies and byte counts at 1024 dims understate Sage's cost.
- If Sage ever passes `dimensions=` or switches models, the Kosha field's dimension changes.
  Per `DESIGN.md` §6, dimensions are fixed per namespace at the first vector written, and #209
  refuses a model change while vectors exist. Either change means a new namespace or a
  re-index.

## 2. Which models can do what (vendor claims, checked 2026-11-21)

These are vendor documentation claims, not measurements. Truncate-and-renormalize is the
documented manual alternative to the `dimensions` parameter.

| Model | Matryoshka / shortening | Native int8 / binary output | Source |
|---|---|---|---|
| OpenAI `text-embedding-3-small` (Sage) | Yes: `dimensions` param; docs show manual "slice then L2-normalize"; trained with MRL (footnote in the launch post) | **No**: `encoding_format` is `float` or `base64` only | developers.openai.com/api/docs/guides/embeddings; …/api/reference/resources/embeddings/methods/create; openai.com/index/new-embedding-models-and-api-updates |
| OpenAI `text-embedding-3-large` | Yes, same mechanism (3072 default); "shortened to 256 still outperforms ada-002 at 1536" on MTEB (vendor claim) | No | same |
| Cohere `embed-english-v3.0` (bench corpus) | **No**: "`output_dimension` … only available for embed-v4 and newer"; fixed 1024 | Yes: `embedding_types` int8, uint8, binary, ubinary "supported with Embed v3.0 and newer" | docs.cohere.com/reference/embed; docs.cohere.com/docs/cohere-embed |
| Cohere `embed-v4.0` | Yes: 256 / 512 / 1024 / 1536 (default) | Yes (v3.0 and newer) | same |
| Voyage `voyage-3-large`, `voyage-3.5`, `voyage-4*`, `voyage-code-3` | Yes: `output_dimension` 2048 / 1024 (default) / 512 / 256 | Yes: `output_dtype` int8, uint8, binary, ubinary | docs.voyageai.com/docs/embeddings |

**Not verified:**

- Whether Cohere's or Voyage's native `binary` is anything more than `sign(float)` packed into
  bits, that is, whether it was trained with compression in mind. Cohere's int8/binary blog
  post did not render, and their API docs do not say.
- How to find out: embed a few hundred passages with `embedding_types=["float","ubinary"]` and
  compare `ubinary` with `packbits(float > 0)`. If they are identical, native binary adds
  nothing over Kosha deriving the bits itself.

## 3. Offline simulation (measured)

Tool: `scripts/bench/bm25_scale/repr_recall_sim.py` (numpy only).

- **Ground truth:** exact cosine top-10 over the sample with full vectors.
- **Each representation:** pick the top R by the approximate score, rescore those R with the
  exact full-dim score, and count overlap with the ground truth.
- **Global or per segment.** With `--segments 1` (the tables in 3a and 3b), R is **one
  global shortlist** over the whole sample. Kosha does not work that way: each segment
  rescores its own best `k * vector_rescore_factor` probe candidates
  (`SegmentReader::finish_probe`, `crates/kosha-segment/src/lib.rs`), and the merge takes
  the best k. `--segments N` models that by splitting the sample into N contiguous shards,
  taking the top R in each, and rescoring the union (section 3c).
- **Not a bound on Kosha's recall in either direction.** The simulator has no IVF probing, so
  it misses nothing Kosha's probe would miss. A 100k sample is less dense than 10M documents.
  RaBitQ columns widen their shortlist by an error bound. Against that, the global-R tables
  ask more than the engine does. The mapping from these R values to
  `KOSHA_VECTOR_RESCORE_FACTOR` is **not measured**.
- **Bytes.** "B/vec" is the search representation only: the per-entry payload of
  `vector.idx`, ignoring the 4-byte doc id and any posting replication. "Total ×" compares
  search representation plus the 4·D f32 rescore vector in `doc_vectors.bin` with today's
  f32 layout (4·D + 4·D). The upper limit of "total ×" is 2 while the rescore store holds full
  f32 vectors.

Machine: Apple M5 (arm64), macOS, numpy 2.0.2 (Accelerate BLAS). numpy 2.0.x on Accelerate
prints spurious "divide by zero / overflow in matmul" warnings. Checked: all scores are finite,
and the max |f32 − f64| is 1.6e-6.

### 3a. Sage's model: `text-embedding-3-small`, 1536-d

- **Data:** `Qdrant/dbpedia-entities-openai3-text-embedding-3-small-1536-100K` (public HF
  dataset). 1000 random documents held out as queries; 99,000 documents form the corpus.
  `scripts/bench/bm25_scale/fetch_te3s_dbpedia.py` reproduces the split (seed 383).
- **Caveat, which applies to every `text-embedding-3-small` number in this document:** these
  are **document-to-document** neighbours on DBpedia entities, not short queries over
  passages, and not Sage queries over legal paragraphs. Doc-to-doc neighbourhoods (near
  duplicate entities, high top-1 similarity) are likely easier for both truncation and sign
  bits. No public query-to-passage corpus embedded with this model was found (HF search,
  2026-11-21), and embedding one needs an OpenAI key, so none was run (plan step 1a).

recall@10 after full-dim rescore of one **global** top R (`--segments 1`). "Total ×" is
total vector bytes per document versus today's f32 `vector.idx` + `doc_vectors.bin`
(12,288 B at 1536-d):

| repr | B/vec | search × | total × | R=10 | R=40 | R=100 | R=200 |
|---|---:|---:|---:|---:|---:|---:|---:|
| int8 (v4-style) | 1540 | 4.0 | 1.60 | 0.9960 | 1.0000 | 1.0000 | 1.0000 |
| bin (sign, Hamming) | 192 | 32 | 1.94 | 0.6981 | 0.9673 | 0.9955 | 0.9992 |
| bin-asym (sign docs, f32 query) | 192 | 32 | 1.94 | 0.7949 | 0.9949 | 1.0000 | 1.0000 |
| trunc768 | 3072 | 2 | 1.33 | 0.8287 | 0.9988 | 1.0000 | 1.0000 |
| trunc512 | 2048 | 3 | 1.50 | 0.7667 | 0.9887 | 0.9994 | 0.9997 |
| **trunc384** | 1536 | **4** | **1.60** | 0.6972 | 0.9639 | 0.9942 | 0.9986 |
| trunc256 | 1024 | 6 | 1.71 | 0.5931 | 0.8944 | 0.9696 | 0.9897 |
| trunc128 | 512 | 12 | 1.85 | 0.4228 | 0.6992 | 0.8330 | 0.9015 |
| trunc768-bin | 96 | 64 | 1.97 | 0.5640 | 0.8735 | 0.9605 | 0.9868 |
| trunc512-bin | 64 | 96 | 1.98 | 0.4852 | 0.7842 | 0.9026 | 0.9557 |
| trunc384-bin | 48 | 128 | 1.98 | 0.4164 | 0.6920 | 0.8276 | 0.9045 |
| trunc256-bin | 32 | 192 | 1.99 | 0.3082 | 0.5455 | 0.6913 | 0.7838 |
| trunc128-bin | 16 | 384 | 1.99 | 0.1548 | 0.3013 | 0.4147 | 0.5048 |

### 3b. Bench model: Cohere `embed-english-v3`, 1024-d (negative control for truncation)

- **Data:** the first 100,000 passages of `CohereLabs/msmarco-v2.1-embed-english-v3`,
  `passages_npy/…_00.npy`, the same prefix the bench corpus starts with. Stored as f16 and
  widened to f32.
- **Queries:** all 1677 TREC queries from `queries_parquet`.

Global top R (`--segments 1`); total × is against 8,192 B per document at 1024-d:

| repr | B/vec | search × | total × | R=10 | R=40 | R=100 | R=200 |
|---|---:|---:|---:|---:|---:|---:|---:|
| int8 (v4-style) | 1028 | 4.0 | 1.60 | 0.9941 | 1.0000 | 1.0000 | 1.0000 |
| bin (sign, Hamming) | 128 | 32 | 1.94 | 0.5113 | 0.8162 | 0.9226 | 0.9661 |
| bin-asym | 128 | 32 | 1.94 | 0.6510 | 0.9419 | 0.9883 | 0.9973 |
| trunc512 | 2048 | 2 | 1.33 | 0.5584 | 0.8603 | 0.9474 | 0.9766 |
| trunc256 | 1024 | 4 | 1.60 | 0.3177 | 0.5647 | 0.7049 | 0.7950 |
| trunc128 | 512 | 8 | 1.78 | 0.1367 | 0.2624 | 0.3713 | 0.4639 |
| trunc512-bin | 64 | 64 | 1.97 | 0.3039 | 0.5404 | 0.6887 | 0.7798 |
| trunc256-bin | 32 | 128 | 1.98 | 0.1414 | 0.2727 | 0.3822 | 0.4735 |
| trunc128-bin | 16 | 256 | 1.99 | 0.0458 | 0.1002 | 0.1540 | 0.2092 |

Run time: about 19 s and 3.8 GB peak RSS for 3b with `--segments 1`. 3a is similar. With
`--segments 1,2,10` each run took about 55 s and 4.2 to 4.8 GB peak RSS.

### 3c. Per-segment shortlists and total bytes

**Per-segment shortlist.** The same two samples, with R taken **per shard** (`--segments 2`
and `--segments 10`) and the union rescored. Two shards of about 50k documents are about the
size of one bench segment (10M documents across about 200 segments). At Kosha's default
`KOSHA_VECTOR_RESCORE_FACTOR=4` and k=10, the per-segment shortlist is 40. The simulator still
has no IVF probing, so these numbers are **not** Kosha's recall at that factor.

| corpus | repr | search × | global R=40 | per-shard R=40, 2 × ~50k | per-shard R=40, 10 × ~10k | global R=100 | per-shard R=100, 2 × ~50k |
|---|---|---:|---:|---:|---:|---:|---:|
| te3-small (doc→doc) | bin-asym | 32 | 0.9949 | 0.9999 | 1.0000 | 1.0000 | 1.0000 |
| te3-small (doc→doc) | trunc768 | 2 | 0.9988 | 0.9999 | 1.0000 | 1.0000 | 1.0000 |
| te3-small (doc→doc) | trunc384 | 4 | 0.9639 | 0.9903 | 0.9996 | 0.9942 | 0.9984 |
| te3-small (doc→doc) | trunc256 | 6 | 0.8944 | 0.9560 | 0.9962 | 0.9696 | 0.9899 |
| Cohere v3 (query→passage) | bin-asym | 32 | 0.9419 | 0.9754 | 0.9937 | 0.9883 | 0.9962 |
| Cohere v3 (query→passage) | trunc256 | 4 | 0.5647 | 0.6581 | 0.8236 | 0.7049 | 0.7907 |

Per-shard shortlists are easier than a global one, because a true neighbour only has to rank in its own shard's top R. More shards also means more
candidates rescored in total (N × R). The global-R tables are therefore **not** an upper
bound on Kosha, and the earlier claim that trunc384 "needs R=100" does not carry over to
the engine. Neither does "sign bits need a shorter shortlist". What holds in every setting
simulated is the **ordering**: at a given R, sign bits scored against the f32 query match or
beat a 4× prefix while being 8× smaller.

**Total bytes.** The rescore store is the floor. Per document, at 1536-d:

| layout | search repr (`vector.idx`) | rescore store (`doc_vectors.bin`) | total | vs today |
|---|---:|---:|---:|---:|
| today, f32 + sidecar | 6144 | 6144 | 12,288 | 1.00× |
| int8 (v4) + f32 rescore | 1540 | 6144 | 7684 | 1.60× |
| trunc384 f32 + f32 rescore | 1536 | 6144 | 7680 | 1.60× |
| sign bits / RaBitQ-like + f32 rescore | 192 (RaBitQ v5: 204 incl. id and factors) | 6144 | 6336 | 1.94× |
| sign bits + 768-dim f32 prefix as rescore store | 192 | 3072 | 3264 | 3.76× |
| sign bits + int8 rescore store | 192 | 1540 | 1732 | 7.1× |

Doc ids, posting replication and the doc store are left out. The last two rows are
arithmetic only: the simulator does not model a reduced rescore store after a reduced search
representation, so their recall is **not measured**. Their single-stage numbers (trunc768
0.9988 at global R=40 on te3-small doc→doc, int8 0.996 at R=10) suggest they are worth
simulating next (plan step 1).

So the 4× bar can be met in two ways, and this document has not shown either:

- **Shrink the rescore store too.** For example, sign bits for search plus a 768-dim prefix
  (Matryoshka models only) or int8 for rescoring. This needs the two-stage simulation, and
  rescoring would no longer be exact.
- **Narrow the bar to local probe bytes.** Keep `doc_vectors.bin` cold in S3 and read only
  the shortlist by range at query time, as #381 plans. Local bytes then are the search
  representation (4× for trunc384, 32× for bits), and S3 storage stays at about 1.6× to
  1.94× smaller. This depends on ranged S3 rescore reads fitting the latency budget
  (k × factor × segments probed reads per query), which #381 has to measure. It is a
  narrower bar than the one #383 states, and should be called that.

### Reading the numbers

- **Matryoshka truncation works on Sage's model in the doc-to-doc setting, as documented by
  OpenAI.** A 4× smaller search representation (384 dims) reaches 0.99 or better with
  R=100 globally or R=40 per 50k shard. It gives only 1.6× smaller total vector bytes while
  the rescore store holds full f32 vectors. Query-to-passage behaviour for this model is not
  measured.
- **Sign bits rank ahead of truncation in every setting simulated** (both corpora, global
  and per-shard R). For te3-small that ranking rests on doc-to-doc data only. This is the
  regime RaBitQ (#381) already targets. RaBitQ adds a random rotation and an error bound on
  top of plain sign bits, so it should do at least as well; that is **not measured here**.
- **Truncation and bits stack badly.** `trunc384-bin` (128×) is 0.90 at global R=200, and
  on Cohere v3 every truncated variant fails.
- **Representation loss is small compared with the bench recall gap.** The bench baseline
  recall@10 of ≈0.95 cannot come from int8 representation loss alone (int8 reaches 0.994 at
  global R=10 here). That suggests IVF probing, but it was not measured on the engine.

## 4. What Kosha would need

Kosha today:

- A `Vector` field indexes exactly the vector it is given.
- The per-column posting entry format is f32, i8 (v4) or RaBitQ (v5), chosen by
  `KOSHA_VECTOR_QUANTIZATION[_FIELDS]` (`crates/kosha-segment/src/lib.rs` ~L1592-1645,
  ~L3502).
- Rescoring uses `doc_vectors.bin` (`KOSHA_VECTOR_RESCORE_FACTOR`, default 4).
- There is no truncation and no binary ingest path.

The idea in #383 is a field whose **search representation** (what the IVF index stores and
probes) differs from its **rescore representation** (what final scores come from).

### 4.1 Schema (namespace config)

A new optional key on `FieldSchema` (`crates/kosha-core/src/analysis.rs`), next to #209's
`embedding`:

```json
"contentEmbedding": {
  "type": "vector",
  "embedding": {"model": "text-embedding-3-small", "dims": 1536},
  "search_representation": {
    "prefix_dims": 384,
    "encoding": "f32"
  }
}
```

- `prefix_dims` (optional): index only the first N dims, re-normalized. It must be less than
  the field's dimension. It is valid **only** when the field's model is known to be
  Matryoshka-trained. Kosha cannot check that, so it is a caller assertion. Document it, and
  record it in the footer so a mistaken setting can be found and reverted.
- `encoding` (optional, default `f32`): `f32 | i8 | rabitq`. This moves today's server-wide
  `KOSHA_VECTOR_QUANTIZATION_FIELDS` into the schema, where it belongs per field. The env vars
  stay as the fallback.
- The rescore representation is always the full vector the document supplied, stored in
  `doc_vectors.bin`. A field with `search_representation` **requires** `doc_vectors.bin`. If
  that file is missing, the segment must report degraded (`knn_degraded_segments`) rather
  than silently return approximate scores. #381 flags the same silent-recall risk for v5
  today.
- `search_representation` is absent by default. A schema without it serializes exactly as
  today, because the field is `skip_serializing_if = "Option::is_none"`, the same pattern as
  `embedding`.

### 4.2 Segment format

- `VectorFieldMeta` gains an optional `search_dims: Option<u32>`, omitted when `None` so
  existing footers stay byte-identical. `dim` keeps meaning "dimension of the vectors in this
  column", which is the prefix length.
- A new field `source_dim` records the full length that queries arrive with.
- **Old-reader safety.** A footer whose `dim` is 384 while queries arrive at 1536 would make
  an old reader answer 400 ("dimension differs") or skip the column. Gate it the same way as
  v4 and v5:
  - a new vector index version (6: "prefix column", entries in any encoding of v3-v5);
  - written only when the schema declares `prefix_dims` and the kill switch is not `off`;
  - rolled out readers first.
- `doc_vectors.bin` is unchanged and holds the full vector.

### 4.3 Query path

- `knn.vector` stays full-dimension, so clients do not change. The searcher derives the
  probe vector: take the prefix, then re-normalize, then quantize if the encoding needs it.
  The probe ranks centroids and postings with it.
- The shortlist of `k * vector_rescore_factor` per segment is rescored with the full vector.
  #381's global, bound-driven rescore is the better long-term home for this step.
- Native embedding (#209) embeds once at full dimension and derives the prefix server-side.
  There is no extra provider call.
- Asymmetric scoring (f32 query against sign bits) is what made the binary rows above work at
  small R. Kosha's RaBitQ estimator is already asymmetric.

### 4.4 Native int8 or binary ingest

Accepting a vendor's int8 or binary vector (`{"ubinary": "<base64>"}` or a separate
`BinaryVector` type) only pays off if native codes are better than codes Kosha derives from the
float. That is unverified (section 2), and Sage's model has no such output. Kosha would still
need the float to rescore, so callers would send both representations. **Do not build it** until:

- a model in use offers native codes; and
- the check in section 2 shows they differ from `sign(float)`.

## 5. Interaction with RaBitQ codes (#381) and bitwise kernels (#382)

- **#381 subsumes the binary half of this issue** on the models we have:
  - On Sage's model (doc-to-doc data), 1-bit sign codes plus full-dim rescoring reach 0.995
    recall at global R=40 (asymmetric) with a 32× smaller search representation. Total
    vector bytes shrink only 1.94× while `doc_vectors.bin` holds f32 (section 3c).
  - RaBitQ (v5) is the principled version of the same idea: 140 B at 1024-d per the v5 doc
    comment, and 204 B at 1536-d (`[id:4][norm:4][ip:4][1536/8 bits]`, computed from the v5 entry layout, not measured).
  - #381's bench runs on the Cohere corpus are valid for the binary question because they do
    not depend on Matryoshka.
- **Matryoshka composes with RaBitQ only weakly.** RaBitQ codes of a 384-prefix would be about
  48 B plus overhead per vector. The `trunc384-bin` row (0.90 at R=200) suggests the recall
  cost is too high. The RaBitQ error bound might recover some of it, but that is not measured.
- **Where Matryoshka could still help next to #381:**
  - Centroid scoring: IVF centroids are f32 and resident. Every kNN scores the centroids of all about 200 bench
    segments; a 384-dim centroid probe cuts that dot cost about 4× (arithmetic, not measured).
  - Rescore bytes: on Sage's model (doc-to-doc), `trunc768` alone recovers the full-dim
    top-10 almost exactly (0.9988 at global R=40). Used **as the rescore vector** it would
    halve `doc_vectors.bin` and the ranged S3 rescore reads #381 plans (6 KB → 3 KB per
    vector). This is the main way Matryoshka could help reach the 4× total bar (section 3c:
    sign bits + 768-dim rescore store is 3.76× by arithmetic). The two-stage recall is not
    measured.
- **#382** (popcount kernels and an x86-64-v3 build) speeds up any 1-bit search representation.
  It does not depend on this issue.

## 6. Experiment plan

Ordered from cheapest to most expensive. Steps 1 and 2 need no engine change.

1. **Sage-domain simulation, offline (no VM).**
   - Export a sample of real Sage `contentEmbedding` vectors (for example 200k paragraphs from
     one large matter) plus logged query embeddings, or re-embed a few hundred logged query
     strings.
   - Run `repr_recall_sim.py --truncate 768,512,384,256 --shortlist 10,20,40,100,200
     --segments 1,4` (4 shards of 50k is about one bench segment each).
   - Extend the simulator with a two-stage mode (reduced search representation, then a
     reduced rescore store: 768-dim prefix or int8) to measure the layouts in section 3c
     that could reach 4× in total.
   - Pass for Matryoshka as a search representation: `trunc384` at or above 0.99 at R=40 per
     shard. Pass for the 4× total bar: some two-stage layout at or above 0.99 at R=40 per
     shard with total bytes at least 4× smaller. Otherwise Matryoshka is dropped.
   - Needs a data-access decision (tenant data). Owner: Sage team.
1a. **Public query-to-passage check, offline (no VM).** If Sage data is slow to get, embed the
   MSMarco dev queries and a 100k passage sample with `text-embedding-3-small` and rerun the
   simulator, so the representation ranking does not rest on doc-to-doc data alone. Needs an
   OpenAI key and a small spend (token count not measured); not run in this PR.
2. **Native vs derived binary check, offline.** Embed about 500 MSMarco passages with Cohere
   `embedding_types=["float","ubinary"]` and compare with `packbits(float>0)`. If they are
   identical, close the native-binary half of #383.
3. **A Matryoshka-capable bench corpus (VM, only if step 1 passes).**
   - Embed the 10M MSMarco passages with `text-embedding-3-small`.
   - Cost: measure tokens with tiktoken on a 10k sample first, then extrapolate at $0.02 per 1M
     tokens. Not estimated here because the passage token count is not measured.
   - Upload the embeddings to `s3://decoverai-bench-corpus-cache/msmarco-10m-emb-te3s/` and
     build a new snapshot `msmarco-10m-vec-te3s1536-sidecar`. This needs a fetch/load path for
     1536-d embeddings; `fetch_msmarco_embeddings.py` hard-codes `DIM = 1024`.
   - Run the f32 baseline at 1536-d on it. This is the honest baseline for Sage, since the
     1024-d Cohere numbers do not transfer.
4. **Engine prototype (only if 1 and 3 justify it).**
   - Implement `search_representation.prefix_dims` with format v6 behind the schema key and
     the `KOSHA_VECTOR_SEARCH_REPR` kill switch.
   - Build snapshots `msmarco-10m-vec-te3s-p384-sidecar` and `…-p256-sidecar`.
   - Compare against the step 3 baseline and against `rabitq` on the same corpus: recall@10
     curves against p50/p99, local vector bytes, warmup time.
   - Success bar, as #383 states it: total vector bytes per document (`vector.idx` +
     `doc_vectors.bin`) at least 4× smaller, recall@10 within 0.01 of the f32 baseline, p50
     no worse. A prefix search representation with a full f32 rescore store cannot meet it
     (1.6× at 384 dims), so this step only makes sense together with step 5 or with a cold,
     range-read `doc_vectors.bin`.
   - If the bar is instead narrowed to **local probe bytes** (with `doc_vectors.bin` kept in
     S3 and read by range for the shortlist), say so explicitly, and add #381's measurement of
     ranged rescore latency at p99 as a condition.
5. **Rescore-prefix variant (with #381).** Store a 768-dim prefix in `doc_vectors.bin` and
   rescore with it. Measure recall@10 against full-dim ground truth and the rescore bytes per
   query.

## 7. Risks and open questions

- **Model dependence.** All of this depends on the model. A Sage model change (to
  `-3-large`, Cohere v4 or Voyage) changes the answer and needs a re-embed and a re-index.
- **Corpus mismatch.**
  - Every `text-embedding-3-small` number here is doc-to-doc on DBpedia entities, not
    queries over passages, and not legal paragraphs with real queries.
  - The bench corpus cannot test Matryoshka at all.
  - Decisions must wait for experiment step 1.
- **Silent recall loss.** A wrong `prefix_dims` on a non-MRL model gives terrible recall with
  no error (section 3b). Mitigations:
  - an optional ingest-time self-check: sample cosine(prefix, full) ranking agreement on the
    first segment build and warn;
  - the per-PR recall suite (#258).
- **Simulator to engine mapping.** The simulator's R (global or per shard) is not
  `KOSHA_VECTOR_RESCORE_FACTOR`: Kosha only sees candidates its IVF probe reached, and RaBitQ
  widens the shortlist by an error bound. Only a bench run maps one to the other.
- **`doc_vectors.bin` availability.** Search-representation fields are useless without the
  rescore vectors. The same rollout dependency #381 has.
