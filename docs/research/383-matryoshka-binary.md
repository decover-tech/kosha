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
- **Metric.** recall@10 after full-dimension rescoring of the top R candidates (R = 10, 40,
  100, 200), plus bytes per vector for the search representation. Once there is an index:
  warm p50/p99, local vector bytes and postings probed per query.
- **Success bar (from #383).** At least 4× smaller index, with recall within 0.01 after
  rescoring.

## TL;DR

1. **Sage embeds with OpenAI `text-embedding-3-small`, 1536 dims, as f32.** OpenAI documents
   that this model family was trained so that embeddings can be shortened. Sage can therefore
   truncate its existing vectors **without re-embedding**: take a prefix and re-normalize. The
   model has **no** native int8 or binary output.
2. **The bench corpus is Cohere `embed-english-v3`, 1024 dims.** That model has native
   int8/binary outputs, but Cohere says `output_dimension` (Matryoshka) is only available on
   embed-v4 and newer models. Truncating the bench vectors is therefore a negative control,
   and bench results for truncation would not transfer to Sage.
3. **Offline simulation** (brute force, about 100k-doc samples, Apple M5):
   - On a public `text-embedding-3-small` sample, a 384-dim prefix (4× smaller) with the top
     100 rescored reaches **0.994** recall@10, which meets the bar.
   - Plain sign bits per dimension (32× smaller), scored asymmetrically with the f32 query and
     the top 40 rescored, reach **0.995**.
   - So **binary or RaBitQ codes beat Matryoshka truncation at every size tested**, on both
     models.
   - On Cohere v3, truncation fails as expected: 256 dims gives 0.795 even at R=200.
4. **Recommendation.**
   - Do not build a Matryoshka-specific search path first.
   - The 1-bit code path (#381, RaBitQ v5 already in the tree) gets the larger reduction on
     Sage's model and on the bench model, with no API change and no re-embedding.
   - Matryoshka is worth a second look only for the parts 1-bit codes do not shrink:
     - centroid and probe cost;
     - full-precision rescore bytes, by rescoring with a 768-dim prefix and storing that
       instead of 1536 dims.
   - Both need measurement on Sage-like data first (plan below).

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
- **What this measures:** representation loss only, with no ANN index. Kosha's IVF probing can
  only lose more.
- **Smaller sample, easier corpus:** these figures are upper bounds for 10M documents.

Machine: Apple M5 (arm64), macOS, numpy 2.0.2 (Accelerate BLAS). numpy 2.0.x on Accelerate
prints spurious "divide by zero / overflow in matmul" warnings. Checked: all scores are finite,
and the max |f32 − f64| is 1.6e-6.

### 3a. Sage's model: `text-embedding-3-small`, 1536-d

- **Data:** `Qdrant/dbpedia-entities-openai3-text-embedding-3-small-1536-100K` (public HF
  dataset). 1000 random documents held out as queries; 99,000 documents form the corpus.
- **Caveat:** these are **document-to-document** neighbours on DBpedia entities, not Sage
  queries over legal paragraphs.

recall@10 after full-dim rescore of the top R:

| repr | B/vec | × smaller | R=10 | R=40 | R=100 | R=200 |
|---|---:|---:|---:|---:|---:|---:|
| int8 (v4-style) | 1540 | 4.0 | 0.9960 | 1.0000 | 1.0000 | 1.0000 |
| bin (sign, Hamming) | 192 | 32 | 0.6981 | 0.9673 | 0.9955 | 0.9992 |
| bin-asym (sign docs, f32 query) | 192 | 32 | 0.7949 | 0.9949 | 1.0000 | 1.0000 |
| trunc768 | 3072 | 2 | 0.8287 | 0.9988 | 1.0000 | 1.0000 |
| trunc512 | 2048 | 3 | 0.7667 | 0.9887 | 0.9994 | 0.9997 |
| **trunc384** | 1536 | **4** | 0.6972 | 0.9639 | **0.9942** | 0.9986 |
| trunc256 | 1024 | 6 | 0.5931 | 0.8944 | 0.9696 | 0.9897 |
| trunc128 | 512 | 12 | 0.4228 | 0.6992 | 0.8330 | 0.9015 |
| trunc768-bin | 96 | 64 | 0.5640 | 0.8735 | 0.9605 | 0.9868 |
| trunc512-bin | 64 | 96 | 0.4852 | 0.7842 | 0.9026 | 0.9557 |
| trunc384-bin | 48 | 128 | 0.4164 | 0.6920 | 0.8276 | 0.9045 |
| trunc256-bin | 32 | 192 | 0.3082 | 0.5455 | 0.6913 | 0.7838 |
| trunc128-bin | 16 | 384 | 0.1548 | 0.3013 | 0.4147 | 0.5048 |

### 3b. Bench model: Cohere `embed-english-v3`, 1024-d (negative control for truncation)

- **Data:** the first 100,000 passages of `CohereLabs/msmarco-v2.1-embed-english-v3`,
  `passages_npy/…_00.npy`, the same prefix the bench corpus starts with. Stored as f16 and
  widened to f32.
- **Queries:** all 1677 TREC queries from `queries_parquet`.

| repr | B/vec | × smaller | R=10 | R=40 | R=100 | R=200 |
|---|---:|---:|---:|---:|---:|---:|
| int8 (v4-style) | 1028 | 4.0 | 0.9941 | 1.0000 | 1.0000 | 1.0000 |
| bin (sign, Hamming) | 128 | 32 | 0.5113 | 0.8162 | 0.9226 | 0.9661 |
| bin-asym | 128 | 32 | 0.6510 | 0.9419 | 0.9883 | 0.9973 |
| trunc512 | 2048 | 2 | 0.5584 | 0.8603 | 0.9474 | 0.9766 |
| trunc256 | 1024 | 4 | 0.3177 | 0.5647 | 0.7049 | 0.7950 |
| trunc128 | 512 | 8 | 0.1367 | 0.2624 | 0.3713 | 0.4639 |
| trunc512-bin | 64 | 64 | 0.3039 | 0.5404 | 0.6887 | 0.7798 |
| trunc256-bin | 32 | 128 | 0.1414 | 0.2727 | 0.3822 | 0.4735 |
| trunc128-bin | 16 | 256 | 0.0458 | 0.1002 | 0.1540 | 0.2092 |

Run time: about 19 s and 3.8 GB peak RSS for 3b. 3a is similar.

### Reading the numbers

- **Matryoshka works for Sage's model, as documented.** A 4× smaller prefix (384 dims) passes
  the bar at R=100, and 6× (256 dims) gets close at R=200 (0.990).
- **Sign bits are the better trade on both models.** At 32× smaller, they need a shorter
  shortlist than a 4× prefix (R=40 asymmetric on 3a). This is the regime RaBitQ (#381) already
  targets. RaBitQ adds a random rotation and an error bound on top of plain sign bits, so it
  should do at least as well; that is **not measured here**.
- **Truncation and bits stack badly.** `trunc384-bin` (128×) is 0.90 at R=200, and on Cohere
  v3 every truncated variant fails.
- **The bench recall gap is about the index, not the representation.** The bench baseline
  recall@10 of ≈0.95 comes from IVF probing, not from representation loss (int8 already
  reaches 0.994 at R=10 here).

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
  - On Sage's model, 1-bit sign codes plus rescoring reach 32× smaller at 0.995 recall (R=40,
    asymmetric).
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
  - Rescore bytes: on Sage's model, `trunc768` used **as the rescore vector** recovers the
    full-dim top-10 almost exactly (0.9988 at R=40). That halves `doc_vectors.bin` and the
    ranged S3 rescore reads #381 plans (6 KB → 3 KB per vector). This is a separate,
    measurable variant: rescore with a stored prefix instead of the full vector.
- **#382** (popcount kernels and an x86-64-v3 build) speeds up any 1-bit search representation.
  It does not depend on this issue.

## 6. Experiment plan

Ordered from cheapest to most expensive. Steps 1 and 2 need no engine change.

1. **Sage-domain simulation, offline (no VM).**
   - Export a sample of real Sage `contentEmbedding` vectors (for example 200k paragraphs from
     one large matter) plus logged query embeddings, or re-embed a few hundred logged query
     strings.
   - Run `repr_recall_sim.py --truncate 768,512,384,256 --shortlist 10,40,100,200`.
   - Pass: `trunc384` at or above 0.99 at R≤100. Otherwise Matryoshka is dropped.
   - Needs a data-access decision (tenant data). Owner: Sage team.
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
   - Success bar: 4× smaller search representation, recall@10 within 0.01 of the f32
     baseline, p50 no worse.
5. **Rescore-prefix variant (with #381).** Store a 768-dim prefix in `doc_vectors.bin` and
   rescore with it. Measure recall@10 against full-dim ground truth and the rescore bytes per
   query.

## 7. Risks and open questions

- **Model dependence.** All of this depends on the model. A Sage model change (to
  `-3-large`, Cohere v4 or Voyage) changes the answer and needs a re-embed and a re-index.
- **Corpus mismatch.**
  - The DBpedia doc-to-doc sample is not legal paragraphs with real queries.
  - The bench corpus cannot test Matryoshka at all.
  - Decisions must wait for experiment step 1.
- **Silent recall loss.** A wrong `prefix_dims` on a non-MRL model gives terrible recall with
  no error (section 3b). Mitigations:
  - an optional ingest-time self-check: sample cosine(prefix, full) ranking agreement on the
    first segment build and warn;
  - the per-PR recall suite (#258).
- **`doc_vectors.bin` availability.** Search-representation fields are useless without the
  rescore vectors. The same rollout dependency #381 has.
