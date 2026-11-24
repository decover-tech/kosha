# Posting rewrite baseline (#463)

Set `KOSHA_VECTOR_COMPACTION_PROFILE=1` on the process doing the merge.
Successful local merges in `lire` mode log `vector_compaction_profile` followed
by one JSON record per clustered vector field. Profiling is off by default
and does not change the files written or the query path.

- Divide `membership_unchanged_postings` by `output_postings` for the
  output posting fraction. A match requires an identical set of surviving
  members after doc-seq remapping, regardless of order. A posting that lost
  any member cannot match, even if its survivors stay together.
- `input_postings_examined` counts the postings read by the existing seeding
  path. Flat, unreadable and small input columns are not examined; a record
  with `lire: false` means the field fell back to rebuilding.
- `index_bytes_written` is the actual size of the output index and offsets
  files, not network traffic. `membership_changed_bytes_estimate` subtracts
  the encoded entry bytes of all membership matches. The estimate keeps
  headers and offsets, excludes `doc_vectors.bin`, and does not include a
  future remap table. It does not verify byte compatibility across codecs or
  centroid changes (RaBitQ codes depend on centroids).

## Local baseline

Default f32 format, synthetic vectors, 8-vCPU Intel Xeon Linux VM. No S3
traffic or compaction latency was measured.

| Corpus and upserts | Unchanged postings | Index bytes written | Changed bytes estimate | Written / estimated changed |
| --- | ---: | ---: | ---: | ---: |
| 2,000 × 16d; 4 updates (0.2%) | 13 / 17 | 137,392 | 30,632 | 4.49× |
| 6,000 × 32d; 300 updates (5%), 3 seeds | 0 / 79–80 | 803,408–803,552 | 803,408–803,552 | 1.00× |

```sh
KOSHA_VECTOR_COMPACTION_PROFILE=1 cargo test --locked -p kosha-write --lib \
  lire_compaction_reuses_input_postings_and_remaps_doc_seqs -- --nocapture
KOSHA_VECTOR_COMPACTION_PROFILE=1 cargo test --locked -p kosha-query \
  --test lire_compaction_recall -- --nocapture
```

The two fixtures differ in more than churn; this is not a controlled churn
sweep. Before prototyping, collect byte-weighted totals across representative
merge groups and churn levels on the [10M MSMarco baseline][baseline]. If
matches account for enough bytes to make the 2× target plausible, test an
offline remap-table builder and compare merge time and actual PUT bytes against
the same inputs. The target is new vector-index bytes written at most twice
the changed bytes, including the cost of the remap and posting metadata in
the new write. Report storage retained by references and query recall/latency
separately; the local measurements do not establish any of these outcomes.

[baseline]: https://github.com/decover-tech/kosha/issues/384
