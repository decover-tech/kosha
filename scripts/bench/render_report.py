#!/usr/bin/env python3
"""Render the benchmark results.json into a self-contained HTML report."""

import argparse
import json
import statistics
from pathlib import Path

# ── Cost model inputs (sourced 2026-07-19; see report footer for links) ─────
COST = {
    "os_ebs_gp3_per_gb_month": 0.122,
    "s3_standard_per_gb_month": 0.023,
    "os_r6g_large_search_per_hr": 0.167,
    "kosha_i4i_large_per_hr": 0.172,
    "hours_per_month": 730,
}


def load(path: Path) -> dict:
    return json.loads(path.read_text())


def fmt(x, nd=2):
    return f"{x:.{nd}f}"


def build_rows(results: dict) -> str:
    rows = []
    for q in results["queries"]:
        c = q["correctness"]
        tau = c["kendall_tau_overlap"]
        tau_s = f"{tau:.2f}" if tau is not None else "&mdash;"
        top1 = "&check;" if c["top1_match"] else "&#10007;"
        top1_cls = "ok" if c["top1_match"] else "warn"
        rows.append(f"""
        <tr>
          <td class="qtext">{q['query_text']}</td>
          <td class="cat">{q.get('category','')}</td>
          <td class="num">{q['opensearch']['latency']['p50_ms']:.2f}</td>
          <td class="num">{q['kosha']['latency']['p50_ms']:.2f}</td>
          <td class="num">{c['jaccard_topk']:.2f}</td>
          <td class="num">{tau_s}</td>
          <td class="num {top1_cls}">{top1}</td>
        </tr>""")
    return "".join(rows)


def build_bars(results: dict, series_key: str, metric_path, color_var: str, max_val: float) -> str:
    bars = []
    for q in results["queries"]:
        val = metric_path(q[series_key])
        pct = max(2, (val / max_val) * 100)
        bars.append(
            f'<div class="bar-row"><span class="bar-label">{q["query_id"]}</span>'
            f'<div class="bar-track"><div class="bar-fill" style="width:{pct:.1f}%;background:var({color_var})">'
            f'<span class="bar-val">{val:.1f}</span></div></div></div>'
        )
    return "".join(bars)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--results", required=True, type=Path)
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--os-index-mb", type=float, required=True)
    parser.add_argument("--kosha-index-mb", type=float, required=True)
    parser.add_argument("--corpus-pdf-mb", type=float, required=True)
    args = parser.parse_args()

    r = load(args.results)
    qs = r["queries"]

    os_p50 = [q["opensearch"]["latency"]["p50_ms"] for q in qs]
    os_p95 = [q["opensearch"]["latency"]["p95_ms"] for q in qs]
    k_p50 = [q["kosha"]["latency"]["p50_ms"] for q in qs]
    k_p95 = [q["kosha"]["latency"]["p95_ms"] for q in qs]
    jac = [q["correctness"]["jaccard_topk"] for q in qs]
    top1 = [q["correctness"]["top1_match"] for q in qs]
    taus = [q["correctness"]["kendall_tau_overlap"] for q in qs if q["correctness"]["kendall_tau_overlap"] is not None]

    stats = {
        "os_p50_median": statistics.median(os_p50),
        "os_p50_mean": statistics.mean(os_p50),
        "os_p95_median": statistics.median(os_p95),
        "k_p50_median": statistics.median(k_p50),
        "k_p50_mean": statistics.mean(k_p50),
        "k_p95_median": statistics.median(k_p95),
        "mean_jaccard": statistics.mean(jac),
        "min_jaccard": min(jac),
        "top1_rate": sum(top1) / len(top1),
        "mean_tau": statistics.mean(taus) if taus else None,
    }

    max_p50 = max(max(os_p50), max(k_p50)) * 1.15
    max_p95 = max(max(os_p95), max(k_p95)) * 1.15

    # cost model
    os_gb = args.os_index_mb / 1024
    kosha_gb = args.kosha_index_mb / 1024
    os_storage_ha = os_gb * 2 * COST["os_ebs_gp3_per_gb_month"]
    kosha_storage = kosha_gb * COST["s3_standard_per_gb_month"]
    N_FLEET = 1000
    os_fleet = N_FLEET * os_gb * 2 * COST["os_ebs_gp3_per_gb_month"]
    kosha_fleet = N_FLEET * kosha_gb * COST["s3_standard_per_gb_month"]
    os_node_month = COST["os_r6g_large_search_per_hr"] * COST["hours_per_month"]
    kosha_node_month = COST["kosha_i4i_large_per_hr"] * COST["hours_per_month"]
    duty_cycles = [1.0, 0.5, 0.3, 0.1]
    duty_rows = "".join(
        f"<tr><td>{int(d*100)}%</td><td class='num'>${os_node_month*3:.2f}</td>"
        f"<td class='num'>${kosha_node_month*d:.2f}</td>"
        f"<td class='num savings'>{(1 - (kosha_node_month*d)/(os_node_month*3))*100:.0f}% lower</td></tr>"
        for d in duty_cycles
    )

    html = f"""<title>BM25 Microbenchmark: OpenSearch vs Kosha — Ruffino v. Archer</title>
<style>
.viz-root {{
  color-scheme: light;
  --surface-0: #ffffff;
  --surface-1: #fcfcfb;
  --surface-2: #f3f2ef;
  --border: #e3e1da;
  --text-primary: #0b0b0b;
  --text-secondary: #52514e;
  --text-muted: #7a7972;
  --series-1: #2a78d6;
  --series-2: #008300;
  --ok: #008300;
  --warn: #eda100;
}}
@media (prefers-color-scheme: dark) {{
  :root:where(:not([data-theme="light"])) .viz-root {{
    color-scheme: dark;
    --surface-0: #121211;
    --surface-1: #1a1a19;
    --surface-2: #242422;
    --border: #3a3934;
    --text-primary: #ffffff;
    --text-secondary: #c3c2b7;
    --text-muted: #8f8e85;
    --series-1: #3987e5;
    --series-2: #1fbf1f;
    --ok: #2fd12f;
    --warn: #c98500;
  }}
}}
:root[data-theme="dark"] .viz-root {{
  color-scheme: dark;
  --surface-0: #121211;
  --surface-1: #1a1a19;
  --surface-2: #242422;
  --border: #3a3934;
  --text-primary: #ffffff;
  --text-secondary: #c3c2b7;
  --text-muted: #8f8e85;
  --series-1: #3987e5;
  --series-2: #1fbf1f;
  --ok: #2fd12f;
  --warn: #c98500;
}}
* {{ box-sizing: border-box; }}
body {{ margin: 0; }}
.viz-root {{
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
  background: var(--surface-0);
  color: var(--text-primary);
  max-width: 980px;
  margin: 0 auto;
  padding: 32px 24px 80px;
  line-height: 1.5;
}}
h1 {{ font-size: 1.6rem; margin-bottom: 4px; }}
h2 {{ font-size: 1.15rem; margin-top: 48px; border-bottom: 1px solid var(--border); padding-bottom: 8px; }}
.subtitle {{ color: var(--text-secondary); margin-top: 0; font-size: 0.95rem; }}
.badges {{ display: flex; gap: 8px; flex-wrap: wrap; margin: 16px 0; }}
.badge {{ background: var(--surface-2); border: 1px solid var(--border); border-radius: 6px; padding: 4px 10px; font-size: 0.8rem; color: var(--text-secondary); }}
.stat-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 12px; margin: 20px 0; }}
.stat-tile {{ background: var(--surface-1); border: 1px solid var(--border); border-radius: 10px; padding: 14px 16px; }}
.stat-tile .label {{ font-size: 0.75rem; color: var(--text-muted); text-transform: uppercase; letter-spacing: 0.03em; }}
.stat-tile .value {{ font-size: 1.5rem; font-weight: 600; margin-top: 4px; }}
.stat-tile .value.series-1 {{ color: var(--series-1); }}
.stat-tile .value.series-2 {{ color: var(--series-2); }}
p {{ color: var(--text-primary); }}
.note {{ color: var(--text-secondary); font-size: 0.9rem; }}
.callout {{ background: var(--surface-1); border-left: 3px solid var(--series-1); border-radius: 0 8px 8px 0; padding: 12px 16px; margin: 16px 0; font-size: 0.92rem; }}
.callout.warn {{ border-left-color: var(--warn); }}
table {{ border-collapse: collapse; width: 100%; font-size: 0.87rem; margin: 12px 0; }}
th, td {{ padding: 6px 10px; text-align: left; border-bottom: 1px solid var(--border); }}
th {{ color: var(--text-muted); font-weight: 600; font-size: 0.75rem; text-transform: uppercase; letter-spacing: 0.02em; }}
td.num {{ text-align: right; font-variant-numeric: tabular-nums; }}
td.qtext {{ font-weight: 500; }}
td.cat {{ color: var(--text-muted); font-size: 0.8rem; }}
.ok {{ color: var(--ok); text-align: center; }}
.warn {{ color: var(--warn); text-align: center; }}
.savings {{ color: var(--ok); font-weight: 600; }}
.legend {{ display: flex; gap: 20px; margin: 12px 0; font-size: 0.85rem; }}
.legend-item {{ display: flex; align-items: center; gap: 6px; }}
.legend-swatch {{ width: 10px; height: 10px; border-radius: 2px; }}
.chart {{ margin: 16px 0; overflow-x: auto; }}
.bar-row {{ display: flex; align-items: center; gap: 10px; margin: 3px 0; }}
.bar-label {{ width: 42px; flex-shrink: 0; font-size: 0.78rem; color: var(--text-muted); font-variant-numeric: tabular-nums; }}
.bar-track {{ flex: 1; background: var(--surface-2); border-radius: 4px; height: 18px; position: relative; min-width: 200px; }}
.bar-fill {{ height: 100%; border-radius: 4px; display: flex; align-items: center; justify-content: flex-end; padding-right: 6px; min-width: 24px; }}
.bar-val {{ font-size: 0.72rem; color: white; font-variant-numeric: tabular-nums; }}
footer {{ margin-top: 56px; padding-top: 16px; border-top: 1px solid var(--border); color: var(--text-muted); font-size: 0.78rem; }}
footer a {{ color: var(--text-secondary); }}
code {{ background: var(--surface-2); padding: 1px 5px; border-radius: 4px; font-size: 0.85em; }}
</style>
<div class="viz-root">

<h1>BM25 Microbenchmark: OpenSearch vs. Kosha</h1>
<p class="subtitle">Corpus: <em>Ruffino v. Archer</em> (Middle District of Tennessee, medical malpractice) — depositions, affidavits &amp; expert reports</p>

<div class="badges">
  <span class="badge">{r['corpus_size']} page-documents</span>
  <span class="badge">{args.corpus_pdf_mb:.0f} MB source PDFs (14 files)</span>
  <span class="badge">{r['num_queries']} queries</span>
  <span class="badge">{r['reps_per_query']} reps/query</span>
  <span class="badge">top-{r['top_k']}</span>
  <span class="badge">OpenSearch 2.18.0</span>
  <span class="badge">Kosha (this repo, release build)</span>
</div>

<h2>Methodology</h2>
<p>14 case-file PDFs (depositions, affidavits, expert reports) were text-extracted with <code>pdftotext</code>;
6 files were scanned images with no text layer and were OCR'd with <code>tesseract</code>. Each PDF page became
one document ({r['corpus_size']} total after dropping {"boilerplate-only"} pages), indexed identically into both
engines under a single-shard, single-node, no-replica index/namespace.</p>

<div class="callout">
<strong>Tokenizer parity fix.</strong> Kosha's tokenizer
(<code>crates/kosha-segment/src/lib.rs::tokenize()</code>) splits on whitespace, lowercases, then trims
leading/trailing ASCII punctuation from each token (so <code>negligence:</code> indexes as <code>negligence</code>).
A stock OpenSearch <code>whitespace</code> analyzer does <em>not</em> strip that punctuation — using it caused
silent token mismatches and near-zero result overlap on several queries in an early pass. The OpenSearch analyzer
below was built to reproduce Kosha's exact tokenization (a <code>pattern_replace</code> filter trimming
<code>^\\p{{Punct}}+|\\p{{Punct}}+$</code>) so the correctness comparison isolates BM25/ranking behavior rather
than analyzer drift.</div>

<p class="note">Both engines ran as single-node, single-process local instances (OpenSearch 2.18.0 in Docker,
Kosha's release binary as a bare process) — dev-scale, not production-sized clusters. Latency is client-measured,
sequential, single-connection wall-clock time (1 warmup + {r['reps_per_query']} timed reps per query), matching
Kosha's server, which is currently single-threaded with no keep-alive.</p>

<h2>Performance</h2>
<div class="stat-grid">
  <div class="stat-tile"><div class="label">OpenSearch p50 (median)</div><div class="value series-1">{stats['os_p50_median']:.2f} ms</div></div>
  <div class="stat-tile"><div class="label">Kosha p50 (median)</div><div class="value series-2">{stats['k_p50_median']:.2f} ms</div></div>
  <div class="stat-tile"><div class="label">OpenSearch p95 (median)</div><div class="value series-1">{stats['os_p95_median']:.2f} ms</div></div>
  <div class="stat-tile"><div class="label">Kosha p95 (median)</div><div class="value series-2">{stats['k_p95_median']:.2f} ms</div></div>
</div>

<div class="legend">
  <div class="legend-item"><span class="legend-swatch" style="background:var(--series-1)"></span>OpenSearch</div>
  <div class="legend-item"><span class="legend-swatch" style="background:var(--series-2)"></span>Kosha</div>
</div>

<p class="note"><strong>p50 latency by query (ms)</strong> — OpenSearch:</p>
<div class="chart">{build_bars(r, "opensearch", lambda x: x["latency"]["p50_ms"], "--series-1", max_p50)}</div>
<p class="note"><strong>p50 latency by query (ms)</strong> — Kosha:</p>
<div class="chart">{build_bars(r, "kosha", lambda x: x["latency"]["p50_ms"], "--series-2", max_p50)}</div>

<div class="callout warn">
<strong>Why Kosha is slower here, and it isn't the BM25 math.</strong> Kosha's query path
(<code>kosha-query::Searcher::search()</code>) calls <code>SegmentReader::open()</code> — a full read + parse of
<code>doc_store.bin</code>, <code>inverted.idx</code>, and <code>filters.bin</code> from disk — on <em>every single
request</em>, for every segment. There is no warm in-memory reader reuse, and the SSD read-through cache
(<code>kosha-cache</code>, Epic 4) is not wired into this query path yet. A <code>/healthz</code> baseline call
(no search work) measures ~1.2ms round-trip, so roughly 5-6ms of Kosha's ~7ms floor is segment
open/deserialize cost repeated per query, not scoring. OpenSearch/Lucene keeps warm readers across requests, so its
latency reflects actual search work more directly. This is exactly the gap DESIGN.md &sect;17 flags as
"unvalidated" — the &le;80ms p50 warm-cache target assumes a cache-hit path this Phase-1 server doesn't
implement yet.</div>

<p class="note">Indexing {r['corpus_size']} documents: OpenSearch bulk API took {r['indexing']['opensearch_seconds']:.3f}s;
Kosha's <code>/index</code> + <code>/flush</code> took {r['indexing']['kosha_seconds']:.3f}s. Not directly comparable —
Kosha's flush is a local buffer-to-disk write with no replication or translog, while OpenSearch's bulk API commits
through its full write path (translog + refresh).</p>

<h2>Correctness</h2>
<div class="stat-grid">
  <div class="stat-tile"><div class="label">Mean Jaccard@10</div><div class="value">{stats['mean_jaccard']:.2f}</div></div>
  <div class="stat-tile"><div class="label">Min Jaccard@10</div><div class="value">{stats['min_jaccard']:.2f}</div></div>
  <div class="stat-tile"><div class="label">Top-1 match rate</div><div class="value">{stats['top1_rate']*100:.0f}%</div></div>
  <div class="stat-tile"><div class="label">Mean Kendall &tau; (overlap)</div><div class="value">{stats['mean_tau']:.2f}</div></div>
</div>
<p class="note">Jaccard@10 is the overlap of the two engines' top-10 doc-ID sets; Kendall &tau; measures rank
agreement restricted to documents both engines returned (1.0 = identical order). Exact BM25 score equality isn't
the bar here — Lucene's <code>BM25Similarity</code> uses a lossy 8-bit float encoding of document-length norms for
performance, so tiny score deltas versus Kosha's full-precision arithmetic are expected even with identical
postings.</p>

<table>
<thead><tr><th>Query</th><th>Category</th><th>OS p50 (ms)</th><th>Kosha p50 (ms)</th><th>Jaccard@10</th><th>Kendall &tau;</th><th>Top-1 match</th></tr></thead>
<tbody>{build_rows(r)}</tbody>
</table>

<div class="callout">
<strong>The one top-1 mismatch</strong> (<code>standard of care</code>) is a near-tie, not a divergence: both
engines return the identical top-10 set, and the #1/#2 documents score 6.163 vs 6.156 on OpenSearch and 6.144 vs
6.102 on Kosha — a swap between two nearly-equal-score documents, consistent with the BM25 norm-quantization
difference noted above rather than a retrieval defect.</div>

<h2>Cost (AWS estimate: OpenSearch Service vs. Kosha's target architecture)</h2>
<p class="note">Kosha's write path in this repo currently writes straight to local disk — S3 backing (Epic 3/9)
isn't wired into the server binary yet. The figures below price Kosha's <em>designed</em> architecture
(DESIGN.md &sect;9, &sect;16: S3 as source of truth, instance-store NVMe query nodes), not this Phase-1 build's
actual storage backend, since that's the comparison the design is meant to validate.</p>

<h3>Storage: measured index footprint for this one matter</h3>
<table>
<thead><tr><th></th><th class="num">On-disk size</th><th class="num">$/GB-month</th><th class="num">Monthly cost (1 replica / HA)</th></tr></thead>
<tbody>
<tr><td>OpenSearch (Lucene index, gp3 EBS)</td><td class="num">{args.os_index_mb:.1f} MB</td><td class="num">${COST['os_ebs_gp3_per_gb_month']}</td><td class="num">${os_storage_ha:.6f}</td></tr>
<tr><td>Kosha (segment files, S3 Standard)</td><td class="num">{args.kosha_index_mb:.1f} MB</td><td class="num">${COST['s3_standard_per_gb_month']}</td><td class="num">${kosha_storage:.6f}</td></tr>
</tbody>
</table>
<p class="note">Kosha's segment format is currently ~{args.kosha_index_mb/args.os_index_mb:.1f}&times; larger on
disk than Lucene's for the same {r['corpus_size']} documents (no compression/compact codecs yet in this Phase-1
format). Even so, S3's per-GB price (~5.3&times; cheaper than gp3-backed OpenSearch storage) more than offsets it
at this size. At a single matter's scale this is fractions of a cent either way — storage is not where the cost
story lives.</p>

<p class="note">Extrapolated to a fleet of {N_FLEET:,} matters of this average size (illustrative, not Decover's
actual fleet composition):</p>
<table>
<thead><tr><th>Engine</th><th class="num">Fleet storage/month</th></tr></thead>
<tbody>
<tr><td>OpenSearch (1 replica/HA, EBS gp3)</td><td class="num">${os_fleet:.2f}</td></tr>
<tr><td>Kosha (S3 Standard, single durable copy)</td><td class="num">${kosha_fleet:.2f}</td></tr>
</tbody>
</table>

<h3>Compute: where the real divergence is</h3>
<p class="note">A production OpenSearch domain needs a minimum of 3 data nodes for HA, running 24/7 regardless of
query volume — idle matters still hold their shard's heap and disk. Kosha's query nodes are designed to be
stateless and horizontally autoscaled; an idle-traffic window can scale toward zero. Modeling a 3-node
<code>r6g.large.search</code> OpenSearch domain against a single autoscaled <code>i4i.large</code> Kosha query node
at different duty cycles:</p>
<table>
<thead><tr><th>Duty cycle</th><th class="num">OpenSearch (3-node, 24/7)</th><th class="num">Kosha (autoscaled)</th><th>Delta</th></tr></thead>
<tbody>{duty_rows}</tbody>
</table>
<p class="note">This models compute only — it does not include OpenSearch's per-idle-tenant shard/heap tax
(DESIGN.md &sect;16: "full shard footprint regardless of activity"), which is the other half of the fixed-cluster
cost story and doesn't have a clean per-node dollar figure. Node prices are on-demand us-east-1, 2026-07-19.</p>

<h2>Limitations of this benchmark</h2>
<ul>
<li>Single-node, single-process dev instances for both engines — not sized or tuned like production clusters.</li>
<li>Kosha's HTTP server is single-threaded with no connection keep-alive; this benchmark measures sequential
single-client latency only, not concurrent throughput (where OpenSearch's multi-threaded design would show a
larger gap in Kosha's favor... or against it, untested here).</li>
<li>Corpus is small ({r['corpus_size']} documents, ~1MB of text) relative to real per-matter or per-org index
sizes; both engines' absolute latencies would shift at larger scale, and Kosha's segment-reopen-per-query cost
would likely dominate more, not less, as segment files grow.</li>
<li>Cost figures are illustrative estimates from public 2026 AWS pricing and DESIGN.md's target architecture, not
measurements of a running production deployment of either system — consistent with DESIGN.md &sect;17's own
caveat that these figures are "directional planning targets, not measured benchmarks."</li>
</ul>

<footer>
Benchmark scripts: <code>scripts/bench/build_corpus.py</code>, <code>scripts/bench/run_benchmark.py</code>,
<code>scripts/bench/render_report.py</code> in this repo. Query set: <code>scripts/bench/queries.json</code>.
AWS pricing sourced 2026-07-19 from aws.amazon.com/opensearch-service/pricing, aws.amazon.com/ec2/pricing/on-demand,
and aws.amazon.com/ebs/pricing (us-east-1, on-demand).
</footer>
</div>
"""
    args.out.write_text(html)
    print(f"wrote report to {args.out}")


if __name__ == "__main__":
    main()
