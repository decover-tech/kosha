#!/usr/bin/env python3
"""Doc-store byte accounting for one snapshot (research #388).

Deterministic evidence for the compressed doc store experiment: object
sizes from an `aws s3 ls --recursive` listing of a snapshot's segments,
plus each segment's footer.json. No timing, no noise.

Reports (one JSON object on stdout):
  - segments, docs, and how many segments are compressed (footer
    `doc_store_compression.codec == "zstd-dict"`), raw, or name another
    codec;
  - summed bytes of doc_store.bin, doc_store.offsets, doc_vectors.bin and
    every object in the snapshot;
  - doc_store.bin bytes split into stored records, the 4-byte header and
    the dictionary tail copy a compressed file ends with (`dict_bytes + 8`
    per compressed segment), so the record ratio between two snapshots is
    exact;
  - mean stored record bytes and the doc_store.bin bytes a top-10 page of
    ranged reads would fetch on average (10 x the mean). That is derived
    from sizes, not measured on the server;
  - build_info.txt of the snapshot, when build_and_import.sh wrote one.

--expect zstd|raw exits 3 when the footers disagree: zstd needs every
non-empty segment compressed with zstd-dict, raw needs none compressed.
A snapshot named *-zstd that was built by a branch without #388 is raw,
and this is what catches it before any number from it is trusted.

Usage (vm/doc_store_bytes.sh does the S3 part):
  doc_store_bytes.py --listing listing.txt --footers DIR [--expect zstd]
                     [--build-info build_info.txt] [--label NAME]
"""

import argparse
import json
import os
import sys

ZSTD_DICT = "zstd-dict"
TRACKED = ("doc_store.bin", "doc_store.offsets", "doc_vectors.bin")


def parse_listing(path, marker="/segments/"):
    """{segment_rel_dir: {file_name: size}} from `aws s3 ls --recursive`."""
    segments = {}
    total = 0
    with open(path) as f:
        for line in f:
            parts = line.split(None, 3)
            if len(parts) != 4 or not parts[2].isdigit():
                continue
            size, key = int(parts[2]), parts[3].strip()
            total += size
            at = key.find(marker)
            rel = key[at + len(marker):] if at >= 0 else key
            seg, _, name = rel.rpartition("/")
            segments.setdefault(seg, {})[name] = size
    return segments, total


def load_footers(root):
    """{segment_rel_dir: footer dict} for every */footer.json under root."""
    footers = {}
    for dirpath, _dirs, files in os.walk(root):
        if "footer.json" in files:
            rel = os.path.relpath(dirpath, root).replace(os.sep, "/")
            with open(os.path.join(dirpath, "footer.json")) as f:
                footers[rel] = json.load(f)
    return footers


def account(segments, total, footers):
    out = {
        "segments": 0,
        "segments_without_footer": 0,
        "docs": 0,
        "compressed_segments": 0,
        "raw_segments": 0,
        "empty_segments": 0,
        "other_codec_segments": 0,
        "bytes": {name: 0 for name in TRACKED},
        "bytes_all_objects": total,
        "doc_store_dict_tail_bytes": 0,
    }
    for seg, files in sorted(segments.items()):
        if "footer.json" not in files:
            continue
        out["segments"] += 1
        for name in TRACKED:
            out["bytes"][name] += files.get(name, 0)
        footer = footers.get(seg)
        if footer is None:
            out["segments_without_footer"] += 1
            continue
        docs = int(footer.get("doc_count", 0))
        out["docs"] += docs
        comp = footer.get("doc_store_compression")
        if comp is None:
            if docs == 0:
                out["empty_segments"] += 1
            else:
                out["raw_segments"] += 1
        elif comp.get("codec") == ZSTD_DICT:
            out["compressed_segments"] += 1
            out["doc_store_dict_tail_bytes"] += int(comp.get("dict_bytes", 0)) + 8
        else:
            out["other_codec_segments"] += 1
    # Each doc_store.bin starts with a 4-byte doc count.
    records = out["bytes"]["doc_store.bin"] - out["doc_store_dict_tail_bytes"] - 4 * out["segments"]
    out["doc_store_record_bytes"] = records
    if out["docs"]:
        mean = records / out["docs"]
        out["mean_stored_record_bytes"] = round(mean, 1)
        out["derived_top10_page_doc_store_bytes"] = round(10 * mean)
    return out


def check(out, expect):
    """Problems with `out` against the expected format (empty: fine)."""
    problems = []
    if out["segments"] == 0:
        problems.append("no segments found")
    if out["segments_without_footer"]:
        problems.append(f"{out['segments_without_footer']} segment(s) without a downloaded footer")
    if out["other_codec_segments"]:
        problems.append(f"{out['other_codec_segments']} segment(s) name an unknown codec")
    if expect == "zstd":
        if out["raw_segments"]:
            problems.append(f"{out['raw_segments']} non-empty segment(s) are raw, expected zstd-dict")
        if out["compressed_segments"] == 0:
            problems.append("no compressed segment: the builder never saw KOSHA_DOC_STORE_COMPRESSION=zstd")
    elif expect == "raw" and out["compressed_segments"]:
        problems.append(f"{out['compressed_segments']} segment(s) are compressed, expected raw")
    return problems


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--listing", required=True)
    ap.add_argument("--footers", required=True)
    ap.add_argument("--expect", choices=["zstd", "raw"])
    ap.add_argument("--build-info")
    ap.add_argument("--label", default="")
    args = ap.parse_args(argv)

    segments, total = parse_listing(args.listing)
    out = {"snapshot": args.label}
    out.update(account(segments, total, load_footers(args.footers)))
    if args.build_info and os.path.isfile(args.build_info):
        with open(args.build_info) as f:
            out["build_info"] = f.read().strip()
    else:
        out["build_info"] = None
    problems = check(out, args.expect) if args.expect else []
    out["expect"] = args.expect
    out["problems"] = problems
    print(json.dumps(out, indent=2, sort_keys=True))
    if problems:
        for p in problems:
            print(f"DOC-STORE-CHECK-FAILED: {p}", file=sys.stderr)
        return 3
    return 0


if __name__ == "__main__":
    sys.exit(main())
