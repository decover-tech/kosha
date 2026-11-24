"""Replay fixed kNN posting selections against alternative physical layouts (#460)."""

import argparse
import json
import random
import statistics
import struct
from pathlib import Path


MIB = 1024 * 1024
CURRENT_GAP = 64 * 1024
CURRENT_CAP = 16 * MIB
# 30 ms request cost / (20 ms per MiB), from the issue's illustrative model.
MODEL_GAP = 3 * MIB // 2
CAPS = (MIB // 2, MIB, 2 * MIB)


def read_offsets(path):
    data = path.read_bytes()
    if len(data) < 16:
        raise ValueError(f"{path}: truncated sidecar")
    magic, version, dim, count = struct.unpack_from("<4sIII", data)
    # Radius: none before v3, max from v3, max/p50/p90 in the v6-v8 quantile layouts.
    radius_bytes = 12 if version >= 6 else 4 if version >= 3 else 0
    record_bytes = 4 * dim + 12 + radius_bytes
    if (magic != b"KVOF" or not 1 <= version <= 8 or dim == 0
            or len(data) != 16 + count * record_bytes):
        raise ValueError(f"{path}: invalid vector.offsets header or length")
    centroids, spans = [], []
    for pos in range(16, len(data), record_bytes):
        centroids.append(struct.unpack_from(f"<{dim}f", data, pos))
        offset, length = struct.unpack_from("<QI", data, pos + 4 * dim)
        spans.append((offset, offset + length))
    return centroids, spans


def projection_order(centroids):
    if not centroids:
        return []
    anchor = centroids[0]
    far = max(centroids, key=lambda c: sum((x - y) ** 2 for x, y in zip(c, anchor)))
    axis = [x - y for x, y in zip(far, anchor)]
    return sorted(range(len(centroids)),
                  key=lambda i: sum(x * y for x, y in zip(centroids[i], axis)))


def repack(spans, order):
    out = [None] * len(spans)
    offset = 16  # vector.idx header
    for i in order:
        start, end = spans[i]
        length = end - start
        out[i] = (offset, offset + length)
        offset += length
    return out


def coalesce(spans, gap, cap):
    out = []
    for start, end in sorted(spans):
        if end <= start:
            continue
        if out and start - out[-1][1] <= gap and max(end, out[-1][1]) - out[-1][0] <= cap:
            out[-1] = (out[-1][0], max(end, out[-1][1]))
        else:
            out.append((start, end))
    return out


def replay(layouts, queries, *, proximity, gap, cap):
    for query in queries:
        gets = transferred = requested = 0
        for key, ordinals in query.items():
            spans = layouts[key][proximity]
            wanted = []
            for i in set(ordinals):
                if not 0 <= i < len(spans):
                    raise ValueError(f"{key}: posting ordinal out of bounds: {i}")
                wanted.append(spans[i])
            ranges = coalesce(wanted, gap, cap)
            requested += sum(e - s for s, e in wanted)
            gets += len(ranges)
            transferred += sum(e - s for s, e in ranges)
        yield gets, transferred, requested


def load_trace(path):
    trace = json.loads(path.read_text())
    layouts = {}
    for key, sidecar in trace["segments"].items():
        centroids, spans = read_offsets(path.parent / sidecar)
        layouts[key] = (spans, repack(spans, projection_order(centroids)))
    return layouts, trace["queries"]


def synthetic(localized):
    rng = random.Random(460)
    layouts = {}
    by_rank = {}
    posting_count, objects = 565, 200
    for key in range(objects):
        ranks = rng.sample(range(posting_count), posting_count)
        centroids = [(rank,) for rank in ranks]
        size = 88 * 140  # RaBitQ at 1024 dimensions; 9.944M vectors total
        spans = [(16 + i * size, 16 + (i + 1) * size) for i in range(posting_count)]
        layouts[key] = (spans, repack(spans, projection_order(centroids)))
        by_rank[key] = sorted(range(posting_count), key=ranks.__getitem__)
    queries = []
    for _ in range(20):
        query = {}
        for key in layouts:
            count = 28 + (key < 100)  # 5,700 wanted postings/query
            if localized:
                start = rng.randrange(posting_count - count + 1)
                query[key] = by_rank[key][start:start + count]
            else:
                query[key] = rng.sample(range(posting_count), count)
        queries.append(query)
    return layouts, queries


def report(layouts, queries):
    policies = [("current", False, CURRENT_GAP, CURRENT_CAP),
                ("proximity only", True, CURRENT_GAP, CURRENT_CAP)]
    for cap in CAPS:
        policies.extend((f"{order} + bridge, {cap / MIB:g} MiB", proximity, MODEL_GAP, cap)
                        for order, proximity in (("current", False), ("proximity", True)))
    print(f"{'Policy':40} {'GETs/query':>10} {'MiB/query':>10} {'Gap MiB/query':>14}")
    for label, proximity, gap, cap in policies:
        gets, transferred, requested = zip(
            *replay(layouts, queries, proximity=proximity, gap=gap, cap=cap)
        )
        print(f"{label:40} {statistics.mean(gets):10.1f} "
              f"{statistics.mean(transferred) / MIB:10.1f} "
              f"{(statistics.mean(transferred) - statistics.mean(requested)) / MIB:14.1f}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trace", type=Path, help="JSON trace manifest; otherwise run synthetic cases")
    args = parser.parse_args()
    if args.trace:
        layouts, queries = load_trace(args.trace)
        if not queries:
            parser.error("trace must contain at least one query")
        report(layouts, queries)
        return
    for localized in (True, False):
        print(f"Synthetic {'localized' if localized else 'uniform'} probes")
        report(*synthetic(localized))


if __name__ == "__main__":
    main()
