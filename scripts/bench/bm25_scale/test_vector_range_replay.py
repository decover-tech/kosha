import json
import struct
import tempfile
import unittest
from pathlib import Path

from vector_range_replay import coalesce, load_trace, replay


class VectorRangeReplayTests(unittest.TestCase):
    def test_coalescing_keeps_a_posting_whole_even_when_it_exceeds_cap(self):
        spans = [(100, 110), (20, 100), (5, 15), (0, 5)]
        self.assertEqual(coalesce(spans, gap=5, cap=20), [(0, 15), (20, 100), (100, 110)])

    def test_trace_repack_preserves_posting_identity_and_object_boundaries(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            header = struct.pack("<4sIII", b"KVOF", 5, 1, 3)
            records = b"".join(struct.pack("<fQIf", *record) for record in [
                (0.0, 16, 13, 0.0), (2.0, 29, 39, 0.0), (1.0, 68, 26, 0.0),
            ])
            (root / "vector.offsets").write_bytes(header + records)
            manifest = root / "trace.json"
            manifest.write_text(json.dumps({
                "segments": {"a": "vector.offsets", "b": "vector.offsets"},
                "queries": [{"a": [2, 0, 2], "b": [0]}],
            }))
            layouts, queries = load_trace(manifest)
            self.assertEqual(list(replay(layouts, queries, proximity=False, gap=0, cap=100)),
                             [(3, 52, 52)])
            self.assertEqual(list(replay(layouts, queries, proximity=True, gap=0, cap=100)),
                             [(2, 52, 52)])
            self.assertEqual(list(replay(layouts, queries, proximity=False, gap=39, cap=100)),
                             [(2, 91, 52)])


if __name__ == "__main__":
    unittest.main()
