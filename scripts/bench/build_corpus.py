#!/usr/bin/env python3
"""Build a page-level JSONL corpus from extracted PDF/OCR text files.

Reads every .txt file in --text-dir (one file per source PDF, pages
separated by form-feed \\f — both `pdftotext` and our tesseract OCR step
emit that separator), and writes one JSON object per non-trivial page to
--out. Pages that are pure e-filing header/footer boilerplate (no real
content — common on scanned exhibit pages before OCR, or blank pages) are
dropped so they don't pollute the corpus with empty documents.

Usage:
    python3 scripts/bench/build_corpus.py \\
        --text-dir /path/to/pdftext --out corpus.jsonl
"""

import argparse
import json
import re
from pathlib import Path

BOILERPLATE_RE = re.compile(
    r"^\s*(Case\s+[\d:cv\-]+\s+Document.*PageID.*)?\s*$", re.IGNORECASE
)
MIN_ALPHA_CHARS = 40  # a page must have at least this many letters to count as real content


def doc_type_for(filename: str) -> str:
    lower = filename.lower()
    if "affidavit" in lower:
        return "affidavit"
    if "deposition" in lower:
        return "deposition"
    if "expert report" in lower or "rule 26" in lower:
        return "expert_report"
    return "other"


def is_real_content(page_text: str) -> bool:
    stripped = BOILERPLATE_RE.sub("", page_text).strip()
    alpha_count = sum(1 for c in stripped if c.isalpha())
    return alpha_count >= MIN_ALPHA_CHARS


def build(text_dir: Path, out_path: Path) -> None:
    docs = []
    dropped = 0
    for txt_file in sorted(text_dir.glob("*.txt")):
        source = txt_file.stem
        raw = txt_file.read_text(errors="replace")
        pages = raw.split("\f")
        for page_num, page_text in enumerate(pages, start=1):
            if not is_real_content(page_text):
                dropped += 1
                continue
            normalized = " ".join(page_text.split())
            docs.append(
                {
                    "id": f"{source}::p{page_num}".replace(" ", "_"),
                    "source_file": source,
                    "doc_type": doc_type_for(source),
                    "page": page_num,
                    "text": normalized,
                }
            )

    with out_path.open("w") as f:
        for doc in docs:
            f.write(json.dumps(doc) + "\n")

    total_chars = sum(len(d["text"]) for d in docs)
    print(f"wrote {len(docs)} page-documents ({total_chars:,} chars) to {out_path}")
    print(f"dropped {dropped} boilerplate/empty pages")
    by_type: dict[str, int] = {}
    for d in docs:
        by_type[d["doc_type"]] = by_type.get(d["doc_type"], 0) + 1
    print("by doc_type:", by_type)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--text-dir", required=True, type=Path)
    parser.add_argument("--out", required=True, type=Path)
    args = parser.parse_args()
    build(args.text_dir, args.out)


if __name__ == "__main__":
    main()
