"""``kosha-ingest``: index a folder of PDF, DOCX, HTML and text files (#587).

Each file becomes one Kosha document (or, with ``per_page``, one per page):

- ``path`` (Keyword): ``prefix`` + the file's path under the folder, e.g.
  ``/contracts/2024/acme.pdf``, so ``prefix`` / ``glob`` filters work.
- ``body`` (Text): the extracted text. PDF pages are joined by a form feed
  (``\\f``), the page separator ``read(page=n)`` (#586) cuts on; DOCX pages
  are its explicit page breaks.
- ``title``, ``mime_type``, ``size_bytes``, ``modified_at`` (Date),
  ``content_sha256``, ``page_count``, ``scanned`` and ``source``.

Re-ingesting is idempotent: ids derive from ``path`` (so a changed file
replaces its document) and a file whose ``content_sha256`` is unchanged is
skipped. Scanned PDFs (no extractable text; OCR is out of scope) are
indexed with ``scanned: true`` and an empty body so they can be found and
flagged. Needs the ``ingest`` extra: ``pip install 'kosha-client[ingest]'``.
"""

from __future__ import annotations

import argparse
import datetime as _dt
import hashlib
import json
import mimetypes
import os
import re
import sys
import zipfile
from dataclasses import dataclass, field
from html.parser import HTMLParser
from pathlib import Path, PurePosixPath
from typing import Any, Iterable, Iterator

from .native import KoshaConfigError, ns_path

PAGE_BREAK = "\f"
BATCH = 64
# Well under the server's default 64 MiB request limit (KOSHA_MAX_BODY_BYTES).
MAX_REQUEST_BYTES = 8 << 20
# A PDF page with no non-space characters has no text layer.
_MIN_PAGE_CHARS = 1

MIME_TYPES = {
    ".pdf": "application/pdf",
    ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    ".html": "text/html",
    ".htm": "text/html",
    ".txt": "text/plain",
    ".md": "text/markdown",
}


@dataclass
class Extracted:
    """Text and structure pulled out of one file."""

    pages: list[str]
    mime_type: str
    title: str = ""
    scanned: bool = False

    @property
    def text(self) -> str:
        return PAGE_BREAK.join(self.pages)


@dataclass
class IngestReport:
    indexed: int = 0
    unchanged: int = 0
    unsupported: int = 0
    scanned: list[str] = field(default_factory=list)
    failed: dict[str, str] = field(default_factory=dict)

    def as_dict(self) -> dict:
        return {
            "indexed": self.indexed,
            "unchanged": self.unchanged,
            "unsupported": self.unsupported,
            "scanned": self.scanned,
            "failed": self.failed,
        }


# ── Extraction ────────────────────────────────────────────────────────────


def extract(path: str | os.PathLike, data: bytes | None = None) -> Extracted:
    """Extract ``path`` (read from disk unless ``data`` is given). Raises
    ``KoshaConfigError`` for an unsupported extension."""
    suffix = Path(path).suffix.lower()
    mime = MIME_TYPES.get(suffix)
    if mime is None:
        raise KoshaConfigError(f"unsupported file type: {suffix or path}")
    if data is None:
        data = Path(path).read_bytes()
    if suffix == ".pdf":
        return _extract_pdf(data)
    if suffix == ".docx":
        return _extract_docx(data)
    if mime == "text/html":
        return _extract_html(_decode_text(data))
    # Plain text keeps its form feeds as page breaks (`pdftotext` output).
    return Extracted([_clean(p) for p in _decode_text(data).split(PAGE_BREAK)], mime)


def _decode_text(data: bytes) -> str:
    try:
        return data.decode("utf-8-sig")
    except UnicodeDecodeError:
        return data.decode("latin-1")


def _clean(text: str) -> str:
    # A form feed inside a page would shift every later page number.
    text = text.replace(PAGE_BREAK, "\n").replace("\r\n", "\n").replace("\r", "\n")
    return re.sub(r"\n{3,}", "\n\n", text).strip()


def _extract_pdf(data: bytes) -> Extracted:
    import io

    from pypdf import PdfReader

    reader = PdfReader(io.BytesIO(data))
    pages = [_clean(p.extract_text() or "") for p in reader.pages]
    title = ""
    try:
        title = (reader.metadata.title or "") if reader.metadata else ""
    except Exception:  # noqa: BLE001 - malformed metadata must not fail the file
        title = ""
    textless = sum(1 for p in pages if len(re.sub(r"\s", "", p)) < _MIN_PAGE_CHARS)
    scanned = bool(pages) and textless == len(pages)
    if scanned:
        pages = ["" for _ in pages]
    return Extracted(pages, MIME_TYPES[".pdf"], str(title).strip(), scanned)


_W = "{http://schemas.openxmlformats.org/wordprocessingml/2006/main}"


def _extract_docx(data: bytes) -> Extracted:
    import io

    from docx import Document

    doc = Document(io.BytesIO(data))
    pages: list[list[str]] = [[]]

    def paragraph(p: Any) -> None:
        ppr = p.find(f"{_W}pPr")
        if ppr is not None and ppr.find(f"{_W}pageBreakBefore") is not None and any(pages[-1]):
            pages.append([])
        line: list[str] = []
        for node in p.iter():
            if node.tag == f"{_W}t":
                line.append(node.text or "")
            elif node.tag == f"{_W}tab":
                line.append("\t")
            elif node.tag in (f"{_W}br", f"{_W}cr"):
                if node.get(f"{_W}type") == "page":
                    pages[-1].append("".join(line))
                    line = []
                    pages.append([])
                else:
                    line.append("\n")
        pages[-1].append("".join(line))

    for child in doc.element.body.iterchildren():
        if child.tag == f"{_W}p":
            paragraph(child)
        elif child.tag == f"{_W}tbl":
            for row in child.iter(f"{_W}tr"):
                cells = ["".join(t.text or "" for t in tc.iter(f"{_W}t")) for tc in row.iter(f"{_W}tc")]
                pages[-1].append("\t".join(cells))
    title = (doc.core_properties.title or "").strip()
    return Extracted([_clean("\n".join(p)) for p in pages], MIME_TYPES[".docx"], title)


_BLOCK_TAGS = {
    "address", "article", "aside", "blockquote", "br", "dd", "div", "dl", "dt",
    "figcaption", "footer", "form", "h1", "h2", "h3", "h4", "h5", "h6", "header",
    "hr", "li", "main", "nav", "ol", "p", "pre", "section", "table", "td", "th",
    "tr", "ul",
}  # fmt: skip
_SKIP_TAGS = {"script", "style", "noscript", "template", "svg", "head"}


class _HtmlText(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []
        self.title: list[str] = []
        self._skip = 0
        self._in_title = False

    def handle_starttag(self, tag: str, attrs: Any) -> None:
        if tag == "title":
            self._in_title = True
        elif tag in _SKIP_TAGS:
            self._skip += 1
        elif tag in _BLOCK_TAGS:
            self.parts.append("\n")

    def handle_endtag(self, tag: str) -> None:
        if tag == "title":
            self._in_title = False
        elif tag in _SKIP_TAGS:
            self._skip = max(0, self._skip - 1)
        elif tag in _BLOCK_TAGS:
            self.parts.append("\n")

    def handle_data(self, data: str) -> None:
        if self._in_title:
            self.title.append(data)
        elif not self._skip:
            self.parts.append(data)


def _extract_html(html: str) -> Extracted:
    parser = _HtmlText()
    parser.feed(html)
    parser.close()
    lines = (re.sub(r"[ \t\u00a0]+", " ", line).strip() for line in "".join(parser.parts).split("\n"))
    text = "\n".join(lines)
    return Extracted([_clean(text)], "text/html", " ".join("".join(parser.title).split()))


# ── Ingestion ─────────────────────────────────────────────────────────────


def doc_id(path: str) -> str:
    """The stable document id for a Kosha ``path``."""
    return "f_" + hashlib.sha256(path.encode()).hexdigest()[:32]


def iter_files(root: Path) -> Iterator[Path]:
    """Every regular file under ``root``, in a stable order, skipping
    hidden files and directories and symlinks that leave ``root``."""
    if root.is_file():
        yield root
        return
    real_root = root.resolve()
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = sorted(d for d in dirnames if not d.startswith("."))
        for name in sorted(filenames):
            file = Path(dirpath) / name
            if name.startswith("."):
                continue
            # A symlink out of the folder would publish its target's
            # contents under the link's path.
            if file.is_symlink() and not file.resolve().is_relative_to(real_root):
                continue
            yield file


def kosha_path(root: Path, file: Path, prefix: str = "/") -> str:
    rel = file.name if root.is_file() else file.relative_to(root).as_posix()
    return str(PurePosixPath("/" + prefix.strip("/")) / rel)


def build_documents(
    path: str,
    extracted: Extracted,
    data: bytes,
    mtime: float,
    source: str,
    per_page: bool = False,
) -> list[dict]:
    """The Kosha documents for one extracted file."""
    meta = [
        _f("path", "Keyword", path),
        _f("mime_type", "Keyword", extracted.mime_type),
        _f("size_bytes", "Integer", str(len(data))),
        _f("modified_at", "Date", _iso(mtime)),
        _f("content_sha256", "Keyword", hashlib.sha256(data).hexdigest()),
        _f("page_count", "Integer", str(len(extracted.pages))),
        _f("scanned", "Boolean", "true" if extracted.scanned else "false"),
        _f("source", "Keyword", source),
    ]
    if extracted.title:
        meta.append(_f("title", "Text", extracted.title))
    base = doc_id(path)
    if not per_page:
        return [{"id": base, "fields": [*meta, _f("body", "Text", extracted.text)]}]
    return [
        {
            "id": f"{base}_p{n}",
            "fields": [*meta, _f("page", "Integer", str(n)), _f("body", "Text", text)],
        }
        for n, text in enumerate(extracted.pages, start=1)
    ]


def ingest(
    client: Any,
    namespace: str,
    root: str | os.PathLike,
    prefix: str = "/",
    per_page: bool = False,
    force: bool = False,
    source: str | None = None,
) -> IngestReport:
    """Ingest every supported file under ``root`` into ``namespace``,
    skipping files whose content is unchanged since the last run (unless
    ``force``), and flush."""
    transport = client._transport
    root = Path(root)
    report = IngestReport()
    source = source or "kosha-ingest"
    pending: list[tuple[str, Path]] = []
    for file in iter_files(root):
        if file.suffix.lower() not in MIME_TYPES:
            report.unsupported += 1
            continue
        pending.append((kosha_path(root, file, prefix), file))
    for i in range(0, len(pending), BATCH):
        batch = pending[i : i + BATCH]
        known = {} if force else _known_hashes(transport, namespace, [p for p, _ in batch], per_page)
        docs: list[dict] = []
        stale: list[tuple[str, int]] = []
        for path, file in batch:
            try:
                data = file.read_bytes()
                if known.get(path) == hashlib.sha256(data).hexdigest():
                    report.unchanged += 1
                    continue
                extracted = extract(file, data)
                docs.extend(
                    build_documents(path, extracted, data, file.stat().st_mtime, source, per_page)
                )
                if per_page:
                    stale.append((path, len(extracted.pages)))
                if extracted.scanned:
                    report.scanned.append(path)
                report.indexed += 1
            except Exception as e:  # noqa: BLE001 - one bad file must not stop the folder
                report.failed[path] = f"{type(e).__name__}: {e}"
        for chunk in _chunks(docs):
            transport.request("POST", ns_path(namespace, "documents"), {"documents": chunk})
        if stale:
            # Pages past each file's new page count, from an earlier and
            # longer version. Only this ingester's own page records.
            transport.request(
                "POST",
                ns_path(namespace, "delete"),
                {
                    "filter": {
                        "bool": {
                            "must": [
                                {"term": {"source": source}},
                                {"exists": {"field": "page"}},
                                {
                                    "bool": {
                                        "should": [
                                            {
                                                "bool": {
                                                    "must": [
                                                        {"term": {"path": path}},
                                                        {"range": {"page": {"gt": str(pages)}}},
                                                    ]
                                                }
                                            }
                                            for path, pages in stale
                                        ]
                                    }
                                },
                            ]
                        }
                    }
                },
            )
    if report.indexed:
        transport.request("POST", ns_path(namespace, "flush"), {})
    return report


def _chunks(docs: list[dict]) -> Iterator[list[dict]]:
    """``docs`` in write requests of at most ``MAX_REQUEST_BYTES`` of JSON
    each (a single larger document goes alone)."""
    chunk: list[dict] = []
    size = 0
    for doc in docs:
        n = len(json.dumps(doc).encode())
        if chunk and size + n > MAX_REQUEST_BYTES:
            yield chunk
            chunk, size = [], 0
        chunk.append(doc)
        size += n
    if chunk:
        yield chunk


def _known_hashes(
    transport: Any, namespace: str, paths: list[str], per_page: bool = False
) -> dict[str, str]:
    # One record per path: page 1 per-page, the page-less record otherwise,
    # so a batch never needs more than `len(paths)` hits.
    one = {"term": {"page": "1"}} if per_page else {"bool": {"must_not": [{"exists": {"field": "page"}}]}}
    body = {
        "query_text": "",
        "max_results": len(paths),
        "filter": {"bool": {"must": [{"terms": {"path": paths}}, one]}},
        "source_includes": ["path", "content_sha256"],
    }
    try:
        res = transport.request("POST", ns_path(namespace, "search"), body)
    except Exception as e:  # noqa: BLE001
        if getattr(e, "status_code", None) == 404:  # a namespace that does not exist yet
            return {}
        raise
    known: dict[str, str] = {}
    for hit in res.get("results", []):
        values = {f["name"]: f["value"] for f in hit.get("fields", [])}
        if "path" in values and "content_sha256" in values:
            known[values["path"]] = values["content_sha256"]
    return known


def _f(name: str, kind: str, value: str) -> dict:
    return {"name": name, "field_type": kind, "value": value}


def _iso(ts: float) -> str:
    return _dt.datetime.fromtimestamp(ts, _dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="kosha-ingest", description=__doc__.splitlines()[0])
    parser.add_argument("root", help="folder (or single file) to ingest")
    parser.add_argument("--namespace", default=os.environ.get("KOSHA_NAMESPACE"))
    parser.add_argument("--prefix", default="/", help="path prefix inside Kosha (default /)")
    parser.add_argument("--per-page", action="store_true", help="one document per page, with a `page` field")
    parser.add_argument("--force", action="store_true", help="re-index unchanged files too")
    parser.add_argument("--source", help="value of the `source` field (default kosha-ingest)")
    args = parser.parse_args(list(argv) if argv is not None else None)
    if not args.namespace:
        parser.error("--namespace (or KOSHA_NAMESPACE) is required")
    from .client import KoshaClient

    client = KoshaClient(api_key=os.environ.get("KOSHA_API_KEY"))
    report = ingest(client, args.namespace, args.root, args.prefix, args.per_page, args.force, args.source)
    json.dump(report.as_dict(), sys.stdout, indent=2)
    sys.stdout.write("\n")
    return 1 if report.failed else 0


if __name__ == "__main__":
    sys.exit(main())
