"""kosha-ingest (#587): extraction with page boundaries, document shape and
idempotent re-ingest."""

import hashlib
import io

import pytest

from kosha_client import KoshaClient
from kosha_client.ingest import (
    PAGE_BREAK,
    build_documents,
    doc_id,
    extract,
    ingest,
    iter_files,
    kosha_path,
)


def make_pdf(pages: list[str], title: str = "") -> bytes:
    """A minimal PDF with one line of Helvetica text per page ("" = an
    image-only page with no text layer)."""
    n = len(pages)
    font = 3 + 2 * n
    objs = {
        1: "<< /Type /Catalog /Pages 2 0 R >>",
        2: "<< /Type /Pages /Kids [%s] /Count %d >>" % (" ".join(f"{3 + 2 * i} 0 R" for i in range(n)), n),
        font: "<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
    }
    for i, text in enumerate(pages):
        stream = f"BT /F1 12 Tf 72 720 Td ({text}) Tj ET" if text else ""
        objs[3 + 2 * i] = (
            f"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
            f"/Resources << /Font << /F1 {font} 0 R >> >> /Contents {4 + 2 * i} 0 R >>"
        )
        objs[4 + 2 * i] = f"<< /Length {len(stream)} >>\nstream\n{stream}\nendstream"
    if title:
        objs[font + 1] = f"<< /Title ({title}) >>"
    out = io.BytesIO()
    out.write(b"%PDF-1.4\n")
    offsets = {}
    for num in sorted(objs):
        offsets[num] = out.tell()
        out.write(f"{num} 0 obj\n{objs[num]}\nendobj\n".encode())
    xref = out.tell()
    size = max(objs) + 1
    out.write(f"xref\n0 {size}\n0000000000 65535 f \n".encode())
    for num in range(1, size):
        out.write(f"{offsets[num]:010d} 00000 n \n".encode())
    info = f" /Info {font + 1} 0 R" if title else ""
    out.write(f"trailer\n<< /Size {size} /Root 1 0 R{info} >>\nstartxref\n{xref}\n%%EOF\n".encode())
    return out.getvalue()


def make_docx(pages: list[list[str]], title: str = "") -> bytes:
    docx = pytest.importorskip("docx")
    from docx.enum.text import WD_BREAK

    d = docx.Document()
    d.core_properties.title = title
    for i, paras in enumerate(pages):
        for j, text in enumerate(paras):
            p = d.add_paragraph(text)
            if i < len(pages) - 1 and j == len(paras) - 1:
                p.add_run().add_break(WD_BREAK.PAGE)
    t = d.add_table(rows=1, cols=2)
    t.rows[0].cells[0].text, t.rows[0].cells[1].text = "Party", "Acme"
    buf = io.BytesIO()
    d.save(buf)
    return buf.getvalue()


def test_pdf_pages_are_form_feed_separated():
    pytest.importorskip("pypdf")
    out = extract("a.pdf", make_pdf(["Indemnity clause here", "Late fees accrue monthly", "Termination for cause"], "MSA"))
    assert out.mime_type == "application/pdf"
    assert out.title == "MSA"
    assert out.pages == ["Indemnity clause here", "Late fees accrue monthly", "Termination for cause"]
    assert out.text.split(PAGE_BREAK)[1] == "Late fees accrue monthly"
    assert not out.scanned


def test_pdf_without_a_text_layer_is_flagged_scanned():
    pytest.importorskip("pypdf")
    out = extract("scan.pdf", make_pdf(["", ""]))
    assert out.scanned
    assert out.pages == ["", ""]


def test_docx_explicit_page_breaks_and_tables():
    out = extract("a.docx", make_docx([["Recitals", "Whereas Acme"], ["Payment terms"]], "Lease"))
    assert out.title == "Lease"
    assert len(out.pages) == 2
    assert out.pages[0] == "Recitals\nWhereas Acme"
    assert out.pages[1] == "Payment terms\nParty\tAcme"


def test_html_text_title_and_skipped_tags():
    html = (
        "<html><head><title> Policy  Doc </title><style>p{}</style></head><body>"
        "<script>var x = 1;</script><h1>Refunds</h1><p>Within&nbsp;30 days.</p>"
        "<ul><li>One</li><li>Two</li></ul>\f</body></html>"
    )
    out = extract("p.html", html.encode())
    assert out.title == "Policy Doc"
    assert out.pages == ["Refunds\n\nWithin 30 days.\n\nOne\n\nTwo"]
    assert "var x" not in out.text


def test_text_and_unsupported():
    assert extract("a.md", b"# Hi\n").pages == ["# Hi"]
    assert extract("a.txt", b"page one\n\fpage two\r\n").pages == ["page one", "page two"]
    with pytest.raises(Exception, match="unsupported"):
        extract("a.xlsx", b"")


def test_paths_and_ids(tmp_path):
    (tmp_path / "c" / "2024").mkdir(parents=True)
    f = tmp_path / "c" / "2024" / "a.pdf"
    f.write_bytes(b"")
    assert kosha_path(tmp_path, f) == "/c/2024/a.pdf"
    assert kosha_path(tmp_path, f, "/docs/") == "/docs/c/2024/a.pdf"
    assert kosha_path(f, f, "x") == "/x/a.pdf"
    assert doc_id("/a") == doc_id("/a") != doc_id("/b")


def test_build_documents_whole_and_per_page():
    pytest.importorskip("pypdf")
    data = make_pdf(["one", "two"])
    ex = extract("a.pdf", data)
    (doc,) = build_documents("/a.pdf", ex, data, 0, "test")
    fields = {f["name"]: (f["field_type"], f["value"]) for f in doc["fields"]}
    assert doc["id"] == doc_id("/a.pdf")
    assert fields["path"] == ("Keyword", "/a.pdf")
    assert fields["body"] == ("Text", "one\ftwo")
    assert fields["page_count"] == ("Integer", "2")
    assert fields["modified_at"] == ("Date", "1970-01-01T00:00:00Z")
    assert fields["content_sha256"] == ("Keyword", hashlib.sha256(data).hexdigest())
    assert fields["scanned"] == ("Boolean", "false")
    pages = build_documents("/a.pdf", ex, data, 0, "test", per_page=True)
    assert [d["id"] for d in pages] == [doc_id("/a.pdf") + "_p1", doc_id("/a.pdf") + "_p2"]
    assert [{f["name"]: f["value"] for f in d["fields"]}["page"] for d in pages] == ["1", "2"]


class FakeTransport:
    def __init__(self):
        self.calls = []
        self.stored = {}

    def request(self, method, path, body=None, headers=None):  # noqa: ANN001
        self.calls.append((method, path, body))
        if path.endswith("/documents"):
            for d in body["documents"]:
                self.stored[d["id"]] = {f["name"]: f["value"] for f in d["fields"]}
            return {"indexed_count": len(body["documents"])}
        if path.endswith("/search"):
            paths, one = body["filter"]["bool"]["must"]
            wanted = set(paths["terms"]["path"])
            page = one.get("term", {}).get("page")
            return {
                "results": [
                    {"doc_id": i, "fields": [{"name": k, "value": v} for k, v in f.items()]}
                    for i, f in self.stored.items()
                    if f["path"] in wanted and f.get("page") == page
                ]
            }
        return {}


def test_ingest_is_idempotent(tmp_path):
    pytest.importorskip("pypdf")
    (tmp_path / "contracts").mkdir()
    (tmp_path / "contracts" / "a.pdf").write_bytes(make_pdf(["alpha", "beta"]))
    (tmp_path / "contracts" / "scan.pdf").write_bytes(make_pdf([""]))
    (tmp_path / "notes.html").write_text("<p>hello</p>")
    (tmp_path / "data.xlsx").write_bytes(b"x")
    (tmp_path / "broken.pdf").write_bytes(b"%PDF-1.4 garbage")
    (tmp_path / ".hidden.txt").write_text("skip")
    c = KoshaClient("http://kosha.test", api_key="k")
    c._transport = t = FakeTransport()

    r = ingest(c, "docs", tmp_path)
    assert (r.indexed, r.unchanged, r.unsupported) == (3, 0, 1)
    assert r.scanned == ["/contracts/scan.pdf"]
    assert list(r.failed) == ["/broken.pdf"]
    assert {f["path"] for f in t.stored.values()} == {"/contracts/a.pdf", "/contracts/scan.pdf", "/notes.html"}
    assert t.calls[-1][1] == "v1/namespaces/docs/flush"

    t.calls.clear()
    r = ingest(c, "docs", tmp_path)
    assert (r.indexed, r.unchanged) == (0, 3)
    assert not any(p.endswith("/documents") or p.endswith("/flush") for _, p, _ in t.calls)

    (tmp_path / "notes.html").write_text("<p>changed</p>")
    r = ingest(c, "docs", tmp_path)
    assert (r.indexed, r.unchanged) == (1, 2)
    assert t.stored[doc_id("/notes.html")]["body"] == "changed"
    assert ingest(c, "docs", tmp_path, force=True).indexed == 3


def test_per_page_reingest_deletes_dropped_pages(tmp_path):
    pytest.importorskip("pypdf")
    f = tmp_path / "a.pdf"
    f.write_bytes(make_pdf(["one", "two", "three"]))
    c = KoshaClient("http://kosha.test", api_key="k")
    c._transport = t = FakeTransport()
    ingest(c, "docs", tmp_path, per_page=True)
    assert len(t.stored) == 3
    f.write_bytes(make_pdf(["one"]))
    ingest(c, "docs", tmp_path, per_page=True)
    deletes = [b for _, p, b in t.calls if p.endswith("/delete")]
    assert len(deletes) == 2  # the first run's is a no-op
    source, exists, paths = deletes[-1]["filter"]["bool"]["must"]
    assert source == {"term": {"source": "kosha-ingest"}}
    assert exists == {"exists": {"field": "page"}}
    assert paths["bool"]["should"] == [
        {"bool": {"must": [{"term": {"path": "/a.pdf"}}, {"range": {"page": {"gt": "1"}}}]}}
    ]
    # --force still drops the pages past the new end.
    t.calls.clear()
    ingest(c, "docs", tmp_path, per_page=True, force=True)
    assert [p for _, p, _ in t.calls if p.endswith("/delete")] == ["v1/namespaces/docs/delete"]


def test_iter_files_skips_symlinks_out_of_the_folder(tmp_path):
    root, outside = tmp_path / "root", tmp_path / "secret.txt"
    root.mkdir()
    outside.write_text("secret")
    (root / "a.txt").write_text("a")
    (root / "leak.txt").symlink_to(outside)
    (root / "alias.txt").symlink_to(root / "a.txt")
    (root / "loop.txt").symlink_to(root / "loop.txt")
    assert [p.name for p in iter_files(root)] == ["a.txt", "alias.txt"]


def test_ingest_splits_writes_by_request_size(tmp_path, monkeypatch):
    from kosha_client import ingest as ingest_mod

    for i in range(5):
        (tmp_path / f"{i}.txt").write_text("x" * 1000)
    monkeypatch.setattr(ingest_mod, "MAX_REQUEST_BYTES", 3000)
    c = KoshaClient("http://kosha.test", api_key="k")
    c._transport = t = FakeTransport()
    assert ingest(c, "docs", tmp_path).indexed == 5
    writes = [len(b["documents"]) for _, p, b in t.calls if p.endswith("/documents")]
    assert writes == [2, 2, 1]
