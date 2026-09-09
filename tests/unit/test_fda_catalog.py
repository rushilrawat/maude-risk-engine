import hashlib
from datetime import UTC, datetime
from email.message import Message
from pathlib import Path
from urllib.request import Request
from uuid import uuid4

import pytest

from maude.domain.enums import SourceRole, TableKind
from maude.ingestion.fda_catalog import (
    FDA_CATALOG_URL,
    CatalogError,
    fetch_current_catalog,
    parse_current_catalog,
)
from maude.storage.layout import RefreshLayout

ARCHIVE_NAMES = (
    "mdrfoi.zip",
    "mdrfoiadd.zip",
    "mdrfoichange.zip",
    "device.zip",
    "deviceadd.zip",
    "devicechange.zip",
    "patient.zip",
    "patientadd.zip",
    "patientchange.zip",
    "foitext.zip",
    "foitextadd.zip",
    "foitextchange.zip",
)


def _catalog_html(*, links: tuple[str, ...] = ARCHIVE_NAMES) -> str:
    anchors = "".join(
        f'<a href="https://www.accessdata.fda.gov/MAUDE/ftparea/{name}">{name}</a>'
        for name in links
    )
    return "".join(
        (
            '<a href="https://www.accessdata.fda.gov/MAUDE/ftparea/mdrfoithru2025.zip">old</a>',
            "<h2>MAUDE Data Downloadable Files</h2>",
            anchors,
            '<a href="https://www.accessdata.fda.gov/MAUDE/ftparea/patientproblemcode.zip">codes</a>',
            "<h2>Alternative Summary Reports</h2>",
            '<a href="https://www.accessdata.fda.gov/MAUDE/ftparea/mdrfoi.zip">outside</a>',
        )
    )


def test_catalog_selects_exact_current_archives_in_deterministic_order() -> None:
    artifacts = parse_current_catalog(_catalog_html(), FDA_CATALOG_URL)

    assert [(item.table, item.role, item.filename) for item in artifacts] == [
        (TableKind.MASTER, SourceRole.BASE, "mdrfoi.zip"),
        (TableKind.MASTER, SourceRole.ADD, "mdrfoiadd.zip"),
        (TableKind.MASTER, SourceRole.CHANGE, "mdrfoichange.zip"),
        (TableKind.DEVICE, SourceRole.BASE, "device.zip"),
        (TableKind.DEVICE, SourceRole.ADD, "deviceadd.zip"),
        (TableKind.DEVICE, SourceRole.CHANGE, "devicechange.zip"),
        (TableKind.PATIENT, SourceRole.BASE, "patient.zip"),
        (TableKind.PATIENT, SourceRole.ADD, "patientadd.zip"),
        (TableKind.PATIENT, SourceRole.CHANGE, "patientchange.zip"),
        (TableKind.NARRATIVE, SourceRole.BASE, "foitext.zip"),
        (TableKind.NARRATIVE, SourceRole.ADD, "foitextadd.zip"),
        (TableKind.NARRATIVE, SourceRole.CHANGE, "foitextchange.zip"),
    ]


def test_catalog_resolves_relative_archive_links() -> None:
    html = _catalog_html().replace(
        "https://www.accessdata.fda.gov/MAUDE/ftparea/", "//www.accessdata.fda.gov/MAUDE/ftparea/"
    )

    artifacts = parse_current_catalog(html, FDA_CATALOG_URL)

    assert artifacts[0].url == "https://www.accessdata.fda.gov/MAUDE/ftparea/mdrfoi.zip"


def test_catalog_rejects_missing_current_archive() -> None:
    with pytest.raises(CatalogError, match=r"missing.*narrative/change"):
        parse_current_catalog(_catalog_html(links=ARCHIVE_NAMES[:-1]), FDA_CATALOG_URL)


def test_catalog_rejects_duplicate_current_archive() -> None:
    with pytest.raises(CatalogError, match=r"duplicate.*master/base"):
        parse_current_catalog(_catalog_html(links=(*ARCHIVE_NAMES, "mdrfoi.zip")), FDA_CATALOG_URL)


@pytest.mark.parametrize(
    "bad_url",
    (
        "http://www.accessdata.fda.gov/MAUDE/ftparea/mdrfoi.zip",
        "https://example.com/MAUDE/ftparea/mdrfoi.zip",
        "https://www.accessdata.fda.gov:444/MAUDE/ftparea/mdrfoi.zip",
        "https://user@www.accessdata.fda.gov/MAUDE/ftparea/mdrfoi.zip",
        "https://www.accessdata.fda.gov/MAUDE/ftparea/mdrfoi.zip#fragment",
    ),
)
def test_catalog_rejects_untrusted_archive_urls(bad_url: str) -> None:
    html = _catalog_html().replace(
        "https://www.accessdata.fda.gov/MAUDE/ftparea/mdrfoi.zip", bad_url, 1
    )

    with pytest.raises(CatalogError, match="archive URL"):
        parse_current_catalog(html, FDA_CATALOG_URL)


class _Response:
    def __init__(self, body: bytes, final_url: str = FDA_CATALOG_URL) -> None:
        self._body = body
        self._position = 0
        self._final_url = final_url
        self.headers = Message()
        self.headers["Content-Type"] = "text/html; charset=utf-8"
        self.headers["ETag"] = '"catalog-v1"'
        self.headers["Last-Modified"] = "Mon, 07 Sep 2026 12:00:00 GMT"

    def read(self, amount: int = -1) -> bytes:
        if amount < 0:
            amount = len(self._body) - self._position
        block = self._body[self._position : self._position + amount]
        self._position += len(block)
        return block

    def geturl(self) -> str:
        return self._final_url

    def __enter__(self) -> "_Response":
        return self

    def __exit__(self, *args: object) -> None:
        return None


def test_fetch_catalog_persists_exact_page_and_http_evidence(tmp_path: Path) -> None:
    body = _catalog_html().encode()
    response = _Response(body)
    requests: list[tuple[Request, float]] = []

    def opener(request: Request, timeout: float) -> _Response:
        requests.append((request, timeout))
        return response

    layout = RefreshLayout(tmp_path, uuid4())
    retrieved_at = datetime(2026, 9, 8, tzinfo=UTC)

    evidence, artifacts = fetch_current_catalog(opener, layout, retrieved_at)

    expected_hash = hashlib.sha256(body).hexdigest()
    assert len(artifacts) == 12
    assert evidence.sha256 == expected_hash
    assert evidence.etag == '"catalog-v1"'
    assert evidence.last_modified == "Mon, 07 Sep 2026 12:00:00 GMT"
    assert Path(evidence.html_path) == layout.catalogs / f"{expected_hash}.html"
    assert Path(evidence.html_path).read_bytes() == body
    assert requests[0][0].full_url == FDA_CATALOG_URL
    assert requests[0][1] == 30.0


def test_fetch_catalog_rejects_redirected_final_url(tmp_path: Path) -> None:
    response = _Response(_catalog_html().encode(), "https://example.com/mdr-data-files")

    with pytest.raises(CatalogError, match="catalog URL"):
        fetch_current_catalog(
            lambda request, timeout: response,
            RefreshLayout(tmp_path, uuid4()),
            datetime.now(UTC),
        )
