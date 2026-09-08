import hashlib
import os
import tempfile
from datetime import datetime
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urljoin, urlsplit
from urllib.request import Request

from maude.domain.enums import SourceRole, TableKind
from maude.domain.models import CatalogEvidence, DiscoveredArtifact
from maude.ingestion.http import HttpResponse, UrlOpener
from maude.ingestion.manifests import fsync_directory
from maude.storage.layout import RefreshLayout

FDA_CATALOG_URL = (
    "https://www.fda.gov/medical-devices/medical-device-reporting-mdr-how-report-"
    "medical-device-problems/mdr-data-files"
)
CATALOG_TIMEOUT_SECONDS = 30.0
MAX_CATALOG_BYTES = 8 * 1024 * 1024

CURRENT_ARCHIVES = {
    "mdrfoi.zip": (TableKind.MASTER, SourceRole.BASE),
    "mdrfoiadd.zip": (TableKind.MASTER, SourceRole.ADD),
    "mdrfoichange.zip": (TableKind.MASTER, SourceRole.CHANGE),
    "device.zip": (TableKind.DEVICE, SourceRole.BASE),
    "deviceadd.zip": (TableKind.DEVICE, SourceRole.ADD),
    "devicechange.zip": (TableKind.DEVICE, SourceRole.CHANGE),
    "patient.zip": (TableKind.PATIENT, SourceRole.BASE),
    "patientadd.zip": (TableKind.PATIENT, SourceRole.ADD),
    "patientchange.zip": (TableKind.PATIENT, SourceRole.CHANGE),
    "foitext.zip": (TableKind.NARRATIVE, SourceRole.BASE),
    "foitextadd.zip": (TableKind.NARRATIVE, SourceRole.ADD),
    "foitextchange.zip": (TableKind.NARRATIVE, SourceRole.CHANGE),
}


class CatalogError(ValueError):
    """Raised when the FDA catalog cannot produce one trusted current source set."""


class _CatalogParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self._heading_parts: list[str] | None = None
        self.in_download_section = False
        self.links: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() == "h2":
            self._heading_parts = []
            return
        if tag.lower() != "a" or not self.in_download_section:
            return
        href = next((value for name, value in attrs if name.lower() == "href"), None)
        if href is not None:
            self.links.append(href)

    def handle_data(self, data: str) -> None:
        if self._heading_parts is not None:
            self._heading_parts.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() != "h2" or self._heading_parts is None:
            return
        heading = " ".join("".join(self._heading_parts).split()).casefold()
        if heading == "maude data downloadable files":
            self.in_download_section = True
        elif heading == "alternative summary reports":
            self.in_download_section = False
        self._heading_parts = None


def _url_parts(url: str, *, label: str, host: str) -> tuple[str, str]:
    parsed = urlsplit(url)
    try:
        port = parsed.port
    except ValueError as error:
        raise CatalogError(f"untrusted {label}: invalid port") from error
    if (
        parsed.scheme != "https"
        or parsed.hostname != host
        or port is not None
        or parsed.username is not None
        or parsed.password is not None
        or parsed.fragment
    ):
        raise CatalogError(f"untrusted {label}: {url}")
    return parsed.path, parsed.query


def _validate_catalog_url(url: str) -> None:
    path, query = _url_parts(url, label="catalog URL", host="www.fda.gov")
    expected_path = urlsplit(FDA_CATALOG_URL).path
    if path.rstrip("/") != expected_path or query:
        raise CatalogError(f"untrusted catalog URL: {url}")


def _validate_archive_url(url: str) -> str:
    path, query = _url_parts(url, label="archive URL", host="www.accessdata.fda.gov")
    if query:
        raise CatalogError(f"untrusted archive URL: {url}")
    return path.rsplit("/", 1)[-1]


def parse_current_catalog(html: str, catalog_url: str) -> tuple[DiscoveredArtifact, ...]:
    """Return exactly the allowlisted current-year artifacts from the FDA table."""
    _validate_catalog_url(catalog_url)
    parser = _CatalogParser()
    parser.feed(html)
    parser.close()

    discovered: dict[tuple[TableKind, SourceRole], DiscoveredArtifact] = {}
    for href in parser.links:
        url = urljoin(catalog_url, href)
        filename = urlsplit(url).path.rsplit("/", 1)[-1]
        classification = CURRENT_ARCHIVES.get(filename)
        if classification is None:
            continue
        trusted_filename = _validate_archive_url(url)
        table, role = classification
        key = (table, role)
        if key in discovered:
            raise CatalogError(f"duplicate FDA catalog artifact for {table.value}/{role.value}")
        discovered[key] = DiscoveredArtifact(
            table=table,
            role=role,
            filename=trusted_filename,
            url=url,
        )

    missing = [
        f"{table.value}/{role.value}"
        for table in TableKind
        for role in SourceRole
        if (table, role) not in discovered
    ]
    if missing:
        raise CatalogError(f"missing FDA catalog artifacts: {', '.join(missing)}")
    return tuple(discovered[(table, role)] for table in TableKind for role in SourceRole)


def _read_catalog(response: HttpResponse) -> bytes:
    read = response.read
    body = bytearray()
    while block := read(min(1024 * 1024, MAX_CATALOG_BYTES + 1 - len(body))):
        body.extend(block)
        if len(body) > MAX_CATALOG_BYTES:
            raise CatalogError("FDA catalog exceeds the maximum allowed size")
    return bytes(body)


def _store_catalog(layout: RefreshLayout, body: bytes, sha256: str) -> Path:
    layout.catalogs.mkdir(parents=True, exist_ok=True)
    target = layout.catalogs / f"{sha256}.html"
    if target.exists():
        if target.read_bytes() != body:
            raise CatalogError("catalog checksum path contains different bytes")
        return target
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{target.name}.", suffix=".tmp", dir=target.parent
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(body)
            handle.flush()
            os.fsync(handle.fileno())
        temporary.replace(target)
        fsync_directory(target.parent)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise
    return target


def fetch_current_catalog(
    opener: UrlOpener,
    layout: RefreshLayout,
    retrieved_at: datetime,
) -> tuple[CatalogEvidence, tuple[DiscoveredArtifact, ...]]:
    """Fetch, preserve, and parse the official FDA current-year catalog."""
    request = Request(FDA_CATALOG_URL, headers={"User-Agent": "maude-risk-engine/0.1"})
    with opener(request, CATALOG_TIMEOUT_SECONDS) as response:
        final_url = response.geturl()
        _validate_catalog_url(final_url)
        body = _read_catalog(response)
        charset = response.headers.get_content_charset() or "utf-8"
        etag = response.headers.get("ETag")
        last_modified = response.headers.get("Last-Modified")
    digest = hashlib.sha256(body).hexdigest()
    path = _store_catalog(layout, body, digest)
    try:
        html = body.decode(charset)
    except (LookupError, UnicodeDecodeError) as error:
        raise CatalogError(f"FDA catalog could not be decoded as {charset}") from error
    artifacts = parse_current_catalog(html, final_url)
    evidence = CatalogEvidence(
        catalog_url=FDA_CATALOG_URL,
        retrieved_at=retrieved_at,
        final_url=final_url,
        etag=etag,
        last_modified=last_modified,
        sha256=digest,
        html_path=str(path),
    )
    return evidence, artifacts
