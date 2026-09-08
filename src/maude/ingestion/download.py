import hashlib
import os
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from urllib.request import Request

from maude.domain.models import DiscoveredArtifact, DownloadedArtifact
from maude.ingestion.archive import (
    inspect_source,
    open_source_text,
    select_source_encoding,
    sha256_file,
)
from maude.ingestion.fda_catalog import CatalogError, validate_archive_url
from maude.ingestion.http import UrlOpener
from maude.ingestion.manifests import fsync_directory
from maude.ingestion.schemas import SchemaMismatch, detect_source_role, detect_table
from maude.storage.layout import RefreshLayout

DOWNLOAD_TIMEOUT_SECONDS = 30.0
DOWNLOAD_BLOCK_SIZE = 1024 * 1024
MAX_DOWNLOAD_BYTES = 2 * 1024**3


class DownloadError(ValueError):
    """Raised when a remote artifact cannot be admitted to immutable storage."""


def _content_length(value: str | None) -> int | None:
    if value is None:
        return None
    try:
        length = int(value)
    except ValueError as error:
        raise DownloadError("invalid HTTP Content-Length") from error
    if length < 0:
        raise DownloadError("invalid HTTP Content-Length")
    if length > MAX_DOWNLOAD_BYTES:
        raise DownloadError("download exceeds the maximum allowed size")
    return length


def _header(path: Path, member: str, encoding: str) -> tuple[str, ...]:
    with open_source_text(path, member, encoding=encoding) as source:
        return tuple(source.readline().rstrip("\r\n").split("|"))


def _admit_raw(layout: RefreshLayout, temporary: Path, digest: str, byte_size: int) -> Path:
    target = layout.raw_object(digest)
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        if target.stat().st_size != byte_size or sha256_file(target) != digest:
            raise DownloadError("raw checksum path contains different bytes")
        return target
    temporary.replace(target)
    fsync_directory(target.parent)
    return target


def download_artifact(
    artifact: DiscoveredArtifact,
    layout: RefreshLayout,
    opener: UrlOpener,
    started_at: datetime,
) -> DownloadedArtifact:
    """Stream and validate one FDA ZIP before admitting it to raw storage."""
    temporary: Path | None = None
    try:
        validate_archive_url(artifact.url)
        layout.raw_root.mkdir(parents=True, exist_ok=True)
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=".download.", suffix=".tmp", dir=layout.raw_root
        )
        temporary = Path(temporary_name)
        digest = hashlib.sha256()
        byte_size = 0
        request = Request(artifact.url, headers={"User-Agent": "maude-risk-engine/0.1"})
        with opener(request, DOWNLOAD_TIMEOUT_SECONDS) as response:
            final_url = response.geturl()
            final_filename = validate_archive_url(final_url)
            if final_filename != artifact.filename:
                raise DownloadError("redirected archive filename does not match discovery")
            expected_length = _content_length(response.headers.get("Content-Length"))
            etag = response.headers.get("ETag")
            last_modified = response.headers.get("Last-Modified")
            with os.fdopen(descriptor, "wb") as handle:
                while block := response.read(DOWNLOAD_BLOCK_SIZE):
                    byte_size += len(block)
                    if byte_size > MAX_DOWNLOAD_BYTES:
                        raise DownloadError("download exceeds the maximum allowed size")
                    digest.update(block)
                    handle.write(block)
                handle.flush()
                os.fsync(handle.fileno())
        if expected_length is not None and byte_size != expected_length:
            raise DownloadError(
                f"HTTP Content-Length mismatch: expected {expected_length}, received {byte_size}"
            )

        inspected = inspect_source(temporary)
        if inspected.member is None:
            raise DownloadError("FDA artifact must be a ZIP archive")
        encoding = select_source_encoding(temporary, inspected)
        header = _header(temporary, inspected.member, encoding)
        detected = detect_table(inspected.member, header)
        if detected.kind is not artifact.table:
            raise DownloadError(
                f"archive member family {detected.kind.value} does not match {artifact.table.value}"
            )
        role = detect_source_role(inspected.member)
        if role is not artifact.role:
            raise DownloadError(
                f"archive member role {role.value} does not match {artifact.role.value}"
            )

        checksum = digest.hexdigest()
        raw_path = _admit_raw(layout, temporary, checksum, byte_size)
        if raw_path != temporary:
            temporary.unlink(missing_ok=True)
        temporary = None
        return DownloadedArtifact(
            table=artifact.table,
            role=artifact.role,
            filename=artifact.filename,
            url=artifact.url,
            started_at=started_at,
            completed_at=datetime.now(UTC),
            final_url=final_url,
            etag=etag,
            last_modified=last_modified,
            byte_size=byte_size,
            sha256=checksum,
            raw_path=str(raw_path),
            archive_member=inspected.member,
            encoding=encoding,
            header=header,
        )
    except DownloadError:
        raise
    except (CatalogError, SchemaMismatch, OSError, ValueError) as error:
        raise DownloadError(str(error)) from error
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
