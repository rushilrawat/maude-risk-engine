import hashlib
import io
from datetime import UTC, datetime
from email.message import Message
from pathlib import Path
from urllib.request import Request
from uuid import uuid4
from zipfile import ZIP_DEFLATED, ZipFile

import pytest

from maude.domain.enums import SourceRole, TableKind
from maude.domain.models import DiscoveredArtifact
from maude.ingestion.download import MAX_DOWNLOAD_BYTES, DownloadError, download_artifact
from maude.storage.layout import RefreshLayout


def _zip_bytes(member: str = "mdrfoi.txt", header: str | None = None) -> bytes:
    if header is None:
        header = "MDR_REPORT_KEY|DATE_RECEIVED|EVENT_TYPE"
    buffer = io.BytesIO()
    with ZipFile(buffer, "w", compression=ZIP_DEFLATED) as archive:
        archive.writestr(member, f"{header}\n1|01/01/2026|M\n")
    return buffer.getvalue()


def _artifact() -> DiscoveredArtifact:
    return DiscoveredArtifact(
        table=TableKind.MASTER,
        role=SourceRole.BASE,
        filename="mdrfoi.zip",
        url="https://www.accessdata.fda.gov/MAUDE/ftparea/mdrfoi.zip",
    )


class _Response:
    def __init__(
        self,
        body: bytes,
        *,
        final_url: str | None = None,
        content_length: str | None = None,
        max_read: int | None = None,
    ) -> None:
        self._body = body
        self._position = 0
        self._final_url = final_url or _artifact().url
        self._max_read = max_read
        self.headers = Message()
        if content_length is not None:
            self.headers["Content-Length"] = content_length
        self.headers["ETag"] = '"archive-v1"'
        self.headers["Last-Modified"] = "Tue, 08 Sep 2026 12:00:00 GMT"

    def read(self, amount: int = -1) -> bytes:
        if self._max_read is not None:
            amount = min(amount, self._max_read)
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


def _opener(response: _Response):  # type: ignore[no-untyped-def]
    def open_response(request: Request, timeout: float) -> _Response:
        return response

    return open_response


def test_download_streams_valid_zip_into_checksum_storage(tmp_path: Path) -> None:
    body = _zip_bytes()
    layout = RefreshLayout(tmp_path, uuid4())
    started_at = datetime(2026, 9, 8, tzinfo=UTC)

    result = download_artifact(
        _artifact(),
        layout,
        _opener(_Response(body, content_length=str(len(body)), max_read=7)),
        started_at,
    )

    digest = hashlib.sha256(body).hexdigest()
    assert result.sha256 == digest
    assert result.raw_path == str(layout.raw_object(digest))
    assert Path(result.raw_path).read_bytes() == body
    assert result.archive_member == "mdrfoi.txt"
    assert result.header == ("MDR_REPORT_KEY", "DATE_RECEIVED", "EVENT_TYPE")
    assert result.etag == '"archive-v1"'
    assert result.role is SourceRole.BASE
    assert not tuple(layout.raw_root.rglob("*.tmp"))


def test_download_allows_missing_content_length(tmp_path: Path) -> None:
    result = download_artifact(
        _artifact(),
        RefreshLayout(tmp_path, uuid4()),
        _opener(_Response(_zip_bytes())),
        datetime.now(UTC),
    )

    assert result.byte_size > 0


@pytest.mark.parametrize("content_length", ["invalid", "-1"])
def test_download_rejects_invalid_content_length(tmp_path: Path, content_length: str) -> None:
    layout = RefreshLayout(tmp_path, uuid4())

    with pytest.raises(DownloadError, match="Content-Length"):
        download_artifact(
            _artifact(),
            layout,
            _opener(_Response(_zip_bytes(), content_length=content_length)),
            datetime.now(UTC),
        )

    assert not tuple(layout.raw_root.rglob("*.zip"))


def test_download_rejects_truncated_response(tmp_path: Path) -> None:
    body = _zip_bytes()
    layout = RefreshLayout(tmp_path, uuid4())

    with pytest.raises(DownloadError, match="Content-Length"):
        download_artifact(
            _artifact(),
            layout,
            _opener(_Response(body, content_length=str(len(body) + 10))),
            datetime.now(UTC),
        )

    assert not tuple(layout.raw_root.rglob("*.zip"))


def test_download_rejects_oversized_body(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("maude.ingestion.download.MAX_DOWNLOAD_BYTES", 20)
    layout = RefreshLayout(tmp_path, uuid4())

    with pytest.raises(DownloadError, match="maximum"):
        download_artifact(
            _artifact(),
            layout,
            _opener(_Response(b"x" * 21)),
            datetime.now(UTC),
        )

    assert MAX_DOWNLOAD_BYTES > 20
    assert not tuple(layout.raw_root.rglob("*.zip"))


def test_download_rejects_wrong_redirect_host(tmp_path: Path) -> None:
    layout = RefreshLayout(tmp_path, uuid4())
    response = _Response(_zip_bytes(), final_url="https://example.com/mdrfoi.zip")

    with pytest.raises(DownloadError, match="archive URL"):
        download_artifact(_artifact(), layout, _opener(response), datetime.now(UTC))


@pytest.mark.parametrize(
    ("member", "header", "message"),
    (
        ("patient.txt", "MDR_REPORT_KEY|PATIENT_SEQUENCE_NUMBER", "family"),
        ("mdrfoiadd.txt", None, "role"),
        ("mdrfoi.txt", "MDR_REPORT_KEY|DATE_RECEIVED", "required columns"),
    ),
)
def test_download_rejects_member_contract_mismatch(
    tmp_path: Path, member: str, header: str | None, message: str
) -> None:
    layout = RefreshLayout(tmp_path, uuid4())

    with pytest.raises(DownloadError, match=message):
        download_artifact(
            _artifact(),
            layout,
            _opener(_Response(_zip_bytes(member, header))),
            datetime.now(UTC),
        )

    assert not tuple(layout.raw_root.rglob("*.zip"))


def test_download_reuses_existing_identical_raw_object(tmp_path: Path) -> None:
    body = _zip_bytes()
    layout = RefreshLayout(tmp_path, uuid4())
    first = download_artifact(_artifact(), layout, _opener(_Response(body)), datetime.now(UTC))
    before = Path(first.raw_path).stat().st_mtime_ns

    second = download_artifact(_artifact(), layout, _opener(_Response(body)), datetime.now(UTC))

    assert second.raw_path == first.raw_path
    assert Path(second.raw_path).stat().st_mtime_ns == before
