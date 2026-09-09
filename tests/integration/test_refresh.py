import io
from datetime import UTC, datetime
from email.message import Message
from pathlib import Path
from urllib.request import Request
from zipfile import ZIP_DEFLATED, ZipFile, ZipInfo

import pyarrow.parquet as pq
import pytest

from maude.config import Settings
from maude.domain.enums import RefreshOutcome, TableKind
from maude.ingestion import pipeline
from maude.ingestion.fda_catalog import CURRENT_ARCHIVES, FDA_CATALOG_URL
from maude.ingestion.manifests import load_manifest
from maude.ingestion.refresh import refresh_fda
from maude.storage.layout import SnapshotLayout

HEADERS = {
    TableKind.MASTER: "MDR_REPORT_KEY|DATE_RECEIVED|EVENT_TYPE",
    TableKind.DEVICE: "MDR_REPORT_KEY|DEVICE_SEQUENCE_NO|DEVICE_REPORT_PRODUCT_CODE",
    TableKind.PATIENT: "MDR_REPORT_KEY|PATIENT_SEQUENCE_NUMBER",
    TableKind.NARRATIVE: "MDR_REPORT_KEY|MDR_TEXT_KEY|TEXT_TYPE_CODE|FOI_TEXT",
}


def _row(table: TableKind, key: str, value: str) -> str:
    if table is TableKind.MASTER:
        return f"{key}|01/01/2026|{value}"
    if table is TableKind.DEVICE:
        return f"{key}|1|{value}"
    if table is TableKind.PATIENT:
        return f"{key}|1"
    return f"{key}|{key}0|D|{value}"


def _zip(member: str, table: TableKind, rows: list[str]) -> bytes:
    buffer = io.BytesIO()
    with ZipFile(buffer, "w", ZIP_DEFLATED) as archive:
        info = ZipInfo(member, date_time=(2026, 1, 1, 0, 0, 0))
        info.compress_type = ZIP_DEFLATED
        archive.writestr(info, f"{HEADERS[table]}\n{'\n'.join(rows)}\n")
    return buffer.getvalue()


def _source_bodies(change_value: str = "change") -> dict[str, bytes]:
    bodies: dict[str, bytes] = {}
    for filename, (table, role) in CURRENT_ARCHIVES.items():
        stem = filename.removesuffix(".zip")
        if role.value == "base":
            rows = [_row(table, "1", "base")]
        elif role.value == "add":
            rows = [_row(table, "2", "add")]
        else:
            rows = [_row(table, "1", change_value)]
        bodies[filename] = _zip(f"{stem}.txt", table, rows)
    return bodies


def _catalog() -> bytes:
    anchors = "".join(
        f'<a href="https://www.accessdata.fda.gov/MAUDE/ftparea/{filename}">{filename}</a>'
        for filename in CURRENT_ARCHIVES
    )
    return (
        f"<h2>MAUDE Data Downloadable Files</h2>{anchors}<h2>Alternative Summary Reports</h2>"
    ).encode()


class _Response:
    def __init__(self, body: bytes, url: str, content_type: str) -> None:
        self._body = body
        self._position = 0
        self._url = url
        self.headers = Message()
        self.headers["Content-Type"] = content_type
        self.headers["Content-Length"] = str(len(body))

    def read(self, amount: int = -1) -> bytes:
        if amount < 0:
            amount = len(self._body) - self._position
        block = self._body[self._position : self._position + amount]
        self._position += len(block)
        return block

    def geturl(self) -> str:
        return self._url

    def __enter__(self) -> "_Response":
        return self

    def __exit__(self, *args: object) -> None:
        return None


class _Opener:
    def __init__(self, bodies: dict[str, bytes]) -> None:
        self.bodies = bodies

    def __call__(self, request: Request, timeout: float) -> _Response:
        if request.full_url == FDA_CATALOG_URL:
            return _Response(_catalog(), FDA_CATALOG_URL, "text/html; charset=utf-8")
        filename = request.full_url.rsplit("/", 1)[-1]
        return _Response(self.bodies[filename], request.full_url, "application/zip")


def _settings(tmp_path: Path) -> Settings:
    return Settings(data_root=tmp_path / "data", batch_rows=2, parser_version="test")


def test_refresh_downloads_reconciles_and_promotes_current_snapshot(tmp_path: Path) -> None:
    settings = _settings(tmp_path)

    result = refresh_fda(
        settings,
        opener=_Opener(_source_bodies()),
        now=lambda: datetime(2026, 9, 8, tzinfo=UTC),
    )

    assert result.outcome is RefreshOutcome.PROMOTED
    assert result.snapshot_id is not None
    assert result.snapshot_id.startswith("fda-current-")
    assert len(result.artifacts) == 12
    assert len(result.reconciliation) == 12
    current = load_manifest(settings.data_root / "manifests" / "current.json")
    assert current.snapshot_id == result.snapshot_id
    assert {table.table for table in current.tables} == set(TableKind)
    for table in current.tables:
        rows = pq.read_table(table.silver_path).to_pylist()
        assert len(rows) == 2
        changed = next(row for row in rows if row["mdr_report_key"] == "1")
        assert changed["_source_role"] == "change"


def test_identical_refresh_is_unchanged_without_rewriting_current(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    opener = _Opener(_source_bodies())
    first = refresh_fda(settings, opener=opener)
    assert first.snapshot_id is not None
    layout = SnapshotLayout(settings.data_root, first.snapshot_id)
    current_before = layout.current_manifest.read_bytes()
    mtimes_before = {
        path.name: path.stat().st_mtime_ns for path in layout.promoted.glob("*.parquet")
    }

    second = refresh_fda(settings, opener=_Opener(_source_bodies()))

    assert second.outcome is RefreshOutcome.UNCHANGED
    assert second.snapshot_id == first.snapshot_id
    assert layout.current_manifest.read_bytes() == current_before
    mtimes_after = {
        path.name: path.stat().st_mtime_ns for path in layout.promoted.glob("*.parquet")
    }
    assert mtimes_after == mtimes_before


def test_download_failure_preserves_previous_current_snapshot(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    first = refresh_fda(settings, opener=_Opener(_source_bodies()))
    assert first.snapshot_id is not None
    current_path = settings.data_root / "manifests" / "current.json"
    current_before = current_path.read_bytes()
    broken = _source_bodies()
    broken["patientchange.zip"] = b"not a zip"

    failed = refresh_fda(settings, opener=_Opener(broken))

    assert failed.outcome is RefreshOutcome.FAILED
    assert failed.failure is not None
    assert failed.failure.phase == "download"
    assert failed.failure.table is TableKind.PATIENT
    assert current_path.read_bytes() == current_before
    assert Path(settings.data_root / "refresh-runs" / f"{failed.run_id}.json").exists()


def test_quality_failure_reports_exact_phase_and_preserves_current(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    first = refresh_fda(settings, opener=_Opener(_source_bodies()))
    current_path = settings.data_root / "manifests" / "current.json"
    current_before = current_path.read_bytes()
    orphaned = _source_bodies("changed-again")
    orphaned["patientchange.zip"] = _zip(
        "patientchange.txt", TableKind.PATIENT, [_row(TableKind.PATIENT, "999", "unused")]
    )

    failed = refresh_fda(settings, opener=_Opener(orphaned))

    assert first.outcome is RefreshOutcome.PROMOTED
    assert failed.outcome is RefreshOutcome.FAILED
    assert failed.failure is not None
    assert failed.failure.phase == "quality"
    assert current_path.read_bytes() == current_before


def test_parse_failure_reports_exact_phase_and_preserves_current(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = _settings(tmp_path)
    refresh_fda(settings, opener=_Opener(_source_bodies("version-a")))
    current_path = settings.data_root / "manifests" / "current.json"
    current_before = current_path.read_bytes()

    def fail_parse(*args: object, **kwargs: object) -> None:
        raise RuntimeError("parse failed")

    monkeypatch.setattr(pipeline, "parse_to_bronze", fail_parse)
    failed = refresh_fda(settings, opener=_Opener(_source_bodies("version-b")))

    assert failed.outcome is RefreshOutcome.FAILED
    assert failed.failure is not None
    assert failed.failure.phase == "parse"
    assert current_path.read_bytes() == current_before


def test_reconcile_failure_reports_exact_phase_and_preserves_current(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = _settings(tmp_path)
    refresh_fda(settings, opener=_Opener(_source_bodies("version-a")))
    current_path = settings.data_root / "manifests" / "current.json"
    current_before = current_path.read_bytes()

    def fail_reconcile(*args: object, **kwargs: object) -> None:
        raise RuntimeError("reconcile failed")

    monkeypatch.setattr(pipeline, "reconcile_table", fail_reconcile)
    failed = refresh_fda(settings, opener=_Opener(_source_bodies("version-b")))

    assert failed.outcome is RefreshOutcome.FAILED
    assert failed.failure is not None
    assert failed.failure.phase == "reconcile"
    assert current_path.read_bytes() == current_before


def test_previously_seen_fingerprint_is_revalidated_and_republished(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    first = refresh_fda(settings, opener=_Opener(_source_bodies("version-a")))
    second = refresh_fda(settings, opener=_Opener(_source_bodies("version-b")))
    assert first.snapshot_id is not None
    assert second.snapshot_id is not None
    assert first.snapshot_id != second.snapshot_id

    restored = refresh_fda(settings, opener=_Opener(_source_bodies("version-a")))

    assert restored.outcome is RefreshOutcome.PROMOTED
    assert restored.snapshot_id == first.snapshot_id
    current = load_manifest(settings.data_root / "manifests" / "current.json")
    assert current.snapshot_id == first.snapshot_id
