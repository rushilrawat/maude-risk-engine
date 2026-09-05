from __future__ import annotations

from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

import pytest

import maude.ingestion.pipeline as pipeline
from maude.config import Settings
from maude.domain.enums import QualityLevel, RunStatus, TableKind
from maude.ingestion.manifests import load_manifest
from maude.ingestion.pipeline import SnapshotConflict, ingest_snapshot
from maude.storage.layout import SnapshotLayout


def _write_zip(path: Path, member: str, content: str) -> Path:
    with ZipFile(path, "w", ZIP_DEFLATED) as archive:
        archive.writestr(member, content)
    return path


def _sources(
    root: Path,
    *,
    narrative_header: bool = True,
    second_master_event: str = "M",
    first_master_date: str = "01/02/2025",
) -> tuple[Path, ...]:
    root.mkdir(parents=True, exist_ok=True)
    master = _write_zip(
        root / "mdrfoi.zip",
        "mdrfoi.txt",
        "MDR_REPORT_KEY|DATE_RECEIVED|EVENT_TYPE\n"
        f"1|{first_master_date}|D\n"
        f"2|01/03/2025|{second_master_event}\n",
    )
    device = _write_zip(
        root / "device.zip",
        "device.txt",
        "MDR_REPORT_KEY|DEVICE_SEQUENCE_NO|DEVICE_REPORT_PRODUCT_CODE\n1|1|ABC\n",
    )
    patient = _write_zip(
        root / "patient.zip",
        "patient.txt",
        "MDR_REPORT_KEY|PATIENT_SEQUENCE_NUMBER\n2|1\n",
    )
    narrative_columns = "MDR_REPORT_KEY|MDR_TEXT_KEY|TEXT_TYPE_CODE|DATE_REPORT"
    if narrative_header:
        narrative_columns += "|FOI_TEXT"
    narrative_row = "1|10|N|01/04/2025"
    if narrative_header:
        narrative_row += "|Pump stopped unexpectedly"
    narrative = _write_zip(
        root / "foitext.zip", "foitext.txt", f"{narrative_columns}\n{narrative_row}\n"
    )
    return (narrative, patient, device, master)


def _settings(tmp_path: Path) -> Settings:
    return Settings(data_root=tmp_path / "data", batch_rows=1, parser_version="test-parser")


def test_ingest_promotes_validated_snapshot_and_persists_manifest(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    result = ingest_snapshot("fixture-2025-06", _sources(tmp_path), settings)
    layout = SnapshotLayout(settings.data_root, "fixture-2025-06")

    assert result.status is RunStatus.PROMOTED
    assert result.promoted_path is not None
    assert Path(result.promoted_path).exists()
    assert not layout.staging.exists()
    assert load_manifest(layout.manifest).model_dump() == result.model_dump()
    assert load_manifest(layout.current_manifest).snapshot_id == "fixture-2025-06"
    assert [table.table for table in result.tables] == list(TableKind)
    assert all(table.quality[0].level is QualityLevel.WARNING for table in result.tables)
    assert all(
        Path(table.silver_path).parent == Path(result.promoted_path) for table in result.tables
    )


def test_failed_snapshot_keeps_staging_evidence_and_current_pointer(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    first = ingest_snapshot("fixture-2025-06", _sources(tmp_path / "first"), settings)
    first_layout = SnapshotLayout(settings.data_root, first.snapshot_id)

    failed = ingest_snapshot(
        "fixture-2025-07", _sources(tmp_path / "failed", narrative_header=False), settings
    )
    failed_layout = SnapshotLayout(settings.data_root, failed.snapshot_id)

    assert failed.status is RunStatus.FAILED
    assert failed_layout.staging.exists()
    assert any(failed_layout.rejects.glob("*.jsonl"))
    assert load_manifest(failed_layout.manifest).status is RunStatus.FAILED
    assert load_manifest(first_layout.current_manifest).snapshot_id == first.snapshot_id
    assert first_layout.promoted.exists()


def test_unparseable_normalization_dates_are_nonblocking_table_warnings(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    result = ingest_snapshot(
        "fixture-2025-06",
        _sources(tmp_path, first_master_date="13/40/2025"),
        settings,
    )
    master = next(table for table in result.tables if table.table is TableKind.MASTER)

    assert result.status is RunStatus.PROMOTED
    assert master.quality[0].check == "date_parse_failure_count"
    assert master.quality[0].level is QualityLevel.WARNING
    assert master.quality[0].passed is False
    assert master.quality[0].metrics["numerator"] == 1


def test_identical_checksum_rerun_returns_promoted_manifest_without_rewriting_parquet(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    sources = _sources(tmp_path)
    first = ingest_snapshot("fixture-2025-06", sources, settings)
    promoted = Path(first.promoted_path or "")
    before = {path.name: path.stat().st_mtime_ns for path in promoted.glob("*.parquet")}

    rerun = ingest_snapshot("fixture-2025-06", sources, settings)

    assert rerun.model_dump() == first.model_dump()
    assert {path.name: path.stat().st_mtime_ns for path in promoted.glob("*.parquet")} == before


def test_reusing_snapshot_id_with_different_source_checksum_raises_conflict(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    sources = _sources(tmp_path)
    ingest_snapshot("fixture-2025-06", sources, settings)
    changed_sources = _sources(tmp_path / "changed", second_master_event="E")

    with pytest.raises(SnapshotConflict, match="different source checksums"):
        ingest_snapshot("fixture-2025-06", changed_sources, settings)


def test_cleanup_failure_does_not_rollback_an_already_published_pointer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = _settings(tmp_path)

    def fail_cleanup(path: Path) -> None:
        raise OSError("simulated cleanup failure")

    monkeypatch.setattr(pipeline, "rmtree", fail_cleanup)
    result = ingest_snapshot("fixture-2025-06", _sources(tmp_path), settings)
    layout = SnapshotLayout(settings.data_root, result.snapshot_id)

    assert result.status is RunStatus.PROMOTED
    assert Path(result.promoted_path or "").exists()
    assert load_manifest(layout.current_manifest).status is RunStatus.PROMOTED


@pytest.mark.parametrize("snapshot_id", ("../escape", "contains space", "slash/name", ""))
def test_snapshot_id_must_stay_inside_snapshot_layout(tmp_path: Path, snapshot_id: str) -> None:
    with pytest.raises(ValueError, match="snapshot_id"):
        ingest_snapshot(snapshot_id, (), _settings(tmp_path))
