from dataclasses import FrozenInstanceError
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest
from pydantic import ValidationError

from maude.domain.enums import QualityLevel, RunStatus, TableKind
from maude.domain.models import (
    ParseStats,
    QualityResult,
    SnapshotResult,
    SourceIdentity,
    TableResult,
)
from maude.storage.layout import SnapshotLayout


def test_snapshot_layout_keeps_staging_separate_from_promoted(tmp_path: Path) -> None:
    layout = SnapshotLayout(data_root=tmp_path, snapshot_id="2025-06-local")
    assert layout.staging == tmp_path / "staging" / "2025-06-local"
    assert layout.promoted == tmp_path / "silver" / "2025-06-local"
    assert layout.manifest == tmp_path / "manifests" / "2025-06-local.json"
    assert layout.current_manifest == tmp_path / "manifests" / "current.json"
    assert layout.bronze == layout.staging / "bronze"
    assert layout.silver == layout.staging / "silver"
    assert layout.rejects == layout.staging / "rejects"
    assert layout.staging != layout.promoted


def test_snapshot_layout_is_immutable(tmp_path: Path) -> None:
    layout = SnapshotLayout(data_root=tmp_path, snapshot_id="x")
    with pytest.raises(FrozenInstanceError):
        layout.snapshot_id = "y"  # type: ignore[misc]


def test_shared_enums_use_contract_values() -> None:
    assert [item.value for item in TableKind] == ["master", "device", "patient", "narrative"]
    assert [item.value for item in RunStatus] == ["running", "failed", "validated", "promoted"]
    assert [item.value for item in QualityLevel] == ["blocking", "warning"]


def test_shared_models_validate_bounds_and_preserve_fields() -> None:
    source = SourceIdentity(
        path="/input.csv",
        filename="input.csv",
        sha256="abc",
        byte_size=3,
        archive_member=None,
        encoding="utf-8",
    )
    stats = ParseStats(rows_seen=2, rows_accepted=1, rows_rejected=1, columns=("a",))
    quality = QualityResult(check="rows", level=QualityLevel.WARNING, passed=True, message="ok")
    table = TableResult(
        table=TableKind.MASTER,
        source=source,
        bronze_path="bronze.parquet",
        silver_path="silver.parquet",
        reject_path="rejects.parquet",
        stats=stats,
        quality=[quality],
    )
    result = SnapshotResult(
        run_id=uuid4(),
        snapshot_id="snapshot",
        started_at=datetime.now(UTC),
        finished_at=None,
        status=RunStatus.RUNNING,
        tables=[table],
        quality=[quality],
        promoted_path=None,
        parser_version="1.0.0",
    )
    assert result.tables[0].table is TableKind.MASTER
    assert result.quality[0].check == "rows"
    with pytest.raises(ValidationError):
        ParseStats(rows_seen=-1, rows_accepted=0, rows_rejected=0, columns=())


@pytest.mark.parametrize("field", ["started_at", "finished_at"])
def test_snapshot_result_rejects_naive_run_timestamps(field: str) -> None:
    values = dict(
        run_id=uuid4(),
        snapshot_id="snapshot",
        started_at=datetime.now(UTC),
        finished_at=datetime.now(UTC),
        status=RunStatus.RUNNING,
        tables=[],
        quality=[],
        promoted_path=None,
        parser_version="1.0.0",
    )
    values[field] = datetime(2025, 1, 1)
    with pytest.raises(ValidationError, match="timezone-aware"):
        SnapshotResult(**values)
