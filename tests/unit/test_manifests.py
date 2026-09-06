from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import maude.ingestion.manifests as manifests
from maude.domain.enums import RunStatus
from maude.domain.models import SnapshotResult
from maude.ingestion.manifests import load_manifest, write_manifest


def _snapshot() -> SnapshotResult:
    return SnapshotResult(
        run_id=uuid4(),
        snapshot_id="fixture-2025-06",
        started_at=datetime.now(UTC),
        finished_at=None,
        status=RunStatus.RUNNING,
        tables=[],
        quality=[],
        promoted_path=None,
        parser_version="test-parser",
    )


def test_manifest_replace_fsyncs_its_parent_directory(tmp_path: Path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    synced: list[Path] = []
    monkeypatch.setattr(manifests, "fsync_directory", synced.append)
    path = tmp_path / "manifests" / "snapshot.json"

    write_manifest(path, _snapshot())

    assert synced == [path.parent]
    assert load_manifest(path).snapshot_id == "fixture-2025-06"
