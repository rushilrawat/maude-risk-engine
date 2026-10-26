import json
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from maude.domain.enums import RunStatus
from maude.domain.models import SnapshotResult
from maude.ingestion.manifests import write_manifest
from maude.storage.layout import SnapshotLayout


def _write_promoted_snapshot(data_root: Path, *, status: RunStatus = RunStatus.PROMOTED) -> Path:
    snapshot_id = "fixture-2025-06"
    layout = SnapshotLayout(data_root, snapshot_id)
    layout.promoted.mkdir(parents=True)
    pq.write_table(
        pa.table(
            {
                "mdr_report_key": ["1", "2"],
                "event_type": ["D", "M"],
                "date_received": ["2025-01-02", "2025-01-03"],
                "dataset_snapshot_id": [snapshot_id, snapshot_id],
            }
        ),
        layout.promoted / "reports.parquet",
    )
    pq.write_table(
        pa.table(
            {
                "mdr_report_key": ["1"],
                "device_sequence_no": ["1"],
                "device_report_product_code": ["ABC"],
                "dataset_snapshot_id": [snapshot_id],
            }
        ),
        layout.promoted / "devices.parquet",
    )
    pq.write_table(
        pa.table(
            {
                "mdr_report_key": ["1"],
                "patient_sequence_number": ["1"],
                "dataset_snapshot_id": [snapshot_id],
            }
        ),
        layout.promoted / "patients.parquet",
    )
    pq.write_table(
        pa.table(
            {
                "mdr_report_key": ["1", "1", "2"],
                "mdr_text_key": ["10", "11", "20"],
                "text_type_code": ["N", "N", "D"],
                "foi_text": ["first\n narrative", "second", "   \t"],
                "_source_filename": ["narrative.txt"] * 3,
                "_source_line_number": ["2", "3", "4"],
                "dataset_snapshot_id": [snapshot_id] * 3,
            }
        ),
        layout.promoted / "narratives.parquet",
    )
    write_manifest(
        layout.manifest,
        SnapshotResult(
            run_id=uuid4(),
            snapshot_id=snapshot_id,
            started_at=datetime.now(UTC),
            finished_at=datetime.now(UTC),
            status=status,
            tables=[],
            quality=[],
            promoted_path=str(layout.promoted),
            parser_version="test-parser",
        ),
    )
    return layout.promoted


def test_build_corpus_streams_documents_and_writes_versioned_manifest(tmp_path: Path) -> None:
    from maude.features.corpus import build_modeling_corpus

    snapshot_path = _write_promoted_snapshot(tmp_path)

    result = build_modeling_corpus(tmp_path, "fixture-2025-06", batch_rows=1)

    assert result.snapshot_id == "fixture-2025-06"
    assert result.reports_seen == 2
    assert result.documents_written == 1
    assert result.narratives_written == 2
    assert result.blank_narratives_omitted == 1
    assert Path(result.promoted_snapshot_path) == snapshot_path
    output_path = Path(result.output_path)
    records = [json.loads(line) for line in output_path.read_text().splitlines()]
    assert records[0]["report_id"] == "1"
    assert records[0]["modeling_text"] == "first narrative\n\nsecond"
    assert records[0]["narratives"][0]["source_line_number"] == 2
    assert result.manifest_path and Path(result.manifest_path).is_file()


def test_build_corpus_rerun_is_idempotent(tmp_path: Path) -> None:
    from maude.features.corpus import build_modeling_corpus

    _write_promoted_snapshot(tmp_path)

    first = build_modeling_corpus(tmp_path, "fixture-2025-06", batch_rows=1)
    first_bytes = Path(first.output_path).read_bytes()
    second = build_modeling_corpus(tmp_path, "fixture-2025-06", batch_rows=2)

    assert Path(second.output_path).read_bytes() == first_bytes
    assert second.model_dump() == first.model_dump()


def test_build_corpus_refuses_snapshot_without_promoted_status(tmp_path: Path) -> None:
    from maude.features.corpus import build_modeling_corpus

    _write_promoted_snapshot(tmp_path, status=RunStatus.RUNNING)

    with pytest.raises(ValueError, match="not promoted"):
        build_modeling_corpus(tmp_path, "fixture-2025-06", batch_rows=1)
