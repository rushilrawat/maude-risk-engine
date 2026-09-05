from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from maude.storage.duckdb import (
    ReportNotFound,
    SnapshotSchemaError,
    fetch_report_document,
    open_snapshot,
)


def _write_snapshot(root: Path) -> Path:
    root.mkdir()
    snapshot_id = "fixture-2025-06"
    pq.write_table(
        pa.table(
            {
                "mdr_report_key": ["1", "2"],
                "event_type": ["D", "M"],
                "date_received": [date(2025, 1, 2), date(2025, 1, 3)],
                "dataset_snapshot_id": [snapshot_id, snapshot_id],
            }
        ),
        root / "reports.parquet",
    )
    pq.write_table(
        pa.table(
            {
                "mdr_report_key": ["1", "1", "1"],
                "device_sequence_no": ["2", "1", "1"],
                "device_report_product_code": ["ZZZ", "ABC", "ABC"],
                "brand_name": ["Bravo", "Alpha", "Alpha"],
                "manufacturer_d_name": ["Mfg B", "Mfg A", "Mfg A"],
                "dataset_snapshot_id": [snapshot_id] * 3,
            }
        ),
        root / "devices.parquet",
    )
    pq.write_table(
        pa.table(
            {
                "mdr_report_key": ["1"],
                "patient_sequence_number": ["1"],
                "dataset_snapshot_id": [snapshot_id],
            }
        ),
        root / "patients.parquet",
    )
    pq.write_table(
        pa.table(
            {
                "mdr_report_key": ["1", "1", "1"],
                "mdr_text_key": ["11", "10", "10"],
                "text_type_code": ["N", "N", "N"],
                "foi_text": ["second", "first", "first duplicate"],
                "_source_filename": ["narrative.txt", "narrative.txt", "other.txt"],
                "_source_line_number": ["5", "2", "2"],
                "dataset_snapshot_id": [snapshot_id] * 3,
            }
        ),
        root / "narratives.parquet",
    )
    return root


def test_report_document_preserves_narrative_evidence_ids(tmp_path: Path) -> None:
    connection = open_snapshot(_write_snapshot(tmp_path / "promoted"))
    try:
        document = fetch_report_document(connection, "1")

        assert document.report_id == "1"
        assert document.reported_event_type == "D"
        assert document.date_received == date(2025, 1, 2)
        assert document.product_codes == ("ABC", "ZZZ")
        assert document.brand_names == ("Alpha", "Bravo")
        assert document.manufacturers == ("Mfg A", "Mfg B")
        assert [(item.evidence_id, item.text) for item in document.narratives] == [
            ("narrative:10", "first"),
            ("narrative:11", "second"),
        ]
        assert document.narratives[0].source_filename == "narrative.txt"
        assert document.narratives[0].source_line_number == 2
        assert document.dataset_snapshot_id == "fixture-2025-06"
    finally:
        connection.close()


def test_fetch_report_document_raises_for_unknown_report(tmp_path: Path) -> None:
    connection = open_snapshot(_write_snapshot(tmp_path / "promoted"))
    try:
        with pytest.raises(ReportNotFound, match="report_id 'missing' was not found"):
            fetch_report_document(connection, "missing")
    finally:
        connection.close()


def test_report_with_no_child_rows_has_empty_metadata_and_narratives(tmp_path: Path) -> None:
    connection = open_snapshot(_write_snapshot(tmp_path / "promoted"))
    try:
        document = fetch_report_document(connection, "2")

        assert document.product_codes == ()
        assert document.brand_names == ()
        assert document.manufacturers == ()
        assert document.narratives == ()
    finally:
        connection.close()


def test_open_snapshot_rejects_missing_artifact_with_actionable_error(tmp_path: Path) -> None:
    root = _write_snapshot(tmp_path / "promoted")
    (root / "patients.parquet").unlink()

    with pytest.raises(FileNotFoundError, match=r"patients\.parquet"):
        open_snapshot(root)


def test_open_snapshot_rejects_mismatched_child_snapshot_id(tmp_path: Path) -> None:
    root = _write_snapshot(tmp_path / "promoted")
    narrative_path = root / "narratives.parquet"
    narrative_table = pq.read_table(narrative_path).to_pylist()
    for row in narrative_table:
        row["dataset_snapshot_id"] = "fixture-2025-07"
    pq.write_table(pa.Table.from_pylist(narrative_table), narrative_path)

    with pytest.raises(SnapshotSchemaError, match=r"narratives.*fixture-2025-07"):
        open_snapshot(root)


def test_open_snapshot_rejects_null_or_multiple_snapshot_ids(tmp_path: Path) -> None:
    root = _write_snapshot(tmp_path / "promoted")
    report_path = root / "reports.parquet"
    report_rows = pq.read_table(report_path).to_pylist()
    report_rows[0]["dataset_snapshot_id"] = None
    pq.write_table(pa.Table.from_pylist(report_rows), report_path)

    with pytest.raises(SnapshotSchemaError, match=r"reports.*non-null"):
        open_snapshot(root)

    report_rows[0]["dataset_snapshot_id"] = "fixture-2025-06"
    report_rows[1]["dataset_snapshot_id"] = "fixture-2025-07"
    pq.write_table(pa.Table.from_pylist(report_rows), report_path)

    with pytest.raises(SnapshotSchemaError, match=r"reports.*multiple"):
        open_snapshot(root)


def test_open_snapshot_allows_empty_child_artifacts(tmp_path: Path) -> None:
    root = _write_snapshot(tmp_path / "promoted")
    for filename in ("devices.parquet", "patients.parquet", "narratives.parquet"):
        path = root / filename
        pq.write_table(pq.read_table(path).slice(0, 0), path)

    connection = open_snapshot(root)
    try:
        document = fetch_report_document(connection, "1")
        assert document.product_codes == ()
        assert document.narratives == ()
    finally:
        connection.close()


def test_open_snapshot_allows_all_empty_artifacts_and_fetch_is_not_found(tmp_path: Path) -> None:
    root = _write_snapshot(tmp_path / "promoted")
    for path in root.glob("*.parquet"):
        pq.write_table(pq.read_table(path).slice(0, 0), path)

    connection = open_snapshot(root)
    try:
        with pytest.raises(ReportNotFound):
            fetch_report_document(connection, "1")
    finally:
        connection.close()


@pytest.mark.parametrize("blank_id", ["", "   "])
def test_open_snapshot_rejects_blank_snapshot_ids(tmp_path: Path, blank_id: str) -> None:
    root = _write_snapshot(tmp_path / "promoted")
    report_path = root / "reports.parquet"
    report_rows = pq.read_table(report_path).to_pylist()
    for row in report_rows:
        row["dataset_snapshot_id"] = blank_id
    pq.write_table(pa.Table.from_pylist(report_rows), report_path)

    with pytest.raises(SnapshotSchemaError, match=r"reports.*blank"):
        open_snapshot(root)


def test_open_snapshot_rejects_duplicate_master_report_ids(tmp_path: Path) -> None:
    root = _write_snapshot(tmp_path / "promoted")
    report_path = root / "reports.parquet"
    report_rows = pq.read_table(report_path).to_pylist()
    report_rows.append(
        {
            "mdr_report_key": "1",
            "event_type": "E",
            "date_received": date(2025, 1, 9),
            "dataset_snapshot_id": "fixture-2025-06",
        }
    )
    pq.write_table(pa.Table.from_pylist(report_rows), report_path)

    with pytest.raises(SnapshotSchemaError, match=r"duplicate.*mdr_report_key.*1"):
        open_snapshot(root)


def test_open_snapshot_supports_special_characters_in_path(tmp_path: Path) -> None:
    root = _write_snapshot(tmp_path / "promoted O'Reilly [v1]")
    connection = open_snapshot(root)
    try:
        assert fetch_report_document(connection, "1").report_id == "1"
    finally:
        connection.close()


def test_open_snapshot_uses_bounded_validation_memory_and_keeps_connection_usable(
    tmp_path: Path,
) -> None:
    connection = open_snapshot(_write_snapshot(tmp_path / "promoted"))
    try:
        memory_limit = connection.execute("SELECT current_setting('memory_limit')").fetchone()[0]
        assert float(str(memory_limit).split()[0]) <= 64
        assert connection.execute("SELECT COUNT(*) FROM reports").fetchone() == (2,)
    finally:
        connection.close()


def test_duplicate_report_validation_handles_many_keys_under_bound(tmp_path: Path) -> None:
    root = _write_snapshot(tmp_path / "promoted")
    row_count = 100_001
    report_path = root / "reports.parquet"
    pq.write_table(
        pa.table(
            {
                "mdr_report_key": [str(index) for index in range(row_count - 1)] + ["0"],
                "event_type": ["D"] * row_count,
                "date_received": [date(2025, 1, 2)] * row_count,
                "dataset_snapshot_id": ["fixture-2025-06"] * row_count,
            }
        ),
        report_path,
    )

    with pytest.raises(SnapshotSchemaError, match=r"duplicate.*mdr_report_key"):
        open_snapshot(root)
