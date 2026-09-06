from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from maude.domain.enums import TableKind
from maude.ingestion.normalize import NormalizationError, normalize_bronze
from maude.ingestion.schemas import spec_for

PROVENANCE = {
    "_source_line_number": ["2"],
    "_source_snapshot_sha256": ["source-sha"],
    "_source_filename": ["source.txt"],
}


def _write_bronze(path: Path, fields: dict[str, list[str]]) -> None:
    """Write a small Bronze fixture with the parser's provenance contract."""
    pq.write_table(pa.table(fields | PROVENANCE), path)


def test_normalize_master_preserves_raw_date_and_parses_typed_date(tmp_path: Path) -> None:
    bronze = tmp_path / "master.parquet"
    pq.write_table(
        pa.table(
            {
                "MDR_REPORT_KEY": ["1", "2"],
                "DATE_RECEIVED": ["01/02/2025", ""],
                "EVENT_TYPE": ["D", "NI"],
                "_source_line_number": ["2", "3"],
                "_source_snapshot_sha256": ["abc", "abc"],
                "_source_filename": ["mdrfoi.txt", "mdrfoi.txt"],
            }
        ),
        bronze,
    )

    result = normalize_bronze(
        bronze, tmp_path / "silver.parquet", spec_for(TableKind.MASTER), "local-2025-06"
    )
    rows = pq.read_table(result.silver_path).to_pylist()

    assert rows[0]["date_received_raw"] == "01/02/2025"
    assert rows[0]["date_received"] == date(2025, 1, 2)
    assert rows[1]["date_received_raw"] is None
    assert rows[1]["date_received"] is None
    assert rows[0]["event_type"] == "D"
    assert rows[1]["event_type"] == "NI"
    assert rows[0]["dataset_snapshot_id"] == "local-2025-06"
    assert result.date_parse_failure_count == 0


def test_normalize_preserves_malformed_date_raw_value_and_counts_failure(tmp_path: Path) -> None:
    bronze = tmp_path / "master.parquet"
    _write_bronze(
        bronze,
        {
            "MDR_REPORT_KEY": ["1"],
            "DATE_RECEIVED": ["13/40/2025"],
            "EVENT_TYPE": ["*"],
        },
    )

    result = normalize_bronze(
        bronze, tmp_path / "silver.parquet", spec_for(TableKind.MASTER), "local-2025-06"
    )
    row = pq.read_table(result.silver_path).to_pylist()[0]

    assert row["date_received_raw"] == "13/40/2025"
    assert row["date_received"] is None
    assert row["event_type"] == "*"
    assert result.date_parse_failure_count == 1


def test_normalize_preserves_nonempty_whitespace_while_parsing_trimmed_dates(
    tmp_path: Path,
) -> None:
    bronze = tmp_path / "master.parquet"
    pq.write_table(
        pa.table(
            {
                "MDR_REPORT_KEY": ["1", "1", "1"],
                "DATE_RECEIVED": [" 01/02/2025 ", "01/02/2025", "  "],
                "EVENT_TYPE": [" NI ", "NI", "   "],
                "_source_line_number": ["2", "3", "4"],
                "_source_snapshot_sha256": ["abc", "abc", "abc"],
                "_source_filename": ["mdrfoi.txt", "mdrfoi.txt", "mdrfoi.txt"],
            }
        ),
        bronze,
    )

    normalize_bronze(bronze, tmp_path / "silver.parquet", spec_for(TableKind.MASTER), "snapshot-42")
    padded, canonical, blank = pq.read_table(tmp_path / "silver.parquet").to_pylist()

    assert padded["event_type"] == " NI "
    assert padded["date_received_raw"] == " 01/02/2025 "
    assert padded["date_received"] == date(2025, 1, 2)
    assert blank["event_type"] is None
    assert blank["date_received_raw"] is None
    assert blank["date_received"] is None
    assert padded["record_content_hash"] != canonical["record_content_hash"]


@pytest.mark.parametrize(
    ("kind", "fields", "key_columns", "date_column", "expected_date"),
    [
        (
            TableKind.MASTER,
            {"MDR_REPORT_KEY": ["1"], "DATE_RECEIVED": ["01/02/2025"], "EVENT_TYPE": ["D"]},
            ("mdr_report_key",),
            "date_received",
            date(2025, 1, 2),
        ),
        (
            TableKind.DEVICE,
            {
                "MDR_REPORT_KEY": ["2"],
                "DEVICE_SEQUENCE_NO": ["1"],
                "DEVICE_REPORT_PRODUCT_CODE": ["ABC"],
                "DATE_RECEIVED": ["2025/02/03"],
            },
            ("mdr_report_key", "device_sequence_no"),
            "date_received",
            date(2025, 2, 3),
        ),
        (
            TableKind.PATIENT,
            {
                "MDR_REPORT_KEY": ["3"],
                "PATIENT_SEQUENCE_NUMBER": ["1"],
                "DATE_RECEIVED": ["01/04/2025"],
            },
            ("mdr_report_key", "patient_sequence_number"),
            "date_received",
            date(2025, 1, 4),
        ),
        (
            TableKind.NARRATIVE,
            {
                "MDR_REPORT_KEY": ["4"],
                "MDR_TEXT_KEY": ["44"],
                "TEXT_TYPE_CODE": ["D"],
                "FOI_TEXT": ["narrative"],
                "DATE_REPORT": ["2025/02/05"],
            },
            ("mdr_text_key",),
            "date_report",
            date(2025, 2, 5),
        ),
    ],
)
def test_normalize_all_table_kinds_keeps_keys_and_provenance(
    tmp_path: Path,
    kind: TableKind,
    fields: dict[str, list[str]],
    key_columns: tuple[str, ...],
    date_column: str,
    expected_date: date,
) -> None:
    bronze = tmp_path / f"{kind.value}.parquet"
    _write_bronze(bronze, fields)

    result = normalize_bronze(bronze, tmp_path / "silver.parquet", spec_for(kind), "snapshot-42")
    row = pq.read_table(result.silver_path).to_pylist()[0]

    assert all(row[column] is not None for column in key_columns)
    assert row[date_column] == expected_date
    assert row["_source_line_number"] == "2"
    assert row["_source_snapshot_sha256"] == "source-sha"
    assert row["_source_filename"] == "source.txt"
    assert row["dataset_snapshot_id"] == "snapshot-42"


def test_normalize_rejects_colliding_canonical_names_without_replacing_output(
    tmp_path: Path,
) -> None:
    bronze = tmp_path / "master.parquet"
    _write_bronze(
        bronze,
        {
            "MDR_REPORT_KEY": ["1"],
            "MDR REPORT KEY": ["duplicate"],
            "DATE_RECEIVED": ["01/02/2025"],
            "EVENT_TYPE": ["D"],
        },
    )
    silver = tmp_path / "silver.parquet"
    silver.write_bytes(b"last successful output")

    with pytest.raises(NormalizationError, match="collision"):
        normalize_bronze(bronze, silver, spec_for(TableKind.MASTER), "snapshot-42")

    assert silver.read_bytes() == b"last successful output"


def test_normalize_hash_distinguishes_field_boundaries_and_nulls(tmp_path: Path) -> None:
    bronze = tmp_path / "master.parquet"
    pq.write_table(
        pa.table(
            {
                "MDR_REPORT_KEY": ["1", "1", "1"],
                "DATE_RECEIVED": ["01/02/2025", "01/02/2025", "01/02/2025"],
                "EVENT_TYPE": ["ab", "a", "<NULL>"],
                "EXTRA": ["c", "bc", None],
                "_source_line_number": ["2", "3", "4"],
                "_source_snapshot_sha256": ["abc", "abc", "abc"],
                "_source_filename": ["mdrfoi.txt", "mdrfoi.txt", "mdrfoi.txt"],
            }
        ),
        bronze,
    )

    normalize_bronze(bronze, tmp_path / "silver.parquet", spec_for(TableKind.MASTER), "snapshot-42")
    rows = pq.read_table(tmp_path / "silver.parquet").to_pylist()

    assert len({row["record_content_hash"] for row in rows}) == 3
    assert all(len(row["record_content_hash"]) == 64 for row in rows)
