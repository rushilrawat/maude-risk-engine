import json
from pathlib import Path
from tempfile import TemporaryDirectory
from zipfile import ZIP_DEFLATED, ZipFile

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from hypothesis import given
from hypothesis import strategies as st

import maude.ingestion.archive as archive
import maude.ingestion.parser as parser
from maude.ingestion.parser import parse_to_bronze


def test_parser_streams_valid_rows_and_records_bad_field_count(tmp_path: Path) -> None:
    source = tmp_path / "foitext.zip"
    content = (
        "MDR_REPORT_KEY|MDR_TEXT_KEY|TEXT_TYPE_CODE|DATE_REPORT|FOI_TEXT\r\n"
        "1|10|N|01/02/2025|Pump stopped unexpectedly\r\n"
        "2|11|N|01/03/2025\r\n"
    )
    with ZipFile(source, "w", ZIP_DEFLATED) as handle:
        handle.writestr("foitext.txt", content)

    result = parse_to_bronze(
        source,
        tmp_path / "bronze.parquet",
        tmp_path / "rejects.jsonl",
        batch_rows=1,
    )

    assert result.stats.rows_seen == 2
    assert result.stats.rows_accepted == 1
    assert result.stats.rows_rejected == 1
    assert pq.read_table(tmp_path / "bronze.parquet").to_pylist()[0]["MDR_REPORT_KEY"] == "1"
    assert '"reason":"field_count"' in (tmp_path / "rejects.jsonl").read_text()


def test_parser_writes_only_bounded_batches(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "patient.txt"
    source.write_text(
        "MDR_REPORT_KEY|PATIENT_SEQUENCE_NUMBER\n"
        "1|1\n"
        "2|2\n"
        "3|3\n"
        "4|4\n"
        "5|5\n",
        encoding="utf-8",
    )
    batch_lengths: list[int] = []
    original_write_batch = parser._write_batch

    def spy_write_batch(writer: object, schema: object, rows: object) -> None:
        batch_lengths.append(len(rows))  # type: ignore[arg-type]
        original_write_batch(writer, schema, rows)  # type: ignore[arg-type]

    monkeypatch.setattr(parser, "_write_batch", spy_write_batch)

    parse_to_bronze(source, tmp_path / "bronze.parquet", tmp_path / "rejects.jsonl", batch_rows=2)

    assert batch_lengths == [2, 2, 1]


def test_parser_rejects_blank_business_key_fields(tmp_path: Path) -> None:
    source = tmp_path / "patient.txt"
    source.write_text(
        "MDR_REPORT_KEY|PATIENT_SEQUENCE_NUMBER\n"
        "|1\n"
        "2|\n",
        encoding="utf-8",
    )

    result = parse_to_bronze(
        source,
        tmp_path / "bronze.parquet",
        tmp_path / "rejects.jsonl",
        batch_rows=10,
    )

    assert result.stats.rows_accepted == 0
    assert result.stats.rows_rejected == 2
    assert (tmp_path / "rejects.jsonl").read_text(encoding="utf-8").count(
        '"reason":"missing_business_key"'
    ) == 2


def test_parser_checks_business_keys_against_normalized_original_headers(tmp_path: Path) -> None:
    source = tmp_path / "patient.txt"
    source.write_text(
        "MDR REPORT KEY|PATIENT SEQUENCE NUMBER\n"
        "|1\n",
        encoding="utf-8",
    )

    result = parse_to_bronze(
        source,
        tmp_path / "bronze.parquet",
        tmp_path / "rejects.jsonl",
        batch_rows=1,
    )

    assert result.stats.rows_rejected == 1
    assert '"reason":"missing_business_key"' in (
        tmp_path / "rejects.jsonl"
    ).read_text(encoding="utf-8")


def test_parser_uses_the_configured_version_for_accepted_and_rejected_provenance(
    tmp_path: Path,
) -> None:
    source = tmp_path / "patient.txt"
    source.write_text("MDR_REPORT_KEY|PATIENT_SEQUENCE_NUMBER\n1\n", encoding="utf-8")

    result = parse_to_bronze(
        source,
        tmp_path / "bronze.parquet",
        tmp_path / "rejects.jsonl",
        batch_rows=1,
        parser_version="review-version-2026.09",
    )

    rejected = json.loads((tmp_path / "rejects.jsonl").read_text(encoding="utf-8"))
    metadata = pq.read_table(tmp_path / "bronze.parquet").schema.metadata
    assert rejected["source_snapshot_sha256"] == result.source.sha256
    assert rejected["source_filename"] == "patient.txt"
    assert rejected["parser_version"] == "review-version-2026.09"
    assert metadata is not None
    assert metadata[b"parser_version"] == b"review-version-2026.09"


def test_parser_reuses_its_initial_source_inspection_for_streaming(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "patient.txt"
    source.write_text("MDR_REPORT_KEY|PATIENT_SEQUENCE_NUMBER\n1|1\n", encoding="utf-8")
    initial_inspection = archive.inspect_source(source)
    parser_inspections: list[Path] = []

    def inspect_once(path: Path) -> object:
        parser_inspections.append(path)
        return initial_inspection

    def fail_second_inspection(path: Path) -> object:
        raise AssertionError(f"source inspected twice: {path}")

    monkeypatch.setattr(parser, "inspect_source", inspect_once)
    monkeypatch.setattr(archive, "inspect_source", fail_second_inspection)

    result = parse_to_bronze(
        source,
        tmp_path / "bronze.parquet",
        tmp_path / "rejects.jsonl",
        batch_rows=1,
    )

    assert parser_inspections == [source]
    assert result.source.sha256 == initial_inspection.sha256
    assert pq.read_table(tmp_path / "bronze.parquet").to_pylist()[0]["MDR_REPORT_KEY"] == "1"


def test_parser_emits_empty_parquet_with_full_schema(tmp_path: Path) -> None:
    source = tmp_path / "patient.txt"
    source.write_text("MDR_REPORT_KEY|PATIENT_SEQUENCE_NUMBER\n", encoding="utf-8")
    bronze_path = tmp_path / "bronze.parquet"

    result = parse_to_bronze(source, bronze_path, tmp_path / "rejects.jsonl", batch_rows=1)

    bronze = pq.read_table(bronze_path)
    assert result.stats.rows_accepted == 0
    assert bronze.to_pylist() == []
    assert bronze.schema.names == [
        "MDR_REPORT_KEY",
        "PATIENT_SEQUENCE_NUMBER",
        "_source_line_number",
        "_source_snapshot_sha256",
        "_source_filename",
    ]
    assert all(field.type == pa.string() for field in bronze.schema)


def test_parser_records_source_attribution_and_encoding(tmp_path: Path) -> None:
    source = tmp_path / "patient.zip"
    with ZipFile(source, "w", ZIP_DEFLATED) as handle:
        handle.writestr("patient.txt", "MDR_REPORT_KEY|PATIENT_SEQUENCE_NUMBER\n1|2\n")

    result = parse_to_bronze(
        source,
        tmp_path / "bronze.parquet",
        tmp_path / "rejects.jsonl",
        batch_rows=1,
    )

    accepted = pq.read_table(tmp_path / "bronze.parquet").to_pylist()
    assert result.source.archive_member == "patient.txt"
    assert result.source.encoding == "utf-8"
    assert accepted == [
        {
            "MDR_REPORT_KEY": "1",
            "PATIENT_SEQUENCE_NUMBER": "2",
            "_source_line_number": "2",
            "_source_snapshot_sha256": result.source.sha256,
            "_source_filename": "patient.txt",
        }
    ]


def test_parser_keeps_final_outputs_when_batch_write_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "patient.txt"
    source.write_text("MDR_REPORT_KEY|PATIENT_SEQUENCE_NUMBER\n1|1\n", encoding="utf-8")
    bronze_path = tmp_path / "bronze.parquet"
    reject_path = tmp_path / "rejects.jsonl"
    bronze_path.write_bytes(b"previous bronze")
    reject_path.write_text("previous rejects", encoding="utf-8")

    def fail_write_batch(writer: object, schema: object, rows: object) -> None:
        raise OSError("simulated write failure")

    monkeypatch.setattr(parser, "_write_batch", fail_write_batch)

    with pytest.raises(OSError, match="simulated write failure"):
        parse_to_bronze(source, bronze_path, reject_path, batch_rows=1)

    assert bronze_path.read_bytes() == b"previous bronze"
    assert reject_path.read_text(encoding="utf-8") == "previous rejects"


@given(valid_rows=st.lists(st.booleans(), max_size=20))
def test_parser_conserves_rows_and_writes_each_valid_row_once(
    valid_rows: list[bool],
) -> None:
    with TemporaryDirectory() as directory:
        temporary_path = Path(directory)
        source = temporary_path / "patient.txt"
        source.write_text(
            "MDR_REPORT_KEY|PATIENT_SEQUENCE_NUMBER\n"
            + "".join(
                f"{index}|{index}\n" if is_valid else f"{index}\n"
                for index, is_valid in enumerate(valid_rows, start=1)
            ),
            encoding="utf-8",
        )

        result = parse_to_bronze(
            source,
            temporary_path / "bronze.parquet",
            temporary_path / "rejects.jsonl",
            batch_rows=2,
        )

        bronze_rows = pq.read_table(temporary_path / "bronze.parquet").to_pylist()
    expected_keys = [str(index) for index, is_valid in enumerate(valid_rows, start=1) if is_valid]
    assert result.stats.rows_seen == result.stats.rows_accepted + result.stats.rows_rejected
    assert [row["MDR_REPORT_KEY"] for row in bronze_rows] == expected_keys
