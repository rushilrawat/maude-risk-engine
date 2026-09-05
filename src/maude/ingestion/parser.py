"""Streaming conversion of validated MAUDE source records into Bronze Parquet."""

import csv
import json
import os
import tempfile
from collections.abc import Sequence
from pathlib import Path
from typing import TextIO

import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]

from maude.domain.models import BronzeResult, InspectedSource, ParseStats, SourceIdentity
from maude.ingestion.archive import inspect_source, open_source_text
from maude.ingestion.encoding import select_encoding
from maude.ingestion.schemas import TableSpec, detect_table, normalize_column

PROVENANCE_COLUMNS = (
    "_source_line_number",
    "_source_snapshot_sha256",
    "_source_filename",
)


def _temporary_sibling(path: Path) -> Path:
    """Create and return a uniquely named temporary file next to *path*."""
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    os.close(descriptor)
    return Path(temporary_name)


def _write_batch(
    writer: pq.ParquetWriter,
    schema: pa.Schema,
    rows: Sequence[dict[str, str]],
) -> None:
    """Write a bounded group of rows through the sole Parquet writer."""
    writer.write_table(pa.Table.from_pylist(rows, schema=schema))


def _write_reject(
    handle: TextIO,
    *,
    line_number: int,
    reason: str,
    expected_field_count: int,
    actual_field_count: int,
    row: Sequence[str],
    source_snapshot_sha256: str,
    source_filename: str,
    parser_version: str,
) -> None:
    """Write a compact JSONL description of one rejected source record."""
    handle.write(
        json.dumps(
            {
                "line_number": line_number,
                "reason": reason,
                "expected_field_count": expected_field_count,
                "actual_field_count": actual_field_count,
                "raw_row": "|".join(row)[:2000],
                "source_snapshot_sha256": source_snapshot_sha256,
                "source_filename": source_filename,
                "parser_version": parser_version,
            },
            separators=(",", ":"),
        )
        + "\n"
    )


def _has_missing_business_key(row: dict[str, str], table: TableSpec) -> bool:
    """Return whether a row is missing any required table business-key field."""
    normalized_row = {normalize_column(column): value for column, value in row.items()}
    return any(not normalized_row[normalize_column(key)].strip() for key in table.business_key)


def parse_to_bronze(
    source_path: Path,
    target_path: Path,
    reject_path: Path,
    batch_rows: int,
    parser_version: str = "1.0.0",
    inspected: InspectedSource | None = None,
) -> BronzeResult:
    """Stream a pipe-delimited FDA source to Bronze Parquet and JSONL rejects."""
    if batch_rows < 1:
        raise ValueError("batch_rows must be at least one")

    source_path = Path(source_path)
    target_path = Path(target_path)
    reject_path = Path(reject_path)
    if target_path == reject_path:
        raise ValueError("target_path and reject_path must differ")

    if inspected is None:
        inspected = inspect_source(source_path)
    elif inspected.path != source_path:
        raise ValueError("provided inspection does not match source_path")
    encoding = select_encoding(inspected.sample)
    source_filename = inspected.member or source_path.name
    parquet_temporary: Path | None = None
    rejects_temporary: Path | None = None

    try:
        with open_source_text(
            source_path,
            inspected.member,
            inspected=inspected,
            encoding=encoding,
        ) as source_handle:
            reader = csv.reader(source_handle, delimiter="|", quoting=csv.QUOTE_NONE)
            header = tuple(next(reader))
            table = detect_table(source_filename, header)
            columns = header + PROVENANCE_COLUMNS
            schema = pa.schema(
                [pa.field(column, pa.string(), nullable=True) for column in columns],
                metadata={b"parser_version": parser_version.encode()},
            )
            parquet_temporary = _temporary_sibling(target_path)
            rejects_temporary = _temporary_sibling(reject_path)

            rows_seen = 0
            rows_accepted = 0
            rows_rejected = 0
            batch: list[dict[str, str]] = []
            with (
                pq.ParquetWriter(parquet_temporary, schema) as writer,
                rejects_temporary.open("w", encoding="utf-8", newline="\n") as rejects_handle,
            ):
                for row in reader:
                    rows_seen += 1
                    if len(row) != len(header):
                        rows_rejected += 1
                        _write_reject(
                            rejects_handle,
                            line_number=reader.line_num,
                            reason="field_count",
                            expected_field_count=len(header),
                            actual_field_count=len(row),
                            row=row,
                            source_snapshot_sha256=inspected.sha256,
                            source_filename=source_filename,
                            parser_version=parser_version,
                        )
                        continue

                    accepted = dict(zip(header, row, strict=True))
                    if _has_missing_business_key(accepted, table):
                        rows_rejected += 1
                        _write_reject(
                            rejects_handle,
                            line_number=reader.line_num,
                            reason="missing_business_key",
                            expected_field_count=len(header),
                            actual_field_count=len(row),
                            row=row,
                            source_snapshot_sha256=inspected.sha256,
                            source_filename=source_filename,
                            parser_version=parser_version,
                        )
                        continue

                    accepted.update(
                        {
                            "_source_line_number": str(reader.line_num),
                            "_source_snapshot_sha256": inspected.sha256,
                            "_source_filename": source_filename,
                        }
                    )
                    batch.append(accepted)
                    rows_accepted += 1
                    if len(batch) == batch_rows:
                        _write_batch(writer, schema, batch)
                        batch = []

                if batch:
                    _write_batch(writer, schema, batch)

        assert parquet_temporary is not None
        assert rejects_temporary is not None
        parquet_temporary.replace(target_path)
        rejects_temporary.replace(reject_path)
    except BaseException:
        if parquet_temporary is not None:
            parquet_temporary.unlink(missing_ok=True)
        if rejects_temporary is not None:
            rejects_temporary.unlink(missing_ok=True)
        raise

    source = SourceIdentity(
        path=str(inspected.path),
        filename=source_filename,
        sha256=inspected.sha256,
        byte_size=inspected.byte_size,
        archive_member=inspected.member,
        encoding=encoding,
    )
    stats = ParseStats(
        rows_seen=rows_seen,
        rows_accepted=rows_accepted,
        rows_rejected=rows_rejected,
        columns=columns,
    )
    return BronzeResult(
        table=table.kind,
        source=source,
        bronze_path=str(target_path),
        reject_path=str(reject_path),
        stats=stats,
    )
