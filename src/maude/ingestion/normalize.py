"""DuckDB-backed normalization from Bronze source columns into Silver Parquet."""

import os
import tempfile
from pathlib import Path
from typing import Any

import duckdb
import pyarrow.parquet as pq  # type: ignore[import-untyped]

from maude.domain.models import NormalizationResult
from maude.ingestion.schemas import TableSpec, normalize_column


class NormalizationError(ValueError):
    """Raised when Bronze fields cannot safely form a canonical Silver schema."""


def _temporary_sibling(path: Path) -> Path:
    """Return a unique temporary path beside *path* for atomic replacement."""
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    os.close(descriptor)
    return Path(temporary_name)


def _identifier(value: str) -> str:
    """Quote a SQL identifier without relying on source headers being trusted."""
    return '"' + value.replace('"', '""') + '"'


def _literal(value: str | Path) -> str:
    """Quote a SQL string literal without interpolating its contents as SQL."""
    return "'" + str(value).replace("'", "''") + "'"


def _normalized_value(source_column: str) -> str:
    """Normalize an FDA field value while preserving nonempty sentinel strings."""
    return f"nullif(trim(CAST({_identifier(source_column)} AS VARCHAR)), '')"


def _date_value(source_value: str) -> str:
    """Return DuckDB SQL for either accepted FDA source date format."""
    return f"try_strptime({source_value}, ['%m/%d/%Y', '%Y/%m/%d'])::DATE"


def _assert_unique_output_names(source_columns: tuple[str, ...], spec: TableSpec) -> None:
    """Reject ambiguities before DuckDB can silently overwrite a canonical field."""
    date_names = {normalize_column(column) for column in spec.date_columns}
    output_names: list[str] = []
    sources_by_output: dict[str, list[str]] = {}

    for source_column in source_columns:
        if source_column.startswith("_source_"):
            output_name = source_column
            output_names.append(output_name)
            sources_by_output.setdefault(output_name, []).append(source_column)
            continue

        normalized = normalize_column(source_column)
        if not normalized:
            raise NormalizationError(f"source column {source_column!r} has no canonical name")
        normalized_outputs = (
            (f"{normalized}_raw", normalized) if normalized in date_names else (normalized,)
        )
        for output_name in normalized_outputs:
            output_names.append(output_name)
            sources_by_output.setdefault(output_name, []).append(source_column)

    for reserved in ("dataset_snapshot_id", "record_content_hash"):
        output_names.append(reserved)
        sources_by_output.setdefault(reserved, []).append("generated field")

    collisions = tuple(name for name in output_names if len(sources_by_output[name]) > 1)
    if collisions:
        names = ", ".join(dict.fromkeys(collisions))
        raise NormalizationError(f"normalized column collision: {names}")


def _output_expressions(
    source_columns: tuple[str, ...], spec: TableSpec
) -> tuple[list[str], list[str]]:
    """Build ordered SELECT expressions and the FDA business values used for hashing."""
    date_names = {normalize_column(column) for column in spec.date_columns}
    select_expressions: list[str] = []
    data_values: list[str] = []

    for source_column in source_columns:
        source_identifier = _identifier(source_column)
        if source_column.startswith("_source_"):
            select_expressions.append(f"{source_identifier} AS {source_identifier}")
            continue

        normalized = normalize_column(source_column)
        raw_value = _normalized_value(source_column)
        if normalized in date_names:
            parsed_value = _date_value(raw_value)
            select_expressions.extend(
                (
                    f"{raw_value} AS {_identifier(f'{normalized}_raw')}",
                    f"{parsed_value} AS {_identifier(normalized)}",
                )
            )
            data_values.extend((raw_value, parsed_value))
        else:
            select_expressions.append(f"{raw_value} AS {_identifier(normalized)}")
            data_values.append(raw_value)

    return select_expressions, data_values


def _hash_expression(data_values: list[str]) -> str:
    """Encode all fields unambiguously before generating a deterministic SHA-256 hash."""
    encoded_values = [
        "CASE "
        f"WHEN ({value}) IS NULL THEN '<NULL>' "
        f"ELSE concat('V', length(CAST(({value}) AS VARCHAR)), ':', CAST(({value}) AS VARCHAR)) "
        "END"
        for value in data_values
    ]
    if not encoded_values:
        return "sha256('')"
    return f"sha256(concat_ws('|', {', '.join(encoded_values)}))"


def _date_failure_expression(source_columns: tuple[str, ...], spec: TableSpec) -> str:
    """Count present source date values DuckDB cannot parse without dropping their rows."""
    date_names = {normalize_column(column) for column in spec.date_columns}
    failure_checks: list[str] = []
    for source_column in source_columns:
        if (
            source_column.startswith("_source_")
            or normalize_column(source_column) not in date_names
        ):
            continue
        raw_value = _normalized_value(source_column)
        parsed_value = _date_value(raw_value)
        failure_checks.append(
            f"CASE WHEN ({raw_value}) IS NOT NULL AND ({parsed_value}) IS NULL THEN 1 ELSE 0 END"
        )
    return " + ".join(failure_checks) if failure_checks else "0"


def normalize_bronze(
    bronze_path: Path,
    silver_path: Path,
    spec: TableSpec,
    snapshot_id: str,
) -> NormalizationResult:
    """Normalize one Bronze Parquet table to canonical Silver Parquet with DuckDB.

    Bronze's FDA headers are normalized in their source-column order. Declared date
    fields produce a retained ``*_raw`` string plus a typed ``DATE`` value; source
    provenance is deliberately retained verbatim and excluded from content hashes.
    """
    bronze_path = Path(bronze_path)
    silver_path = Path(silver_path)
    if bronze_path == silver_path:
        raise ValueError("bronze_path and silver_path must differ")

    source_columns = tuple(pq.ParquetFile(bronze_path).schema_arrow.names)
    _assert_unique_output_names(source_columns, spec)
    select_expressions, data_values = _output_expressions(source_columns, spec)
    select_expressions.extend(
        (
            f"{_literal(snapshot_id)} AS {_identifier('dataset_snapshot_id')}",
            f"{_hash_expression(data_values)} AS {_identifier('record_content_hash')}",
        )
    )

    bronze_literal = _literal(bronze_path)
    failure_expression = _date_failure_expression(source_columns, spec)
    failure_query = " ".join(
        (
            f"SELECT COALESCE(SUM({failure_expression}), 0)::BIGINT",
            f"FROM read_parquet({bronze_literal})",
        )
    )
    temporary_path = _temporary_sibling(silver_path)
    connection: Any | None = None
    try:
        temporary_path.unlink()
        connection = duckdb.connect()
        count_row = connection.execute(failure_query).fetchone()
        if count_row is None:
            raise NormalizationError("DuckDB did not return the date parse failure count")
        date_parse_failure_count = int(count_row[0])
        copy_query = (
            f"COPY (SELECT {', '.join(select_expressions)} "
            f"FROM read_parquet({bronze_literal})) TO {_literal(temporary_path)} (FORMAT PARQUET)"
        )
        connection.execute(copy_query)
        connection.close()
        connection = None
        temporary_path.replace(silver_path)
    except BaseException:
        if connection is not None:
            connection.close()
        temporary_path.unlink(missing_ok=True)
        raise

    return NormalizationResult(
        silver_path=str(silver_path),
        date_parse_failure_count=date_parse_failure_count,
    )
