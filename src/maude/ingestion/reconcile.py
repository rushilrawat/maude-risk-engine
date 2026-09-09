import os
import tempfile
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import duckdb
import pyarrow.parquet as pq  # type: ignore[import-untyped]

from maude.domain.enums import SourceRole, TableKind
from maude.domain.models import ReconciliationStats
from maude.ingestion.schemas import normalize_column, spec_for

_PRIORITY = {
    SourceRole.BASE: 0,
    SourceRole.ADD: 1,
    SourceRole.CHANGE: 2,
}
_INTERNAL_PRIORITY = "__maude_role_priority"
_INTERNAL_RANK = "__maude_selection_rank"


class ReconciliationError(ValueError):
    """Raised when role rows cannot form one unambiguous current table."""


def _identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _literal(value: str | Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _temporary_sibling(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    os.close(descriptor)
    return Path(name)


def _role_select(path: Path, role: SourceRole) -> str:
    return (
        f"SELECT *, {_PRIORITY[role]} AS {_identifier(_INTERNAL_PRIORITY)} "
        f"FROM read_parquet({_literal(path)})"
    )


def _join(left: str, right: str, keys: tuple[str, ...]) -> str:
    return " AND ".join(f"{left}.{_identifier(key)} = {right}.{_identifier(key)}" for key in keys)


def _validate_inputs(
    role_paths: Mapping[SourceRole, Path], keys: tuple[str, ...]
) -> dict[SourceRole, Path]:
    if set(role_paths) != set(SourceRole):
        raise ReconciliationError("base, add, and change inputs are all required")
    validated = {role: Path(role_paths[role]) for role in SourceRole}
    required = {*keys, "record_content_hash", "_source_role"}
    for role, path in validated.items():
        columns = set(pq.ParquetFile(path).schema_arrow.names)
        missing = sorted(required - columns)
        if missing:
            raise ReconciliationError(
                f"{role.value} input is missing required columns: {', '.join(missing)}"
            )
        reserved = {_INTERNAL_PRIORITY, _INTERNAL_RANK} & columns
        if reserved:
            column = reserved.pop()
            raise ReconciliationError(f"input uses reserved reconciliation column: {column}")
    return validated


def _reject_role_duplicates(
    connection: Any, role: SourceRole, path: Path, keys: tuple[str, ...]
) -> None:
    key_list = ", ".join(_identifier(key) for key in keys)
    row = connection.execute(
        " ".join(
            (
                "SELECT COUNT(*)::BIGINT FROM (",
                f"SELECT {key_list} FROM read_parquet({_literal(path)})",
                f"GROUP BY {key_list} HAVING COUNT(*) > 1",
                ") AS duplicate_keys",
            )
        )
    ).fetchone()
    if row is None:
        raise RuntimeError("DuckDB did not return duplicate-key count")
    if int(row[0]):
        raise ReconciliationError(
            f"found {int(row[0])} duplicate business keys in {role.value} input"
        )


def _stats_for_role(
    connection: Any,
    table: TableKind,
    role: SourceRole,
    path: Path,
    keys: tuple[str, ...],
) -> ReconciliationStats:
    priority = _PRIORITY[role]
    key_list = ", ".join(_identifier(key) for key in keys)
    lower_join = _join("role_rows", "lower_rows", keys)
    selected_join = _join("role_rows", "selected_rows", keys)
    first_key = _identifier(keys[0])
    query = " ".join(
        (
            "WITH ranked_lower AS (",
            f"SELECT {key_list}, record_content_hash,",
            f"row_number() OVER (PARTITION BY {key_list}",
            f"ORDER BY {_identifier(_INTERNAL_PRIORITY)} DESC) AS lower_rank",
            "FROM candidates",
            f"WHERE {_identifier(_INTERNAL_PRIORITY)} < {priority}",
            "), lower_rows AS (",
            f"SELECT {key_list}, record_content_hash FROM ranked_lower WHERE lower_rank = 1",
            "), selected_rows AS (",
            f"SELECT {key_list}, {_identifier(_INTERNAL_PRIORITY)} AS selected_priority",
            "FROM selected_candidates",
            "), role_rows AS (",
            f"SELECT * FROM read_parquet({_literal(path)})",
            ") SELECT",
            f"coalesce(count_if(lower_rows.{first_key} IS NULL), 0)::BIGINT AS inserted,",
            f"coalesce(count_if(lower_rows.{first_key} IS NOT NULL AND",
            "role_rows.record_content_hash IS DISTINCT FROM lower_rows.record_content_hash), 0)",
            "::BIGINT AS updated,",
            f"coalesce(count_if(lower_rows.{first_key} IS NOT NULL AND",
            "role_rows.record_content_hash IS NOT DISTINCT FROM",
            "lower_rows.record_content_hash), 0)",
            "::BIGINT AS unchanged,",
            f"coalesce(count_if(selected_rows.selected_priority > {priority}), 0)",
            "::BIGINT AS superseded",
            f"FROM role_rows LEFT JOIN lower_rows ON {lower_join}",
            f"LEFT JOIN selected_rows ON {selected_join}",
        )
    )
    row = connection.execute(query).fetchone()
    if row is None:
        raise RuntimeError("DuckDB did not return reconciliation statistics")
    return ReconciliationStats(
        table=table,
        role=role,
        inserted=int(row[0]),
        updated=int(row[1]),
        unchanged=int(row[2]),
        superseded=int(row[3]),
    )


def reconcile_table(
    table: TableKind,
    role_paths: Mapping[SourceRole, Path],
    output_path: Path,
) -> tuple[ReconciliationStats, ...]:
    """Select one current row per business key using change/add/base precedence."""
    keys = tuple(normalize_column(column) for column in spec_for(table).business_key)
    paths = _validate_inputs(role_paths, keys)
    output_path = Path(output_path)
    temporary = _temporary_sibling(output_path)
    connection: Any | None = None
    try:
        temporary.unlink()
        with tempfile.TemporaryDirectory(prefix="maude-reconcile-") as spill_directory:
            connection = duckdb.connect()
            connection.execute(f"SET temp_directory = {_literal(spill_directory)}")
            for role, path in paths.items():
                _reject_role_duplicates(connection, role, path, keys)
            union = " UNION ALL BY NAME ".join(
                _role_select(paths[role], role) for role in SourceRole
            )
            connection.execute(f"CREATE TEMP VIEW candidates AS {union}")
            key_list = ", ".join(_identifier(key) for key in keys)
            connection.execute(
                " ".join(
                    (
                        "CREATE TEMP VIEW selected_candidates AS SELECT * FROM (",
                        "SELECT *, row_number() OVER (",
                        f"PARTITION BY {key_list}",
                        f"ORDER BY {_identifier(_INTERNAL_PRIORITY)} DESC",
                        f") AS {_identifier(_INTERNAL_RANK)} FROM candidates",
                        f") AS ranked WHERE {_identifier(_INTERNAL_RANK)} = 1",
                    )
                )
            )
            stats = tuple(
                _stats_for_role(connection, table, role, paths[role], keys) for role in SourceRole
            )
            connection.execute(
                " ".join(
                    (
                        "COPY (SELECT * EXCLUDE (",
                        f"{_identifier(_INTERNAL_PRIORITY)}, {_identifier(_INTERNAL_RANK)}",
                        ") FROM selected_candidates",
                        f"ORDER BY {key_list}) TO {_literal(temporary)} (FORMAT PARQUET)",
                    )
                )
            )
            connection.close()
            connection = None
        temporary.replace(output_path)
        return stats
    except BaseException:
        if connection is not None:
            connection.close()
        temporary.unlink(missing_ok=True)
        raise
