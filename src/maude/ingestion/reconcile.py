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
_INTERNAL_KEY_COUNT = "__maude_key_count"
_INTERNAL_HASH_COUNT = "__maude_hash_count"
_INTERNAL_ROLE_ROW = "__maude_role_row"


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


def _role_select(relation: str, role: SourceRole) -> str:
    return (
        f"SELECT *, {_PRIORITY[role]} AS {_identifier(_INTERNAL_PRIORITY)} "
        f"FROM {_identifier(relation)}"
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
        reserved = {
            _INTERNAL_PRIORITY,
            _INTERNAL_RANK,
            _INTERNAL_KEY_COUNT,
            _INTERNAL_HASH_COUNT,
            _INTERNAL_ROLE_ROW,
        } & columns
        if reserved:
            column = reserved.pop()
            raise ReconciliationError(f"input uses reserved reconciliation column: {column}")
    return validated


def _prepare_role(
    connection: Any,
    role: SourceRole,
    path: Path,
    keys: tuple[str, ...],
    allow_conflicts: bool,
) -> tuple[str, str, int, int]:
    key_list = ", ".join(_identifier(key) for key in keys)
    ranked = f"role_{role.value}_ranked"
    selected = f"role_{role.value}"
    connection.execute(
        " ".join(
            (
                f"CREATE TEMP VIEW {_identifier(ranked)} AS SELECT *,",
                f"count(*) OVER (PARTITION BY {key_list}) AS",
                f"{_identifier(_INTERNAL_KEY_COUNT)},",
                f"count(DISTINCT record_content_hash) OVER (PARTITION BY {key_list}) AS",
                f"{_identifier(_INTERNAL_HASH_COUNT)},",
                f"row_number() OVER (PARTITION BY {key_list} ORDER BY file_row_number) AS",
                f"{_identifier(_INTERNAL_ROLE_ROW)}",
                f"FROM read_parquet({_literal(path)}, file_row_number = true)",
            )
        )
    )
    duplicate_row = connection.execute(
        " ".join(
            (
                "SELECT coalesce(sum(",
                f"{_identifier(_INTERNAL_KEY_COUNT)} - 1), 0)::BIGINT",
                f"FROM {_identifier(ranked)}",
                f"WHERE {_identifier(_INTERNAL_ROLE_ROW)} = 1",
                f"AND {_identifier(_INTERNAL_HASH_COUNT)} = 1",
                f"AND {_identifier(_INTERNAL_KEY_COUNT)} > 1",
            )
        )
    ).fetchone()
    conflict_row = connection.execute(
        " ".join(
            (
                "SELECT count(*)::BIGINT,",
                f"coalesce(sum({_identifier(_INTERNAL_KEY_COUNT)}), 0)::BIGINT",
                f"FROM {_identifier(ranked)}",
                f"WHERE {_identifier(_INTERNAL_ROLE_ROW)} = 1",
                f"AND {_identifier(_INTERNAL_HASH_COUNT)} > 1",
            )
        )
    ).fetchone()
    if duplicate_row is None or conflict_row is None:
        raise RuntimeError("DuckDB did not return duplicate-key statistics")
    conflict_groups = int(conflict_row[0])
    conflicting_rows = int(conflict_row[1])
    if conflict_groups and not allow_conflicts:
        raise ReconciliationError(
            f"found {conflict_groups} conflicting duplicate business keys in {role.value} input"
        )
    connection.execute(
        " ".join(
            (
                f"CREATE TEMP VIEW {_identifier(selected)} AS SELECT * EXCLUDE (",
                "file_row_number,",
                f"{_identifier(_INTERNAL_KEY_COUNT)},",
                f"{_identifier(_INTERNAL_HASH_COUNT)},",
                f"{_identifier(_INTERNAL_ROLE_ROW)}",
                f") FROM {_identifier(ranked)}",
                f"WHERE {_identifier(_INTERNAL_HASH_COUNT)} = 1",
                f"AND {_identifier(_INTERNAL_ROLE_ROW)} = 1",
            )
        )
    )
    return selected, ranked, int(duplicate_row[0]), conflicting_rows


def _stats_for_role(
    connection: Any,
    table: TableKind,
    role: SourceRole,
    relation: str,
    keys: tuple[str, ...],
    duplicate_rows_collapsed: int,
    conflicting_rows_quarantined: int,
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
            f"SELECT * FROM {_identifier(relation)}",
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
        duplicate_rows_collapsed=duplicate_rows_collapsed,
        conflicting_rows_quarantined=conflicting_rows_quarantined,
    )


def reconcile_table(
    table: TableKind,
    role_paths: Mapping[SourceRole, Path],
    output_path: Path,
    *,
    conflict_path: Path | None = None,
) -> tuple[ReconciliationStats, ...]:
    """Select one current row per business key using change/add/base precedence."""
    keys = tuple(normalize_column(column) for column in spec_for(table).business_key)
    paths = _validate_inputs(role_paths, keys)
    output_path = Path(output_path)
    temporary = _temporary_sibling(output_path)
    conflict_temporary: Path | None = None
    connection: Any | None = None
    try:
        temporary.unlink()
        with tempfile.TemporaryDirectory(prefix="maude-reconcile-") as spill_directory:
            connection = duckdb.connect()
            connection.execute(f"SET temp_directory = {_literal(spill_directory)}")
            prepared = {
                role: _prepare_role(
                    connection,
                    role,
                    path,
                    keys,
                    allow_conflicts=conflict_path is not None,
                )
                for role, path in paths.items()
            }
            union = " UNION ALL BY NAME ".join(
                _role_select(prepared[role][0], role) for role in SourceRole
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
                _stats_for_role(
                    connection,
                    table,
                    role,
                    prepared[role][0],
                    keys,
                    prepared[role][2],
                    prepared[role][3],
                )
                for role in SourceRole
            )
            if conflict_path is not None and any(item[3] for item in prepared.values()):
                conflict_path = Path(conflict_path)
                conflict_temporary = _temporary_sibling(conflict_path)
                conflict_temporary.unlink()
                conflicts = " UNION ALL BY NAME ".join(
                    " ".join(
                        (
                            "SELECT * EXCLUDE (file_row_number,",
                            f"{_identifier(_INTERNAL_KEY_COUNT)},",
                            f"{_identifier(_INTERNAL_HASH_COUNT)},",
                            f"{_identifier(_INTERNAL_ROLE_ROW)}),",
                            "'conflicting_duplicate_business_key' AS _conflict_reason",
                            f"FROM {_identifier(prepared[role][1])}",
                            f"WHERE {_identifier(_INTERNAL_HASH_COUNT)} > 1",
                        )
                    )
                    for role in SourceRole
                )
                connection.execute(
                    f"COPY ({conflicts}) TO {_literal(conflict_temporary)} (FORMAT PARQUET)"
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
        if conflict_temporary is not None and conflict_path is not None:
            conflict_temporary.replace(conflict_path)
            conflict_temporary = None
        temporary.replace(output_path)
        return stats
    except BaseException:
        if connection is not None:
            connection.close()
        temporary.unlink(missing_ok=True)
        if conflict_temporary is not None:
            conflict_temporary.unlink(missing_ok=True)
        raise
