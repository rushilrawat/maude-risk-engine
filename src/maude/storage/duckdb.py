"""Read-only DuckDB views over a promoted MAUDE Silver snapshot.

The views intentionally keep the source rows available.  ``report_documents``
only pre-aggregates device metadata; narratives are fetched as individual
evidence rows so their identity and source location cannot be lost in a join.
"""

import tempfile
from collections.abc import Iterable
from datetime import date, datetime
from pathlib import Path
from typing import Any

import duckdb
import pyarrow.parquet as pq  # type: ignore[import-untyped]

from maude.domain.models import NarrativeEvidence, ReportDocument


class SnapshotSchemaError(ValueError):
    """Raised when a promoted snapshot is missing a required table or column."""


class ReportNotFound(LookupError):
    """Raised when a report ID is not present in the opened snapshot."""


_ARTIFACT_NAMES: dict[str, tuple[str, ...]] = {
    "reports": ("master.parquet", "reports.parquet"),
    "devices": ("device.parquet", "devices.parquet"),
    "patients": ("patient.parquet", "patients.parquet"),
    "narratives": ("narrative.parquet", "narratives.parquet"),
}

_REQUIRED_COLUMNS: dict[str, frozenset[str]] = {
    "reports": frozenset({"mdr_report_key", "event_type", "date_received", "dataset_snapshot_id"}),
    "devices": frozenset(
        {
            "mdr_report_key",
            "device_sequence_no",
            "device_report_product_code",
            "dataset_snapshot_id",
        }
    ),
    "patients": frozenset({"mdr_report_key", "patient_sequence_number", "dataset_snapshot_id"}),
    "narratives": frozenset(
        {
            "mdr_report_key",
            "mdr_text_key",
            "text_type_code",
            "foi_text",
            "_source_filename",
            "_source_line_number",
            "dataset_snapshot_id",
        }
    ),
}

_BRAND_COLUMNS = ("brand_name", "brand_name_1", "device_brand_name")
_MANUFACTURER_COLUMNS = (
    "manufacturer_d_name",
    "manufacturer_name",
    "manufacturer_g1_name",
    "manufacturer_g2_name",
)
_VALIDATION_MEMORY_LIMIT = "64MB"


def _sql_literal(value: str | Path) -> str:
    """Quote an SQL string literal, including paths containing apostrophes."""
    return "'" + str(value).replace("'", "''") + "'"


def _find_artifacts(snapshot_path: Path) -> dict[str, Path]:
    if not snapshot_path.exists():
        raise FileNotFoundError(f"snapshot directory does not exist: {snapshot_path}")
    if not snapshot_path.is_dir():
        raise NotADirectoryError(f"snapshot path is not a directory: {snapshot_path}")

    artifacts: dict[str, Path] = {}
    for view_name, names in _ARTIFACT_NAMES.items():
        candidates = tuple(
            snapshot_path / name for name in names if (snapshot_path / name).is_file()
        )
        if not candidates:
            expected = " or ".join(names)
            raise FileNotFoundError(
                f"snapshot {snapshot_path} is missing required {view_name} artifact ({expected})"
            )
        artifacts[view_name] = candidates[0]
    return artifacts


def _schema_columns(path: Path, view_name: str) -> frozenset[str]:
    try:
        columns = frozenset(pq.read_schema(path).names)
    except Exception as error:
        raise SnapshotSchemaError(
            f"could not read {view_name} Parquet schema at {path}: {error}"
        ) from error
    missing = _REQUIRED_COLUMNS[view_name] - columns
    if missing:
        missing_names = ", ".join(sorted(missing))
        raise SnapshotSchemaError(
            f"{view_name} artifact {path} is missing required Silver columns: {missing_names}"
        )
    return columns


def _list_expression(column: str | None, alias: str) -> str:
    if column is None:
        return f"[]::VARCHAR[] AS {alias}"
    identifier = '"' + column.replace('"', '""') + '"'
    return (
        f"COALESCE(list(DISTINCT {identifier} ORDER BY {identifier}) "
        f"FILTER (WHERE {identifier} IS NOT NULL AND trim(CAST({identifier} AS VARCHAR)) <> ''), "
        f"[]::VARCHAR[]) AS {alias}"
    )


def _create_views(connection: duckdb.DuckDBPyConnection, artifacts: dict[str, Path]) -> None:
    for view_name, path in artifacts.items():
        # read_parquet does not allow parameters in CREATE VIEW.  _sql_literal
        # doubles apostrophes, so this remains a data path and never executable SQL.
        connection.execute(
            f"CREATE VIEW {view_name} AS SELECT * FROM read_parquet({_sql_literal(path)})"
        )

    device_columns = _schema_columns(artifacts["devices"], "devices")
    brand_column = next((column for column in _BRAND_COLUMNS if column in device_columns), None)
    manufacturer_column = next(
        (column for column in _MANUFACTURER_COLUMNS if column in device_columns), None
    )
    connection.execute(
        """
        CREATE TEMP VIEW _device_metadata AS
        SELECT
            mdr_report_key AS report_id,
            {product_codes},
            {brand_names},
            {manufacturers}
        FROM devices
        GROUP BY mdr_report_key
        """.format(
            product_codes=_list_expression("device_report_product_code", "product_codes"),
            brand_names=_list_expression(brand_column, "brand_names"),
            manufacturers=_list_expression(manufacturer_column, "manufacturers"),
        )
    )
    connection.execute(
        """
        CREATE TEMP VIEW _narrative_evidence AS
        SELECT mdr_report_key, mdr_text_key, text_type_code, foi_text,
               _source_filename, _source_line_number
        FROM (
            SELECT n.*,
                   row_number() OVER (
                       PARTITION BY mdr_report_key, mdr_text_key
                       ORDER BY _source_filename,
                                TRY_CAST(_source_line_number AS BIGINT),
                                _source_line_number,
                                text_type_code,
                                foi_text
                   ) AS _evidence_rank
            FROM narratives AS n
        ) AS ranked
        WHERE _evidence_rank = 1
        """
    )
    connection.execute(
        """
        CREATE VIEW report_documents AS
        SELECT
            r.mdr_report_key AS report_id,
            r.event_type AS reported_event_type,
            CAST(r.date_received AS DATE) AS date_received,
            COALESCE(d.product_codes, []::VARCHAR[]) AS product_codes,
            COALESCE(d.brand_names, []::VARCHAR[]) AS brand_names,
            COALESCE(d.manufacturers, []::VARCHAR[]) AS manufacturers,
            CAST(r.dataset_snapshot_id AS VARCHAR) AS dataset_snapshot_id,
            n.mdr_text_key,
            n.text_type_code,
            n.foi_text,
            n._source_filename,
            n._source_line_number
        FROM reports AS r
        LEFT JOIN _device_metadata AS d ON d.report_id = r.mdr_report_key
        LEFT JOIN _narrative_evidence AS n ON n.mdr_report_key = r.mdr_report_key
        """
    )


def _validate_snapshot_provenance(
    connection: duckdb.DuckDBPyConnection, artifacts: dict[str, Path]
) -> None:
    """Require one non-blank snapshot ID consistently across non-empty tables.

    Each query returns at most one row.  In particular, this deliberately avoids
    ``COUNT(DISTINCT ...)`` so a malformed file with one ID per row cannot grow
    Python or DuckDB state in proportion to its number of distinct IDs.
    """
    expected_snapshot_id: str | None = None
    for view_name, path in artifacts.items():
        first_row = connection.execute(
            f"SELECT CAST(dataset_snapshot_id AS VARCHAR) FROM {view_name} LIMIT 1"
        ).fetchone()
        if first_row is None:
            continue

        candidate_row = connection.execute(
            f"""
            SELECT CAST(dataset_snapshot_id AS VARCHAR)
            FROM {view_name}
            WHERE dataset_snapshot_id IS NOT NULL
              AND trim(CAST(dataset_snapshot_id AS VARCHAR)) <> ''
            LIMIT 1
            """
        ).fetchone()
        if candidate_row is None:
            raise SnapshotSchemaError(
                f"{view_name} artifact {path} contains no usable dataset_snapshot_id; "
                f"first value is {first_row[0]!r}, but every row must be non-null and non-blank"
            )
        snapshot_id = str(candidate_row[0])
        invalid_row = connection.execute(
            f"""
            SELECT CAST(dataset_snapshot_id AS VARCHAR)
            FROM {view_name}
            WHERE dataset_snapshot_id IS NULL
               OR trim(CAST(dataset_snapshot_id AS VARCHAR)) = ''
            LIMIT 1
            """
        ).fetchone()
        if invalid_row is not None:
            raise SnapshotSchemaError(
                f"{view_name} artifact {path} contains invalid dataset_snapshot_id "
                f"value {invalid_row[0]!r}; every row must be non-null and non-blank"
            )

        different_row = connection.execute(
            f"""
            SELECT CAST(dataset_snapshot_id AS VARCHAR)
            FROM {view_name}
            WHERE dataset_snapshot_id IS NOT NULL
              AND trim(CAST(dataset_snapshot_id AS VARCHAR)) <> ''
              AND CAST(dataset_snapshot_id AS VARCHAR) <> ?
            LIMIT 1
            """,
            [snapshot_id],
        ).fetchone()
        if different_row is not None:
            raise SnapshotSchemaError(
                f"{view_name} artifact {path} contains multiple dataset_snapshot_id values; "
                f"found {different_row[0]!r} and {snapshot_id!r}"
            )
        if expected_snapshot_id is None:
            expected_snapshot_id = snapshot_id
        elif snapshot_id != expected_snapshot_id:
            raise SnapshotSchemaError(
                f"{view_name} artifact {path} has dataset_snapshot_id {snapshot_id!r}; "
                f"expected {expected_snapshot_id!r} to match the other snapshot artifacts"
            )


def _validate_report_keys(connection: duckdb.DuckDBPyConnection, artifact: Path) -> None:
    """Reject duplicate report keys before exposing a document reader."""
    duplicate = connection.execute(
        """
        SELECT CAST(mdr_report_key AS VARCHAR), COUNT(*)::BIGINT
        FROM reports
        GROUP BY mdr_report_key
        HAVING COUNT(*) > 1
        LIMIT 1
        """
    ).fetchone()
    if duplicate is not None:
        raise SnapshotSchemaError(
            f"reports artifact {artifact} contains duplicate mdr_report_key "
            f"{duplicate[0]!r} ({duplicate[1]} rows)"
        )


def open_snapshot(snapshot_path: Path) -> duckdb.DuckDBPyConnection:
    """Open an in-memory connection exposing one promoted Silver snapshot.

    All validation occurs before returning the connection.  Any partially
    initialized connection is closed before an error is propagated to the
    caller; a successfully returned connection is owned by the caller.
    """
    snapshot_path = Path(snapshot_path)
    connection: duckdb.DuckDBPyConnection | None = None
    try:
        artifacts = _find_artifacts(snapshot_path)
        for view_name, artifact in artifacts.items():
            _schema_columns(artifact, view_name)
        connection = duckdb.connect(":memory:")
        with tempfile.TemporaryDirectory(prefix="maude-duckdb-validation-") as spill_directory:
            connection.execute("SET memory_limit = ?", [_VALIDATION_MEMORY_LIMIT])
            connection.execute("SET temp_directory = ?", [spill_directory])
            _create_views(connection, artifacts)
            _validate_snapshot_provenance(connection, artifacts)
            _validate_report_keys(connection, artifacts["reports"])
            # The spill directory is only needed for validation.  Resetting the
            # setting before TemporaryDirectory cleanup keeps the returned
            # caller-owned connection usable after this function returns.
            connection.execute("SET temp_directory = ?", [""])
        return connection
    except BaseException:
        if connection is not None:
            connection.close()
        raise


def _as_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        for format_string in ("%Y-%m-%d", "%m/%d/%Y", "%Y/%m/%d"):
            try:
                return datetime.strptime(value, format_string).date()
            except ValueError:
                continue
    raise SnapshotSchemaError(
        f"report date_received has unexpected value type: {type(value).__name__}"
    )


def _as_text(value: Any) -> str:
    return "" if value is None else str(value)


def _sort_value(value: str) -> tuple[int, int | str, str]:
    try:
        return (0, int(value), value)
    except ValueError:
        return (1, value, value)


def _sorted_unique_strings(values: Iterable[Any]) -> tuple[str, ...]:
    normalized = {
        _as_text(value) for value in values if value is not None and _as_text(value).strip()
    }
    return tuple(sorted(normalized))


def fetch_report_document(connection: duckdb.DuckDBPyConnection, report_id: str) -> ReportDocument:
    """Fetch one report and its separately sourced, deduplicated narratives."""
    rows = connection.execute(
        """
        SELECT report_id, reported_event_type, date_received,
               product_codes, brand_names, manufacturers, dataset_snapshot_id,
               mdr_text_key, text_type_code, foi_text,
               _source_filename, _source_line_number
        FROM report_documents
        WHERE report_id = ?
        """,
        [report_id],
    ).fetchall()
    if not rows:
        raise ReportNotFound(f"report_id {report_id!r} was not found")

    row = rows[0]
    narrative_rows = [row[7:] for row in rows if row[7] is not None]
    ordered_rows = sorted(
        narrative_rows,
        key=lambda narrative: (
            _sort_value(_as_text(narrative[0])),
            _as_text(narrative[3]),
            _sort_value(_as_text(narrative[4])),
            _as_text(narrative[1]),
            _as_text(narrative[2]),
        ),
    )
    seen: set[str] = set()
    narratives: list[NarrativeEvidence] = []
    for narrative in ordered_rows:
        evidence_id = f"narrative:{_as_text(narrative[0])}"
        if evidence_id in seen:
            continue
        seen.add(evidence_id)
        try:
            source_line_number = int(_as_text(narrative[4]))
        except ValueError as error:
            raise SnapshotSchemaError(
                f"narratives artifact contains non-integer _source_line_number for {evidence_id}"
            ) from error
        narratives.append(
            NarrativeEvidence(
                evidence_id=evidence_id,
                text_type_code=None if narrative[1] is None else str(narrative[1]),
                text=_as_text(narrative[2]),
                source_filename=_as_text(narrative[3]),
                source_line_number=source_line_number,
            )
        )

    return ReportDocument(
        report_id=_as_text(row[0]),
        reported_event_type=None if row[1] is None else str(row[1]),
        date_received=_as_date(row[2]),
        product_codes=_sorted_unique_strings(row[3] or ()),
        brand_names=_sorted_unique_strings(row[4] or ()),
        manufacturers=_sorted_unique_strings(row[5] or ()),
        narratives=tuple(narratives),
        dataset_snapshot_id=_as_text(row[6]),
    )
