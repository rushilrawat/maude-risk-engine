"""Blocking and warning checks for MAUDE ingestion snapshots.

The check functions accept their inputs explicitly.  In particular, they do not
open a configured database or discover an alternate source behind the caller's
back.  File-based checks stream their inputs in small chunks.
"""

from collections.abc import Iterable, Sequence
from pathlib import Path
from zipfile import ZipFile

import pyarrow.parquet as pq  # type: ignore[import-untyped]

from maude.domain.enums import QualityLevel, TableKind
from maude.domain.models import QualityResult, SnapshotResult, TableResult
from maude.ingestion.schemas import normalize_column, spec_for

type Key = str | Sequence[str]


def _result(
    check: str,
    level: QualityLevel,
    passed: bool,
    message: str,
    *,
    numerator: int | float,
    denominator: int | float,
    threshold: int | float,
    observed: int | float,
    **extra: int | float | str | None,
) -> QualityResult:
    metrics: dict[str, int | float | str | None] = {
        "numerator": numerator,
        "denominator": denominator,
        "threshold": threshold,
        "observed": observed,
    }
    metrics.update(extra)
    return QualityResult(check=check, level=level, passed=passed, message=message, metrics=metrics)


def has_blocking_failure(results: Iterable[QualityResult]) -> bool:
    """Return true only for a failed blocking result; warnings never block promotion."""
    return any(result.level is QualityLevel.BLOCKING and not result.passed for result in results)


def row_conservation(rows_seen: int, accepted: int, rejected: int) -> QualityResult:
    """Ensure every seen source row is either accepted or rejected exactly once."""
    accounted = accepted + rejected
    passed = accounted == rows_seen
    return _result(
        "row_conservation",
        QualityLevel.BLOCKING,
        passed,
        (
            "all source rows are accounted for"
            if passed
            else "accepted and rejected rows do not conserve rows seen"
        ),
        numerator=accounted,
        denominator=rows_seen,
        threshold=rows_seen,
        observed=accounted,
        rows_seen=rows_seen,
        accepted=accepted,
        rejected=rejected,
    )


def reject_fraction(rows_seen: int, rejected: int) -> QualityResult:
    """Gate rejected rows at a strict 0.5 percent blocking threshold."""
    observed = rejected / rows_seen if rows_seen else 0.0
    if rejected == 0:
        level, passed = QualityLevel.WARNING, True
        message = "no rows were rejected"
    elif observed <= 0.005:
        level, passed = QualityLevel.WARNING, False
        message = "some source rows were rejected"
    else:
        level, passed = QualityLevel.BLOCKING, False
        message = "rejected row fraction exceeds 0.5 percent"
    return _result(
        "reject_fraction",
        level,
        passed,
        message,
        numerator=rejected,
        denominator=rows_seen,
        threshold=0.005,
        observed=observed,
    )


def row_count_drift(current: int, previous: int) -> QualityResult:
    """Gate current accepted rows against 90 percent of the prior count."""
    observed = current / previous if previous else (1.0 if current == 0 else float("inf"))
    passed = previous == 0 or current >= previous * 0.9
    return _result(
        "row_count_drift",
        QualityLevel.BLOCKING,
        passed,
        "accepted row count is within the prior snapshot tolerance"
        if passed
        else "accepted row count is below 90 percent of the prior snapshot",
        numerator=current,
        denominator=previous,
        threshold=0.9,
        observed=observed,
    )


def _key_tuple(key: Key) -> tuple[str, ...]:
    return (key,) if isinstance(key, str) else tuple(key)


def business_key_uniqueness(keys: Iterable[Key]) -> QualityResult:
    """Block duplicate canonical business keys."""
    seen: set[tuple[str, ...]] = set()
    duplicates = 0
    total = 0
    for key in keys:
        total += 1
        canonical = _key_tuple(key)
        if canonical in seen:
            duplicates += 1
        seen.add(canonical)
    observed = duplicates / total if total else 0.0
    return _result(
        "business_key_uniqueness",
        QualityLevel.BLOCKING,
        duplicates == 0,
        "canonical business keys are unique"
        if duplicates == 0
        else f"found {duplicates} duplicate canonical business key rows",
        numerator=duplicates,
        denominator=total,
        threshold=0,
        observed=observed,
    )


def _kind(value: TableKind | TableResult | str) -> TableKind:
    if isinstance(value, TableResult):
        return value.table
    return value if isinstance(value, TableKind) else TableKind(value)


def required_table_set(tables: Iterable[TableKind | TableResult | str]) -> QualityResult:
    """Require the four canonical MAUDE source table families."""
    present = {_kind(table) for table in tables}
    required = set(TableKind)
    missing = sorted(kind.value for kind in required - present)
    return _result(
        "required_table_set",
        QualityLevel.BLOCKING,
        not missing,
        "all required tables are present"
        if not missing
        else f"missing required tables: {', '.join(missing)}",
        numerator=len(present & required),
        denominator=len(required),
        threshold=len(required),
        observed=len(present & required),
        missing=", ".join(missing) if missing else None,
    )


def orphan_fraction(child_keys: Iterable[str], master_keys: Iterable[str]) -> QualityResult:
    """Check child report keys against the explicitly supplied master key set."""
    masters = set(master_keys)
    total = 0
    orphans = 0
    for key in child_keys:
        if not key:
            continue
        total += 1
        if key not in masters:
            orphans += 1
    observed = orphans / total if total else 0.0
    if orphans == 0:
        level, passed, message = QualityLevel.WARNING, True, "no orphan child keys found"
    elif observed <= 0.01:
        level, passed, message = (
            QualityLevel.WARNING,
            False,
            "child table contains orphan report keys",
        )
    else:
        level, passed, message = (
            QualityLevel.BLOCKING,
            False,
            "orphan report-key fraction exceeds 1 percent",
        )
    return _result(
        "orphan_fraction",
        level,
        passed,
        message,
        numerator=orphans,
        denominator=total,
        threshold=0.01,
        observed=observed,
    )


def _newline_count(path: Path, member: str | None = None) -> int:
    if member is None and path.suffix.lower() != ".zip":
        with path.open("rb") as handle:
            return sum(chunk.count(b"\n") for chunk in iter(lambda: handle.read(1024 * 1024), b""))
    with ZipFile(path) as archive:
        if member is None:
            names = [
                name
                for name in archive.namelist()
                if not name.endswith("/") and name.lower().endswith(".txt")
            ]
            if len(names) != 1:
                raise ValueError("archive must contain exactly one text member")
            member = names[0]
        with archive.open(member, "r") as handle:
            return sum(chunk.count(b"\n") for chunk in iter(lambda: handle.read(1024 * 1024), b""))


def truncation_comparison(
    archive_path: Path, converted_path: Path, member: str | None = None
) -> QualityResult:
    """Compare newline counts by streaming the archive member and converted file."""
    archive_rows = _newline_count(Path(archive_path), member)
    converted_rows = _newline_count(Path(converted_path))
    observed = (
        converted_rows / archive_rows if archive_rows else (1.0 if converted_rows == 0 else 0.0)
    )
    passed = archive_rows == 0 or observed >= 0.9
    return _result(
        "truncation_comparison",
        QualityLevel.BLOCKING,
        passed,
        "converted file preserves at least 90 percent of archive newlines"
        if passed
        else "converted file appears truncated relative to archive member",
        numerator=converted_rows,
        denominator=archive_rows,
        threshold=0.9,
        observed=observed,
    )


def _parquet_rows(path: Path, columns: Sequence[str]) -> Iterable[tuple[str, ...]]:
    parquet = pq.ParquetFile(path)
    for batch in parquet.iter_batches(batch_size=8192, columns=list(columns)):
        arrays = [batch.column(index).to_pylist() for index in range(batch.num_columns)]
        for row in zip(*arrays, strict=True):
            yield tuple("" if value is None else str(value) for value in row)


def _table_keys(table: TableResult) -> Iterable[tuple[str, ...]]:
    path = Path(table.silver_path)
    if not path.exists():
        return ()
    spec = spec_for(table.table)
    columns = tuple(normalize_column(column) for column in spec.business_key)
    return _parquet_rows(path, columns)


def run_quality_checks(
    tables: Sequence[TableResult], previous: SnapshotResult | None = None
) -> tuple[QualityResult, ...]:
    """Run the snapshot quality gates using only the supplied table results."""
    ordered = tuple(sorted(tables, key=lambda table: table.table.value))
    results: list[QualityResult] = [required_table_set(ordered)]
    for table in ordered:
        results.append(
            row_conservation(
                table.stats.rows_seen, table.stats.rows_accepted, table.stats.rows_rejected
            )
        )
        results.append(reject_fraction(table.stats.rows_seen, table.stats.rows_rejected))
        keys = _table_keys(table)
        results.append(business_key_uniqueness(keys))
        if previous is not None:
            matching = next(
                (
                    prior
                    for prior in previous.tables
                    if prior.table is table.table and prior.source.filename == table.source.filename
                ),
                None,
            )
            if matching is not None:
                results.append(
                    row_count_drift(table.stats.rows_accepted, matching.stats.rows_accepted)
                )

    master = next((table for table in ordered if table.table is TableKind.MASTER), None)
    if master is not None and Path(master.silver_path).exists():
        master_keys = (key[0] for key in _table_keys(master))
        master_key_set = set(master_keys)
        for table in ordered:
            if table.table is TableKind.MASTER or not Path(table.silver_path).exists():
                continue
            child_keys = (
                key[0] for key in _parquet_rows(Path(table.silver_path), ("mdr_report_key",))
            )
            results.append(orphan_fraction(child_keys, master_key_set))
    return tuple(results)
