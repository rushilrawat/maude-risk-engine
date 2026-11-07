"""Build an immutable historical snapshot from the preserved FDA archive set."""

import hashlib
import json
from collections.abc import Sequence
from contextlib import suppress
from datetime import UTC, datetime
from pathlib import Path
from shutil import rmtree
from uuid import uuid4

import duckdb
import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]

from maude.config import Settings
from maude.domain.enums import QualityLevel, RunStatus, SourceRole, TableKind
from maude.domain.models import (
    DownloadedArtifact,
    FailureEvidence,
    HistoricalAcquisitionRun,
    ParseStats,
    PromotionIntent,
    QualityResult,
    ReconciliationStats,
    SnapshotResult,
    SourceIdentity,
    TableResult,
)
from maude.ingestion.archive import inspect_source
from maude.ingestion.fda_catalog import HISTORICAL_ARCHIVES
from maude.ingestion.manifests import fsync_directory, load_manifest, write_manifest
from maude.ingestion.normalize import normalize_bronze
from maude.ingestion.parser import parse_to_bronze
from maude.ingestion.pipeline import (
    _date_parse_quality,
    _prepare_layout,
    _prepare_staging,
    _snapshot_claim,
    _validate_promoted_artifacts,
)
from maude.ingestion.reconcile import reconcile_table
from maude.ingestion.schemas import spec_for
from maude.quality.checks import has_blocking_failure, run_quality_checks
from maude.storage.layout import SnapshotLayout


class HistoricalSnapshotError(ValueError):
    """Raised when an acquisition manifest cannot form a trusted history snapshot."""


def _sql_literal(value: str | Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _snapshot_id(acquisition: HistoricalAcquisitionRun) -> str:
    """Derive an immutable ID from the catalog evidence and archive checksums."""
    if acquisition.catalog is None:
        raise HistoricalSnapshotError("acquisition manifest has no FDA catalog evidence")
    identity = {
        "catalog_sha256": acquisition.catalog.sha256,
        "archives": [
            {
                "filename": artifact.filename,
                "table": artifact.table.value,
                "sha256": artifact.sha256,
            }
            for artifact in sorted(acquisition.artifacts, key=lambda item: item.filename)
        ],
    }
    encoded = json.dumps(identity, sort_keys=True, separators=(",", ":")).encode()
    return f"fda-history-{hashlib.sha256(encoded).hexdigest()[:16]}"


def _source_identity(artifact: DownloadedArtifact) -> SourceIdentity:
    return SourceIdentity(
        path=artifact.raw_path,
        filename=artifact.archive_member,
        sha256=artifact.sha256,
        byte_size=artifact.byte_size,
        archive_member=artifact.archive_member,
        encoding=artifact.encoding,
        source_role=artifact.role,
    )


def _same_inputs(snapshot: SnapshotResult, inputs: Sequence[SourceIdentity]) -> bool:
    actual = sorted((item.filename, item.sha256) for item in snapshot.inputs)
    expected = sorted((item.filename, item.sha256) for item in inputs)
    return actual == expected


def _validate_acquisition(acquisition: HistoricalAcquisitionRun) -> None:
    if acquisition.status != "complete":
        raise HistoricalSnapshotError("historical acquisition is not complete")
    if len(acquisition.artifacts) != acquisition.expected_archive_count:
        raise HistoricalSnapshotError("acquisition artifact count does not match its manifest")
    filenames = {artifact.filename for artifact in acquisition.artifacts}
    if filenames != set(HISTORICAL_ARCHIVES):
        raise HistoricalSnapshotError("acquisition does not contain the complete FDA allowlist")
    for artifact in acquisition.artifacts:
        if HISTORICAL_ARCHIVES.get(artifact.filename) is not artifact.table:
            raise HistoricalSnapshotError(
                f"acquisition table family does not match {artifact.filename}"
            )
        if artifact.role is not SourceRole.BASE:
            raise HistoricalSnapshotError(
                f"historical source role must be base: {artifact.filename}"
            )


def _write_empty_parquet(path: Path, schema: pa.Schema) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_batches([], schema=schema), path)


def _combine_normalized_parts(
    paths: Sequence[Path], output_path: Path, scratch_root: Path
) -> None:
    if not paths:
        raise HistoricalSnapshotError("cannot combine an empty source family")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    scratch_root.mkdir(parents=True, exist_ok=True)
    temporary = output_path.with_name(f".{output_path.name}.{uuid4().hex}.tmp")
    input_sql = ", ".join(_sql_literal(path) for path in paths)
    connection = duckdb.connect()
    try:
        connection.execute(f"SET temp_directory = {_sql_literal(scratch_root)}")
        connection.execute("SET memory_limit = '4GB'")
        connection.execute("SET threads = 1")
        connection.execute(
            "COPY (SELECT * FROM read_parquet(["
            f"{input_sql}], union_by_name=true)) "
            f"TO {_sql_literal(temporary)} (FORMAT PARQUET)"
        )
        temporary.replace(output_path)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise
    finally:
        connection.close()


def _reconcile_parts(
    table: TableKind,
    normalized_parts: Sequence[Path],
    output_path: Path,
    conflict_path: Path,
    roles_root: Path,
    scratch_root: Path,
) -> tuple[ReconciliationStats, ...]:
    combined = roles_root / f"{table.value}-base.parquet"
    _combine_normalized_parts(normalized_parts, combined, scratch_root)
    schema = pq.ParquetFile(combined).schema_arrow
    role_paths = {SourceRole.BASE: combined}
    for role in (SourceRole.ADD, SourceRole.CHANGE):
        empty_path = roles_root / f"{table.value}-{role.value}.parquet"
        _write_empty_parquet(empty_path, schema)
        role_paths[role] = empty_path
    return reconcile_table(
        table,
        role_paths,
        output_path,
        conflict_path=conflict_path,
        scratch_root=scratch_root,
    )


def _ingest_table(
    table: TableKind,
    artifacts: Sequence[DownloadedArtifact],
    settings: Settings,
    snapshot_id: str,
    layout: SnapshotLayout,
    scratch_root: Path,
) -> tuple[TableResult, tuple[ReconciliationStats, ...]]:
    bronze_root = layout.bronze / table.value
    reject_root = layout.rejects / table.value
    parts_root = layout.staging / "silver-parts" / table.value
    for directory in (bronze_root, reject_root, parts_root):
        directory.mkdir(parents=True, exist_ok=True)

    normalized_parts: list[Path] = []
    sources: list[SourceIdentity] = []
    parse_results: list[ParseStats] = []
    date_failures = 0
    columns: list[str] = []
    ordered_artifacts = sorted(artifacts, key=lambda item: item.filename)
    for index, artifact in enumerate(ordered_artifacts):
        source_path = Path(artifact.raw_path)
        inspected = inspect_source(source_path)
        if (
            inspected.sha256 != artifact.sha256
            or inspected.byte_size != artifact.byte_size
            or inspected.member != artifact.archive_member
        ):
            raise HistoricalSnapshotError(
                f"raw source does not match acquisition evidence: {artifact.filename}"
            )
        identity = _source_identity(artifact)
        bronze = parse_to_bronze(
            source_path,
            bronze_root / f"{index:03d}.parquet",
            reject_root / f"{index:03d}.jsonl",
            settings.batch_rows,
            settings.parser_version,
            inspected,
            identity,
        )
        normalized_path = parts_root / f"{index:03d}.parquet"
        normalized = normalize_bronze(
            Path(bronze.bronze_path),
            normalized_path,
            spec_for(table),
            snapshot_id,
            scratch_root=scratch_root,
        )
        normalized_parts.append(normalized_path)
        sources.append(bronze.source)
        parse_results.append(bronze.stats)
        date_failures += normalized.date_parse_failure_count
        columns.extend(column for column in bronze.stats.columns if column not in columns)

    conflicts_path = layout.silver / "_conflicts" / f"{table.value}.parquet"
    stats = _reconcile_parts(
        table,
        normalized_parts,
        layout.silver / f"{table.value}.parquet",
        conflicts_path,
        layout.staging / "roles",
        scratch_root,
    )
    accepted_rows = sum(item.rows_accepted for item in parse_results)
    table_quality = [_date_parse_quality(date_failures, accepted_rows)]
    conflicting_rows = sum(item.conflicting_rows_quarantined for item in stats)
    if conflicting_rows:
        table_quality.append(
            QualityResult(
                check="reconciliation_conflicts",
                level=QualityLevel.WARNING,
                passed=False,
                message=f"{conflicting_rows} conflicting source row(s) were quarantined",
                metrics={"quarantined_rows": conflicting_rows},
            )
        )
    table_result = TableResult(
        table=table,
        source=sources[0],
        sources=tuple(sources),
        bronze_path=str(bronze_root),
        silver_path=str(layout.silver / f"{table.value}.parquet"),
        reject_path=str(reject_root),
        stats=ParseStats(
            rows_seen=sum(item.rows_seen for item in parse_results),
            rows_accepted=sum(item.rows_accepted for item in parse_results),
            rows_rejected=sum(item.rows_rejected for item in parse_results),
            columns=tuple(columns),
        ),
        quality=tuple(table_quality),
    )
    return table_result, stats


def _promote_historical(layout: SnapshotLayout, validated: SnapshotResult) -> SnapshotResult:
    if not layout.promoted.exists():
        if not layout.silver.is_dir():
            raise FileNotFoundError("validated historical Silver output is missing")
        rename_intent = validated.model_copy(
            update={"promotion": PromotionIntent(phase="rename_pending")}
        )
        write_manifest(layout.manifest, rename_intent)
        layout.silver.replace(layout.promoted)
        fsync_directory(layout.promoted.parent)
    _validate_promoted_artifacts(layout, validated)
    promoted = validated.model_copy(
        update={
            "status": RunStatus.PROMOTED,
            "promoted_path": str(layout.promoted),
            "failure": None,
            "promotion": None,
            "tables": [
                table.model_copy(
                    update={"silver_path": str(layout.promoted / Path(table.silver_path).name)}
                )
                for table in validated.tables
            ],
        }
    )
    write_manifest(layout.manifest, promoted)
    with suppress(Exception):
        rmtree(layout.staging)
    return promoted


def ingest_fda_history(
    acquisition: HistoricalAcquisitionRun,
    settings: Settings,
) -> SnapshotResult:
    """Parse, reconcile, quality-gate, and publish the FDA historical archive set.

    The resulting snapshot is immutable and does not replace ``current.json``.
    Temporary DuckDB spill, Bronze, normalized parts, and Silver output remain on
    the configured data volume.
    """
    _validate_acquisition(acquisition)
    snapshot_id = _snapshot_id(acquisition)
    inputs = tuple(_source_identity(artifact) for artifact in acquisition.artifacts)
    grouped = {
        table: tuple(artifact for artifact in acquisition.artifacts if artifact.table is table)
        for table in TableKind
    }
    if any(not grouped[table] for table in TableKind):
        raise HistoricalSnapshotError("historical acquisition is missing a canonical table family")

    layout = SnapshotLayout(settings.data_root, snapshot_id)
    locks_root = _prepare_layout(layout)
    with _snapshot_claim(locks_root, snapshot_id):
        if layout.manifest.exists():
            existing = load_manifest(layout.manifest)
            if existing.snapshot_id != snapshot_id or not _same_inputs(existing, inputs):
                raise HistoricalSnapshotError("snapshot ID is already bound to different inputs")
            if existing.status is RunStatus.PROMOTED and existing.promotion is None:
                _validate_promoted_artifacts(layout, existing)
                return existing
            if existing.status is RunStatus.VALIDATED and (
                layout.silver.is_dir() or layout.promoted.is_dir()
            ):
                return _promote_historical(layout, existing)
            if layout.promoted.exists():
                raise HistoricalSnapshotError(
                    "historical output exists without a recoverable validated manifest"
                )
            if layout.staging.exists():
                rmtree(layout.staging)
        elif layout.staging.exists():
            rmtree(layout.staging)

        started_at = datetime.now(UTC)
        running = SnapshotResult(
            run_id=uuid4(),
            snapshot_id=snapshot_id,
            started_at=started_at,
            finished_at=None,
            status=RunStatus.RUNNING,
            tables=(),
            quality=(),
            promoted_path=None,
            parser_version=settings.parser_version,
            inputs=inputs,
            source_acquisition_run_id=acquisition.run_id,
            promotion=PromotionIntent(phase="running"),
        )
        write_manifest(layout.manifest, running)
        tables: list[TableResult] = []
        reconciliation: list[ReconciliationStats] = []
        quality: tuple[QualityResult, ...] = ()
        active_table: TableKind | None = None
        phase = "staging_setup"
        scratch_root = layout.staging / "duckdb-temp"
        try:
            _prepare_staging(layout)
            for table in TableKind:
                active_table = table
                phase = "parse_normalize_reconcile"
                result, table_stats = _ingest_table(
                    table,
                    grouped[table],
                    settings,
                    snapshot_id,
                    layout,
                    scratch_root,
                )
                tables.append(result)
                reconciliation.extend(table_stats)
                running = running.model_copy(
                    update={"tables": tuple(tables), "reconciliation": tuple(reconciliation)}
                )
                write_manifest(layout.manifest, running)

            phase = "quality"
            quality = run_quality_checks(tables, scratch_root=scratch_root)
            if has_blocking_failure(quality):
                failed = running.model_copy(
                    update={
                        "finished_at": datetime.now(UTC),
                        "status": RunStatus.FAILED,
                        "tables": tuple(tables),
                        "quality": quality,
                        "failure": FailureEvidence(
                            phase="quality",
                            error_type="BlockingQualityFailure",
                            message="one or more historical snapshot quality checks failed",
                        ),
                        "promotion": None,
                    }
                )
                write_manifest(layout.manifest, failed)
                return failed
            validated = running.model_copy(
                update={
                    "finished_at": datetime.now(UTC),
                    "status": RunStatus.VALIDATED,
                    "tables": tuple(tables),
                    "quality": quality,
                    "promotion": PromotionIntent(phase="validated"),
                }
            )
            write_manifest(layout.manifest, validated)
        except Exception as error:
            failed = running.model_copy(
                update={
                    "finished_at": datetime.now(UTC),
                    "status": RunStatus.FAILED,
                    "tables": tuple(tables),
                    "quality": quality,
                    "failure": FailureEvidence(
                        phase=phase,
                        table=active_table,
                        error_type=type(error).__name__,
                        message=str(error),
                    ),
                    "promotion": None,
                }
            )
            write_manifest(layout.manifest, failed)
            return failed

        try:
            return _promote_historical(layout, validated)
        except Exception as error:
            return validated.model_copy(
                update={
                    "failure": FailureEvidence(
                        phase="promotion",
                        error_type=type(error).__name__,
                        message=str(error),
                    )
                }
            )
