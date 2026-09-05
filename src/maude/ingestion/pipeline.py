"""Snapshot ingestion orchestration and atomic Silver promotion."""

import re
from collections.abc import Sequence
from contextlib import suppress
from datetime import UTC, datetime
from pathlib import Path
from shutil import rmtree
from uuid import uuid4

from maude.config import Settings
from maude.domain.enums import QualityLevel, RunStatus, TableKind
from maude.domain.models import QualityResult, SnapshotResult, TableResult
from maude.ingestion.archive import inspect_source, sha256_file
from maude.ingestion.manifests import load_manifest, write_manifest
from maude.ingestion.normalize import normalize_bronze
from maude.ingestion.parser import parse_to_bronze
from maude.ingestion.schemas import TABLE_SPECS, SchemaMismatch, spec_for
from maude.quality.checks import has_blocking_failure, run_quality_checks
from maude.storage.layout import SnapshotLayout

_SNAPSHOT_ID = re.compile(r"[a-zA-Z0-9._-]+\Z")


class SnapshotConflict(ValueError):
    """Raised when an immutable snapshot ID is reused for different sources."""


def _require_snapshot_id(snapshot_id: str) -> None:
    if not _SNAPSHOT_ID.fullmatch(snapshot_id):
        raise ValueError("snapshot_id must match [a-zA-Z0-9._-]+")


def _source_checksums(sources: Sequence[Path]) -> tuple[str, ...]:
    return tuple(sorted(sha256_file(Path(source)) for source in sources))


def _manifest_checksums(snapshot: SnapshotResult) -> tuple[str, ...]:
    return tuple(sorted(table.source.sha256 for table in snapshot.tables))


def _existing_snapshot(layout: SnapshotLayout, sources: Sequence[Path]) -> SnapshotResult | None:
    if not layout.manifest.exists():
        return None
    existing = load_manifest(layout.manifest)
    if _source_checksums(sources) != _manifest_checksums(existing):
        raise SnapshotConflict("snapshot_id is already associated with different source checksums")
    if existing.status is RunStatus.PROMOTED and existing.promoted_path:
        return existing
    raise SnapshotConflict("snapshot_id is already associated with an incomplete snapshot")


def _sources_by_table(sources: Sequence[Path]) -> dict[TableKind, Path]:
    """Identify table families from validated filenames before parsing their headers."""
    discovered: dict[TableKind, Path] = {}
    for raw_path in sources:
        path = Path(raw_path)
        inspected = inspect_source(path)
        filename = (inspected.member or path.name).lower()
        matches = tuple(
            spec for spec in TABLE_SPECS if any(token in filename for token in spec.filename_tokens)
        )
        if len(matches) != 1:
            raise SchemaMismatch(f"filename {filename!r} does not match exactly one table family")
        if matches[0].kind in discovered:
            raise ValueError(f"multiple sources detected for {matches[0].kind.value}")
        discovered[matches[0].kind] = path
    return discovered


def _date_parse_quality(count: int, accepted_rows: int) -> QualityResult:
    return QualityResult(
        check="date_parse_failure_count",
        level=QualityLevel.WARNING,
        passed=count == 0,
        message=(
            "all nonblank date values parsed successfully"
            if count == 0
            else f"{count} nonblank date value(s) could not be parsed"
        ),
        metrics={
            "numerator": count,
            "denominator": accepted_rows,
            "threshold": 0,
            "observed": count,
        },
    )


def _process_tables(
    layout: SnapshotLayout,
    snapshot_id: str,
    sources: dict[TableKind, Path],
    settings: Settings,
) -> tuple[TableResult, ...]:
    tables: list[TableResult] = []
    for kind in TableKind:
        source_path = sources.get(kind)
        if source_path is None:
            continue
        bronze = parse_to_bronze(
            source_path,
            layout.bronze / f"{kind.value}.parquet",
            layout.rejects / f"{kind.value}.jsonl",
            settings.batch_rows,
            settings.parser_version,
        )
        normalization = normalize_bronze(
            Path(bronze.bronze_path),
            layout.silver / f"{kind.value}.parquet",
            spec_for(kind),
            snapshot_id,
        )
        tables.append(
            TableResult(
                table=kind,
                source=bronze.source,
                bronze_path=bronze.bronze_path,
                silver_path=normalization.silver_path,
                reject_path=bronze.reject_path,
                stats=bronze.stats,
                quality=[
                    _date_parse_quality(
                        normalization.date_parse_failure_count, bronze.stats.rows_accepted
                    )
                ],
            )
        )
    return tuple(tables)


def _failure_quality(error: Exception) -> QualityResult:
    return QualityResult(
        check="pipeline_exception",
        level=QualityLevel.BLOCKING,
        passed=False,
        message=f"{type(error).__name__}: {error}",
        metrics={},
    )


def _failed_snapshot(
    running: SnapshotResult,
    tables: Sequence[TableResult],
    quality: Sequence[QualityResult],
) -> SnapshotResult:
    return running.model_copy(
        update={
            "finished_at": datetime.now(UTC),
            "status": RunStatus.FAILED,
            "tables": list(tables),
            "quality": list(quality),
            "promoted_path": None,
        }
    )


def ingest_snapshot(
    snapshot_id: str, sources: Sequence[Path], settings: Settings
) -> SnapshotResult:
    """Parse, validate, and atomically promote one immutable MAUDE snapshot."""
    _require_snapshot_id(snapshot_id)
    source_paths = tuple(Path(source) for source in sources)
    layout = SnapshotLayout(settings.data_root, snapshot_id)
    existing = _existing_snapshot(layout, source_paths)
    if existing is not None:
        return existing

    running = SnapshotResult(
        run_id=uuid4(),
        snapshot_id=snapshot_id,
        started_at=datetime.now(UTC),
        finished_at=None,
        status=RunStatus.RUNNING,
        tables=[],
        quality=[],
        promoted_path=None,
        parser_version=settings.parser_version,
    )
    write_manifest(layout.manifest, running)
    layout.staging.mkdir(parents=True, exist_ok=True)
    tables: tuple[TableResult, ...] = ()
    quality: tuple[QualityResult, ...] = ()
    silver_promoted = False
    try:
        sources_by_table = _sources_by_table(source_paths)
        tables = _process_tables(layout, snapshot_id, sources_by_table, settings)
        previous = (
            load_manifest(layout.current_manifest) if layout.current_manifest.exists() else None
        )
        quality = run_quality_checks(tables, previous)
        if has_blocking_failure(quality):
            failed = _failed_snapshot(running, tables, quality)
            write_manifest(layout.manifest, failed)
            return failed

        validated = running.model_copy(
            update={
                "finished_at": datetime.now(UTC),
                "status": RunStatus.VALIDATED,
                "tables": list(tables),
                "quality": list(quality),
            }
        )
        write_manifest(layout.manifest, validated)
        layout.promoted.parent.mkdir(parents=True, exist_ok=True)
        layout.silver.replace(layout.promoted)
        silver_promoted = True
        promoted = validated.model_copy(
            update={
                "status": RunStatus.PROMOTED,
                "promoted_path": str(layout.promoted),
                "tables": [
                    table.model_copy(
                        update={"silver_path": str(layout.promoted / Path(table.silver_path).name)}
                    )
                    for table in validated.tables
                ],
            }
        )
        write_manifest(layout.manifest, promoted)
        write_manifest(layout.current_manifest, promoted)
        # Promotion and the public pointer are already durable. Retain staging
        # evidence for later cleanup rather than rolling back the public state.
        with suppress(OSError):
            rmtree(layout.staging)
        return promoted
    except Exception as error:
        if silver_promoted and layout.promoted.exists() and not layout.silver.exists():
            layout.promoted.replace(layout.silver)
        failed = _failed_snapshot(running, tables, (*quality, _failure_quality(error)))
        write_manifest(layout.manifest, failed)
        return failed
