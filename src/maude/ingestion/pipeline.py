"""Snapshot ingestion orchestration with recoverable atomic Silver promotion."""

import json
import os
import re
import time
from collections.abc import Iterator, Sequence
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from shutil import rmtree
from uuid import uuid4

from maude.config import Settings
from maude.domain.enums import QualityLevel, RunStatus, TableKind
from maude.domain.models import (
    FailureEvidence,
    PromotionIntent,
    QualityResult,
    SnapshotResult,
    SourceIdentity,
    TableResult,
)
from maude.ingestion.archive import inspect_source, sha256_file
from maude.ingestion.encoding import select_encoding
from maude.ingestion.manifests import fsync_directory, load_manifest, write_manifest
from maude.ingestion.normalize import normalize_bronze
from maude.ingestion.parser import parse_to_bronze
from maude.ingestion.schemas import TABLE_SPECS, SchemaMismatch, spec_for
from maude.quality.checks import has_blocking_failure, run_quality_checks
from maude.storage.layout import SnapshotLayout

_SNAPSHOT_ID = re.compile(r"[a-zA-Z0-9._-]+\Z")
_LOCK_WAIT_SECONDS = 30.0
_LOCK_POLL_SECONDS = 0.05


class SnapshotConflict(ValueError):
    """Raised when an immutable snapshot ID is reused for different sources."""


@dataclass(frozen=True)
class _SourceCandidate:
    table: TableKind
    path: Path
    source: SourceIdentity


class _StageFailure(Exception):
    def __init__(self, error: Exception, evidence: FailureEvidence) -> None:
        super().__init__(str(error))
        self.error = error
        self.evidence = evidence


def _require_snapshot_id(snapshot_id: str) -> None:
    if snapshot_id in {".", ".."} or not _SNAPSHOT_ID.fullmatch(snapshot_id):
        raise ValueError("snapshot_id must match [a-zA-Z0-9._-]+ and cannot be . or ..")


def _reject_symlink(path: Path) -> None:
    if path.is_symlink():
        raise ValueError(f"symlinked snapshot path is not allowed: {path}")


def _ensure_real_directory(path: Path) -> None:
    _reject_symlink(path)
    if path.exists() and not path.is_dir():
        raise ValueError(f"snapshot path is not a directory: {path}")
    path.mkdir(parents=True, exist_ok=True)
    _reject_symlink(path)


def _require_descendant(path: Path, root: Path) -> None:
    resolved_root = root.resolve(strict=True)
    resolved_path = path.resolve(strict=False)
    if not resolved_path.is_relative_to(resolved_root):
        raise ValueError(f"snapshot path escapes its layout root: {path}")


def _prepare_layout(layout: SnapshotLayout) -> Path:
    """Create real layout roots and prove every subsequent write remains contained."""
    _ensure_real_directory(layout.data_root)
    staging_root = layout.data_root / "staging"
    silver_root = layout.data_root / "silver"
    manifests_root = layout.data_root / "manifests"
    locks_root = layout.data_root / "locks"
    for root in (staging_root, silver_root, manifests_root, locks_root):
        _ensure_real_directory(root)
    for target, root in (
        (layout.staging, staging_root),
        (layout.promoted, silver_root),
        (layout.manifest, manifests_root),
        (layout.current_manifest, manifests_root),
    ):
        _reject_symlink(target)
        _require_descendant(target, root)
    return locks_root


def _owner_is_stale(owner_path: Path) -> bool:
    try:
        payload = json.loads(owner_path.read_text(encoding="utf-8"))
        pid = int(payload["pid"])
    except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError):
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return True
    except PermissionError:
        return False
    return False


def _remove_stale_lock(lock_path: Path) -> bool:
    _reject_symlink(lock_path)
    owner_path = lock_path / "owner.json"
    _reject_symlink(owner_path)
    if not _owner_is_stale(owner_path):
        return False
    try:
        owner_path.unlink()
        lock_path.rmdir()
    except OSError:
        return False
    fsync_directory(lock_path.parent)
    return True


@contextmanager
def _snapshot_claim(locks_root: Path, snapshot_id: str) -> Iterator[None]:
    """Hold an exclusive, stale-owner-recoverable claim for one snapshot ID."""
    lock_path = locks_root / f"{snapshot_id}.lock"
    _require_descendant(lock_path, locks_root)
    deadline = time.monotonic() + _LOCK_WAIT_SECONDS
    while True:
        _reject_symlink(lock_path)
        try:
            lock_path.mkdir()
        except FileExistsError:
            if _remove_stale_lock(lock_path):
                continue
            if time.monotonic() >= deadline:
                raise TimeoutError(f"timed out waiting for snapshot claim: {snapshot_id}") from None
            time.sleep(_LOCK_POLL_SECONDS)
            continue
        break
    owner_path = lock_path / "owner.json"
    try:
        with owner_path.open("x", encoding="utf-8", newline="\n") as owner:
            owner.write(
                json.dumps({"pid": os.getpid(), "started_at": datetime.now(UTC).isoformat()})
            )
            owner.flush()
            os.fsync(owner.fileno())
        fsync_directory(lock_path)
        yield
    finally:
        with suppress(OSError):
            owner_path.unlink()
        with suppress(OSError):
            lock_path.rmdir()
            fsync_directory(locks_root)


def _source_checksums(sources: Sequence[Path]) -> tuple[str, ...]:
    return tuple(sorted(sha256_file(Path(source)) for source in sources))


def _manifest_checksums(snapshot: SnapshotResult) -> tuple[str, ...]:
    identities = [table.source for table in snapshot.tables]
    if snapshot.failure is not None and snapshot.failure.source is not None:
        identities.append(snapshot.failure.source)
    return tuple(sorted({identity.sha256 for identity in identities}))


def _same_sources(snapshot: SnapshotResult, sources: Sequence[Path]) -> bool:
    expected = _manifest_checksums(snapshot)
    return bool(expected) and _source_checksums(sources) == expected


def _source_identity(path: Path) -> SourceIdentity:
    inspected = inspect_source(path)
    encoding = select_encoding(inspected.sample)
    return SourceIdentity(
        path=str(inspected.path),
        filename=inspected.member or path.name,
        sha256=inspected.sha256,
        byte_size=inspected.byte_size,
        archive_member=inspected.member,
        encoding=encoding,
    )


def _sources_by_table(sources: Sequence[Path]) -> dict[TableKind, _SourceCandidate]:
    """Identify validated source families before parsing their headers."""
    discovered: dict[TableKind, _SourceCandidate] = {}
    for raw_path in sources:
        path = Path(raw_path)
        try:
            source = _source_identity(path)
        except Exception as error:
            raise _StageFailure(
                error,
                FailureEvidence(
                    phase="source_inspection",
                    source_path=str(path),
                    error_type=type(error).__name__,
                    message=str(error),
                ),
            ) from error
        matches = tuple(
            spec
            for spec in TABLE_SPECS
            if any(token in source.filename.lower() for token in spec.filename_tokens)
        )
        if len(matches) != 1:
            schema_error = SchemaMismatch(
                f"filename {source.filename!r} does not match exactly one table family"
            )
            raise _StageFailure(
                schema_error,
                FailureEvidence(
                    phase="source_identification",
                    source_path=str(path),
                    source=source,
                    error_type=type(schema_error).__name__,
                    message=str(schema_error),
                ),
            ) from schema_error
        kind = matches[0].kind
        if kind in discovered:
            raise ValueError(f"multiple sources detected for {kind.value}")
        discovered[kind] = _SourceCandidate(kind, path, source)
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


def _process_table(
    layout: SnapshotLayout,
    snapshot_id: str,
    candidate: _SourceCandidate,
    settings: Settings,
) -> TableResult:
    try:
        bronze = parse_to_bronze(
            candidate.path,
            layout.bronze / f"{candidate.table.value}.parquet",
            layout.rejects / f"{candidate.table.value}.jsonl",
            settings.batch_rows,
            settings.parser_version,
        )
        normalization = normalize_bronze(
            Path(bronze.bronze_path),
            layout.silver / f"{candidate.table.value}.parquet",
            spec_for(candidate.table),
            snapshot_id,
        )
    except Exception as error:
        evidence = FailureEvidence(
            phase="table_processing",
            table=candidate.table,
            source_path=str(candidate.path),
            source=candidate.source,
            error_type=type(error).__name__,
            message=str(error),
        )
        raise _StageFailure(error, evidence) from error
    return TableResult(
        table=candidate.table,
        source=bronze.source,
        bronze_path=bronze.bronze_path,
        silver_path=normalization.silver_path,
        reject_path=bronze.reject_path,
        stats=bronze.stats,
        quality=[
            _date_parse_quality(normalization.date_parse_failure_count, bronze.stats.rows_accepted)
        ],
    )


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
    evidence: FailureEvidence,
    promotion: PromotionIntent | None = None,
) -> SnapshotResult:
    return running.model_copy(
        update={
            "finished_at": datetime.now(UTC),
            "status": RunStatus.FAILED,
            "tables": list(tables),
            "quality": list(quality),
            "promoted_path": None,
            "failure": evidence,
            "promotion": promotion,
        }
    )


def _promoted_snapshot(validated: SnapshotResult, layout: SnapshotLayout) -> SnapshotResult:
    return validated.model_copy(
        update={
            "status": RunStatus.PROMOTED,
            "promoted_path": str(layout.promoted),
            "failure": None,
            "promotion": PromotionIntent(phase="current_pending"),
            "tables": [
                table.model_copy(
                    update={"silver_path": str(layout.promoted / Path(table.silver_path).name)}
                )
                for table in validated.tables
            ],
        }
    )


def _finalize_current(layout: SnapshotLayout, promoted: SnapshotResult) -> SnapshotResult:
    """Publish current only after output and its manifest are durable."""
    pending = promoted.model_copy(update={"promotion": PromotionIntent(phase="current_pending")})
    write_manifest(layout.manifest, pending)
    try:
        write_manifest(layout.current_manifest, pending)
    except Exception:
        return pending
    finalized = pending.model_copy(update={"promotion": None})
    try:
        write_manifest(layout.manifest, finalized)
    except Exception:
        return pending
    _reject_symlink(layout.staging)
    with suppress(OSError):
        rmtree(layout.staging)
    return finalized


def _finish_promotion(layout: SnapshotLayout, validated: SnapshotResult) -> SnapshotResult:
    """Move validated Silver and publish it, leaving interruptions recoverable."""
    try:
        if layout.promoted.exists():
            _reject_symlink(layout.promoted)
        elif layout.silver.exists():
            _reject_symlink(layout.silver)
            layout.silver.replace(layout.promoted)
            fsync_directory(layout.promoted.parent)
        else:
            raise FileNotFoundError("validated Silver output is missing during promotion")
        pending = _promoted_snapshot(validated, layout)
        write_manifest(layout.manifest, pending)
    except Exception as error:
        rollback_error: Exception | None = None
        if layout.promoted.exists() and not layout.silver.exists():
            try:
                layout.promoted.replace(layout.silver)
                fsync_directory(layout.silver.parent)
            except Exception as rollback_failure:
                rollback_error = rollback_failure
        evidence = FailureEvidence(
            phase="promotion",
            error_type=type(error).__name__,
            message=(
                str(error)
                if rollback_error is None
                else f"{error}; rollback failed: {type(rollback_error).__name__}: {rollback_error}"
            ),
        )
        if rollback_error is not None:
            recoverable = validated.model_copy(
                update={
                    "failure": evidence,
                    "promotion": PromotionIntent(phase="silver_renamed"),
                }
            )
            write_manifest(layout.manifest, recoverable)
            return recoverable
        failed = _failed_snapshot(
            validated,
            validated.tables,
            validated.quality,
            evidence,
            PromotionIntent(phase="validated"),
        )
        write_manifest(layout.manifest, failed)
        return failed
    return _finalize_current(layout, pending)


def _recover_existing(
    layout: SnapshotLayout, existing: SnapshotResult, sources: Sequence[Path]
) -> SnapshotResult:
    """Reconcile durable publication state while holding its snapshot claim."""
    if not _same_sources(existing, sources):
        raise SnapshotConflict("snapshot_id is already associated with different source checksums")
    if existing.status is RunStatus.PROMOTED:
        if existing.promotion is not None or not layout.current_manifest.exists():
            return _finalize_current(layout, existing)
        return existing
    if existing.status is RunStatus.VALIDATED and layout.promoted.exists():
        _reject_symlink(layout.promoted)
        return _finalize_current(layout, _promoted_snapshot(existing, layout))
    if existing.status is RunStatus.FAILED and existing.promotion is not None:
        return _finish_promotion(
            layout,
            existing.model_copy(
                update={"status": RunStatus.VALIDATED, "failure": None, "promoted_path": None}
            ),
        )
    return existing


def ingest_snapshot(
    snapshot_id: str, sources: Sequence[Path], settings: Settings
) -> SnapshotResult:
    """Parse, validate, and atomically promote one immutable MAUDE snapshot."""
    _require_snapshot_id(snapshot_id)
    source_paths = tuple(Path(source) for source in sources)
    layout = SnapshotLayout(settings.data_root, snapshot_id)
    locks_root = _prepare_layout(layout)
    with _snapshot_claim(locks_root, snapshot_id):
        if layout.manifest.exists():
            return _recover_existing(layout, load_manifest(layout.manifest), source_paths)

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
        tables: list[TableResult] = []
        quality: tuple[QualityResult, ...] = ()
        failure_error: Exception | None = None
        try:
            _reject_symlink(layout.staging)
            _ensure_real_directory(layout.staging)
            candidates = _sources_by_table(source_paths)
            for kind in TableKind:
                candidate = candidates.get(kind)
                if candidate is not None:
                    tables.append(_process_table(layout, snapshot_id, candidate, settings))
            previous = (
                load_manifest(layout.current_manifest) if layout.current_manifest.exists() else None
            )
            quality = run_quality_checks(tables, previous)
            if has_blocking_failure(quality):
                evidence = FailureEvidence(
                    phase="quality",
                    error_type="BlockingQualityFailure",
                    message="one or more blocking quality checks failed",
                )
                failed = _failed_snapshot(running, tables, quality, evidence)
                write_manifest(layout.manifest, failed)
                return failed
            validated = running.model_copy(
                update={
                    "finished_at": datetime.now(UTC),
                    "status": RunStatus.VALIDATED,
                    "tables": tables,
                    "quality": list(quality),
                    "promotion": PromotionIntent(phase="validated"),
                }
            )
            write_manifest(layout.manifest, validated)
            return _finish_promotion(layout, validated)
        except _StageFailure as failure:
            failure_error = failure.error
            evidence = failure.evidence
        except Exception as error:
            failure_error = error
            evidence = FailureEvidence(
                phase="staging_setup" if not layout.staging.exists() else "pipeline",
                error_type=type(error).__name__,
                message=str(error),
            )
        assert failure_error is not None
        failed = _failed_snapshot(
            running, tables, (*quality, _failure_quality(failure_error)), evidence
        )
        try:
            write_manifest(layout.manifest, failed)
        except Exception:
            raise failure_error from None
        return failed
