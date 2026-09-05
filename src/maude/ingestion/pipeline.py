"""Snapshot ingestion orchestration with recoverable atomic Silver promotion.

The data root is a private, operator-controlled workspace: this pipeline does
not attempt to defend PyArrow output path operations from an adversarial
same-user process that can replace paths concurrently.  It still rejects
pre-existing symlinks and invalid layout entries, and uses no-follow opening
for the per-snapshot advisory claim to avoid lock redirection races.
"""

import fcntl
import hashlib
import json
import os
import re
import stat
import tempfile
from collections.abc import Callable, Iterator, Sequence
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
    InspectedSource,
    PromotionIntent,
    QualityResult,
    SnapshotResult,
    SourceIdentity,
    TableResult,
)
from maude.ingestion.archive import inspect_source, select_source_encoding, sha256_file
from maude.ingestion.manifests import fsync_directory, load_manifest, write_manifest
from maude.ingestion.normalize import normalize_bronze
from maude.ingestion.parser import parse_to_bronze
from maude.ingestion.schemas import TABLE_SPECS, SchemaMismatch, spec_for
from maude.quality.checks import has_blocking_failure, run_quality_checks
from maude.storage.layout import SnapshotLayout

_SNAPSHOT_ID = re.compile(r"[a-zA-Z0-9._-]+\Z")
_CLAIM_HOOK: Callable[[], None] | None = None
_PRE_FLOCK_HOOK: Callable[[], None] | None = None
_COPY_BLOCK_SIZE = 1024 * 1024


class SnapshotConflict(ValueError):
    """Raised when an immutable snapshot ID is reused for different sources."""


@dataclass(frozen=True)
class _SourceCandidate:
    table: TableKind
    path: Path
    staged_path: Path
    source: SourceIdentity
    inspected: InspectedSource


class _StageFailure(Exception):
    def __init__(
        self,
        error: Exception,
        evidence: FailureEvidence,
        inputs: Sequence[SourceIdentity] = (),
    ) -> None:
        super().__init__(str(error))
        self.error = error
        self.evidence = evidence
        self.inputs = tuple(inputs)


class _SourceIdentificationFailure(Exception):
    """Carry a completed source identity when family recognition fails."""

    def __init__(self, error: Exception, source: SourceIdentity) -> None:
        super().__init__(str(error))
        self.error = error
        self.source = source


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


@contextmanager
def _snapshot_claim(locks_root: Path, snapshot_id: str) -> Iterator[None]:
    """Hold an advisory claim that the OS releases when the owner dies.

    Metadata is informative only: an empty or truncated file is safe because
    ``flock`` guards the actual exclusion and is released by process death.
    """
    lock_path = locks_root / f"{snapshot_id}.lock"
    _require_descendant(lock_path, locks_root)
    _reject_symlink(lock_path)
    if lock_path.exists() and lock_path.is_dir():
        raise ValueError(f"legacy directory claim must be removed safely: {lock_path}")
    flags = os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW
    descriptor = os.open(lock_path, flags, 0o600)
    try:
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise ValueError(f"snapshot claim is not a regular file: {lock_path}")
        if _PRE_FLOCK_HOOK is not None:
            _PRE_FLOCK_HOOK()
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        try:
            with os.fdopen(
                descriptor, "r+", encoding="utf-8", newline="\n", closefd=False
            ) as owner:
                owner.seek(0)
                owner.truncate()
                owner.write(
                    json.dumps({"pid": os.getpid(), "started_at": datetime.now(UTC).isoformat()})
                )
                owner.flush()
                os.fsync(descriptor)
            fsync_directory(locks_root)
            if _CLAIM_HOOK is not None:
                _CLAIM_HOOK()
            yield
        finally:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
    finally:
        os.close(descriptor)


def _source_checksums(sources: Sequence[Path]) -> tuple[str, ...]:
    return tuple(sorted(sha256_file(Path(source)) for source in sources))


def _manifest_checksums(snapshot: SnapshotResult) -> tuple[str, ...]:
    identities = list(snapshot.inputs) or [table.source for table in snapshot.tables]
    if snapshot.failure is not None and snapshot.failure.source is not None:
        identities.append(snapshot.failure.source)
    return tuple(sorted({identity.sha256 for identity in identities}))


def _same_sources(snapshot: SnapshotResult, sources: Sequence[Path]) -> bool:
    expected = _manifest_checksums(snapshot)
    return bool(expected) and _source_checksums(sources) == expected


def _source_identity(path: Path, inspected: InspectedSource) -> SourceIdentity:
    encoding = select_source_encoding(path, inspected)
    return SourceIdentity(
        path=str(path),
        filename=inspected.member or path.name,
        sha256=inspected.sha256,
        byte_size=inspected.byte_size,
        archive_member=inspected.member,
        encoding=encoding,
    )


def _prepare_source_staging(layout: SnapshotLayout) -> None:
    """Prepare the private source-copy leaf before reading any FDA input."""
    _ensure_real_directory(layout.staging)
    _reject_symlink(layout.sources)
    _require_descendant(layout.sources, layout.staging)
    _ensure_real_directory(layout.sources)
    _reject_symlink(layout.sources)


def _stage_source(layout: SnapshotLayout, path: Path, index: int) -> _SourceCandidate:
    """Copy one source in bounded blocks, sync it, and inspect only that copy."""
    staged_path = layout.sources / f"{index:02d}-{path.name}"
    _require_descendant(staged_path, layout.sources)
    _reject_symlink(staged_path)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{staged_path.name}.", suffix=".tmp", dir=staged_path.parent
    )
    temporary_path = Path(temporary_name)
    digest = hashlib.sha256()
    byte_size = 0
    try:
        with os.fdopen(descriptor, "wb") as staged_handle, path.open("rb") as source_handle:
            for block in iter(lambda: source_handle.read(_COPY_BLOCK_SIZE), b""):
                digest.update(block)
                byte_size += len(block)
                staged_handle.write(block)
            staged_handle.flush()
            os.fsync(staged_handle.fileno())
        _reject_symlink(staged_path)
        temporary_path.replace(staged_path)
        fsync_directory(staged_path.parent)
        inspected = inspect_source(staged_path)
        if inspected.sha256 != digest.hexdigest() or inspected.byte_size != byte_size:
            raise ValueError("staged source bytes do not match the copied source checksum")
    except BaseException:
        temporary_path.unlink(missing_ok=True)
        raise
    source = _source_identity(path, inspected)
    matches = tuple(
        spec
        for spec in TABLE_SPECS
        if any(token in source.filename.lower() for token in spec.filename_tokens)
    )
    if len(matches) != 1:
        error = SchemaMismatch(
            f"filename {source.filename!r} does not match exactly one table family"
        )
        raise _SourceIdentificationFailure(error, source) from error
    return _SourceCandidate(matches[0].kind, path, staged_path, source, inspected)


def _sources_by_table(
    layout: SnapshotLayout, sources: Sequence[Path]
) -> dict[TableKind, _SourceCandidate]:
    """Stage and identify each source before the durable RUNNING transition."""
    discovered: dict[TableKind, _SourceCandidate] = {}
    collected: list[SourceIdentity] = []
    for index, raw_path in enumerate(sources):
        path = Path(raw_path)
        try:
            candidate = _stage_source(layout, path, index)
        except _SourceIdentificationFailure as failure:
            error = failure.error
            raise _StageFailure(
                error,
                FailureEvidence(
                    phase="source_identification",
                    source_path=str(path),
                    source=failure.source,
                    error_type=type(error).__name__,
                    message=str(error),
                ),
                [*collected, failure.source],
            ) from error
        except Exception as error:
            raise _StageFailure(
                error,
                FailureEvidence(
                    phase="source_inspection",
                    source_path=str(path),
                    error_type=type(error).__name__,
                    message=str(error),
                ),
                collected,
            ) from error
        collected.append(candidate.source)
        if candidate.table in discovered:
            duplicate_error = ValueError(f"multiple sources detected for {candidate.table.value}")
            raise _StageFailure(
                duplicate_error,
                FailureEvidence(
                    phase="source_identification",
                    table=candidate.table,
                    source_path=str(path),
                    source=candidate.source,
                    error_type=type(duplicate_error).__name__,
                    message=str(duplicate_error),
                ),
                collected,
            ) from duplicate_error
        discovered[candidate.table] = candidate
    return discovered


def _prepare_staging(layout: SnapshotLayout) -> None:
    """Create only contained, real staging leaves before any output writer opens."""
    _ensure_real_directory(layout.staging)
    for leaf in (layout.bronze, layout.rejects, layout.silver):
        _reject_symlink(leaf)
        _require_descendant(leaf, layout.staging)
        _ensure_real_directory(leaf)
        _reject_symlink(leaf)


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
            candidate.staged_path,
            layout.bronze / f"{candidate.table.value}.parquet",
            layout.rejects / f"{candidate.table.value}.jsonl",
            settings.batch_rows,
            settings.parser_version,
            candidate.inspected,
            candidate.source,
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


def _failed_discovery_snapshot(
    snapshot_id: str,
    settings: Settings,
    started_at: datetime,
    inputs: Sequence[SourceIdentity],
    evidence: FailureEvidence,
) -> SnapshotResult:
    """Record a source-stage failure before a full RUNNING manifest exists."""
    return SnapshotResult(
        run_id=uuid4(),
        snapshot_id=snapshot_id,
        started_at=started_at,
        finished_at=datetime.now(UTC),
        status=RunStatus.FAILED,
        tables=[],
        quality=[],
        promoted_path=None,
        parser_version=settings.parser_version,
        inputs=list(inputs),
        failure=evidence,
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


def _validate_promoted_artifacts(layout: SnapshotLayout, snapshot: SnapshotResult) -> None:
    """Require a real promoted directory with every durable-table Parquet artifact."""
    _reject_symlink(layout.promoted)
    if not layout.promoted.is_dir():
        raise ValueError("promoted target is not a real directory")
    expected = {f"{table.table.value}.parquet" for table in snapshot.tables}
    for filename in expected:
        artifact = layout.promoted / filename
        _reject_symlink(artifact)
        if not artifact.is_file():
            raise FileNotFoundError(f"promoted output is missing expected artifact: {filename}")


def _finalize_current(layout: SnapshotLayout, promoted: SnapshotResult) -> SnapshotResult:
    """Publish current only after output and its manifest are durable."""
    _validate_promoted_artifacts(layout, promoted)
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
    with suppress(Exception):
        rmtree(layout.staging)
    return finalized


def _finish_promotion(layout: SnapshotLayout, validated: SnapshotResult) -> SnapshotResult:
    """Move validated Silver and publish it, leaving interruptions recoverable."""
    try:
        _prepare_staging(layout)
        promotion_phase = validated.promotion.phase if validated.promotion is not None else ""
        if layout.promoted.exists():
            if promotion_phase not in {"rename_pending", "silver_renamed", "current_pending"}:
                raise FileExistsError("promoted target already exists for a new snapshot run")
            _validate_promoted_artifacts(layout, validated)
        elif layout.silver.exists():
            if promotion_phase == "validated":
                rename_intent = validated.model_copy(
                    update={"promotion": PromotionIntent(phase="rename_pending")}
                )
                write_manifest(layout.manifest, rename_intent)
                validated = rename_intent
            _reject_symlink(layout.silver)
            layout.silver.replace(layout.promoted)
            fsync_directory(layout.promoted.parent)
            _validate_promoted_artifacts(layout, validated)
        else:
            raise FileNotFoundError("validated Silver output is missing during promotion")
        pending = _promoted_snapshot(validated, layout)
        write_manifest(layout.manifest, pending)
        return _finalize_current(layout, pending)
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


def _recover_existing(
    layout: SnapshotLayout, existing: SnapshotResult, sources: Sequence[Path]
) -> SnapshotResult:
    """Reconcile durable publication state while holding its snapshot claim."""
    if existing.status is RunStatus.RUNNING:
        if existing.inputs and not _same_sources(existing, sources):
            raise SnapshotConflict(
                "snapshot_id is already associated with different source checksums"
            )
        interrupted = _failed_snapshot(
            existing,
            existing.tables,
            existing.quality,
            FailureEvidence(
                phase="interrupted_running",
                error_type="InterruptedRun",
                message="a prior RUNNING manifest was recovered without an active owner",
            ),
        )
        write_manifest(layout.manifest, interrupted)
        return interrupted
    if not _same_sources(existing, sources):
        raise SnapshotConflict("snapshot_id is already associated with different source checksums")
    if existing.status is RunStatus.PROMOTED:
        if existing.promotion is not None or not layout.current_manifest.exists():
            return _finalize_current(layout, existing)
        return existing
    if existing.status is RunStatus.VALIDATED:
        return _finish_promotion(layout, existing)
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
        started_at = datetime.now(UTC)
        try:
            _prepare_source_staging(layout)
            candidates = _sources_by_table(layout, source_paths)
        except _StageFailure as failure:
            failed = _failed_discovery_snapshot(
                snapshot_id,
                settings,
                started_at,
                failure.inputs,
                failure.evidence,
            )
            try:
                write_manifest(layout.manifest, failed)
            except Exception:
                raise failure.error from None
            return failed
        except Exception as error:
            evidence = FailureEvidence(
                phase="source_staging",
                error_type=type(error).__name__,
                message=str(error),
            )
            failed = _failed_discovery_snapshot(snapshot_id, settings, started_at, (), evidence)
            try:
                write_manifest(layout.manifest, failed)
            except Exception:
                raise error from None
            return failed
        inputs = [candidates[kind].source for kind in TableKind if kind in candidates]

        running = SnapshotResult(
            run_id=uuid4(),
            snapshot_id=snapshot_id,
            started_at=started_at,
            finished_at=None,
            status=RunStatus.RUNNING,
            tables=[],
            quality=[],
            promoted_path=None,
            parser_version=settings.parser_version,
            inputs=inputs,
            promotion=PromotionIntent(phase="running"),
        )
        write_manifest(layout.manifest, running)
        tables: list[TableResult] = []
        quality: tuple[QualityResult, ...] = ()
        failure_error: Exception | None = None
        staging_ready = False
        try:
            _prepare_staging(layout)
            staging_ready = True
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
                phase="pipeline" if staging_ready else "staging_setup",
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
