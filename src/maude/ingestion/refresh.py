import hashlib
import json
from collections.abc import Callable
from datetime import UTC, datetime
from uuid import uuid4

from maude.config import Settings
from maude.domain.enums import RefreshOutcome, RunStatus
from maude.domain.models import (
    DiscoveredArtifact,
    DownloadedArtifact,
    RefreshFailure,
    RefreshRunResult,
)
from maude.ingestion.download import download_artifact
from maude.ingestion.fda_catalog import fetch_current_catalog
from maude.ingestion.http import UrlOpener, open_url
from maude.ingestion.manifests import load_manifest, write_json_model
from maude.ingestion.pipeline import ingest_role_snapshot
from maude.storage.layout import RefreshLayout


def _fingerprint(artifacts: tuple[DownloadedArtifact, ...]) -> str:
    identity = [
        {
            "table": artifact.table.value,
            "role": artifact.role.value,
            "sha256": artifact.sha256,
        }
        for artifact in artifacts
    ]
    encoded = json.dumps(identity, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def _terminal_failure(
    running: RefreshRunResult,
    finished_at: datetime,
    phase: str,
    error: Exception,
    artifact: DiscoveredArtifact | None,
) -> RefreshRunResult:
    return running.model_copy(
        update={
            "finished_at": finished_at,
            "outcome": RefreshOutcome.FAILED,
            "failure": RefreshFailure(
                phase=phase,
                table=artifact.table if artifact is not None else None,
                role=artifact.role if artifact is not None else None,
                url=artifact.url if artifact is not None else None,
                error_type=type(error).__name__,
                message=str(error),
            ),
        }
    )


def refresh_fda(
    settings: Settings,
    opener: UrlOpener | None = None,
    now: Callable[[], datetime] | None = None,
) -> RefreshRunResult:
    """Refresh the current-year FDA source set without risking the live pointer."""
    clock = now or (lambda: datetime.now(UTC))
    http_open = opener or open_url
    run_id = uuid4()
    layout = RefreshLayout(settings.data_root, run_id)
    running = RefreshRunResult(
        run_id=run_id,
        started_at=clock(),
        finished_at=None,
        outcome=RefreshOutcome.RUNNING,
    )
    write_json_model(layout.run_manifest, running)
    phase = "catalog"
    active_artifact: DiscoveredArtifact | None = None
    try:
        catalog, discovered = fetch_current_catalog(http_open, layout, clock())
        running = running.model_copy(update={"catalog": catalog})
        write_json_model(layout.run_manifest, running)
        phase = "download"
        downloaded: list[DownloadedArtifact] = []
        for active_artifact in discovered:
            downloaded.append(download_artifact(active_artifact, layout, http_open, clock()))
            running = running.model_copy(update={"artifacts": tuple(downloaded)})
            write_json_model(layout.run_manifest, running)
        artifacts = tuple(downloaded)
        fingerprint = _fingerprint(artifacts)
        snapshot_id = f"fda-current-{fingerprint[:16]}"
        running = running.model_copy(
            update={"refresh_fingerprint": fingerprint, "snapshot_id": snapshot_id}
        )
        current_path = settings.data_root / "manifests" / "current.json"
        if current_path.exists():
            current = load_manifest(current_path)
            if current.status is RunStatus.PROMOTED and current.snapshot_id == snapshot_id:
                unchanged = running.model_copy(
                    update={
                        "finished_at": clock(),
                        "outcome": RefreshOutcome.UNCHANGED,
                        "reconciliation": current.reconciliation,
                    }
                )
                write_json_model(layout.run_manifest, unchanged)
                return unchanged
        phase = "snapshot"
        active_artifact = None
        snapshot = ingest_role_snapshot(snapshot_id, artifacts, settings)
        if snapshot.status is not RunStatus.PROMOTED or snapshot.promotion is not None:
            failure = snapshot.failure
            failed = running.model_copy(
                update={
                    "finished_at": clock(),
                    "outcome": RefreshOutcome.FAILED,
                    "reconciliation": snapshot.reconciliation,
                    "failure": RefreshFailure(
                        phase=failure.phase if failure is not None else "snapshot",
                        table=failure.table if failure is not None else None,
                        error_type=(
                            failure.error_type if failure is not None else "IncompletePromotion"
                        ),
                        message=(
                            failure.message
                            if failure is not None
                            else "snapshot did not complete promotion"
                        ),
                    ),
                }
            )
            write_json_model(layout.run_manifest, failed)
            return failed
        promoted = running.model_copy(
            update={
                "finished_at": clock(),
                "outcome": RefreshOutcome.PROMOTED,
                "reconciliation": snapshot.reconciliation,
            }
        )
        write_json_model(layout.run_manifest, promoted)
        return promoted
    except Exception as error:
        failed = _terminal_failure(running, clock(), phase, error, active_artifact)
        write_json_model(layout.run_manifest, failed)
        return failed
