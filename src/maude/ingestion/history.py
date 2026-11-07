"""Acquisition of immutable historical FDA MAUDE source archives."""

from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID, uuid4

from maude.config import Settings
from maude.domain.models import (
    DiscoveredArtifact,
    DownloadedArtifact,
    HistoricalAcquisitionFailure,
    HistoricalAcquisitionRun,
)
from maude.ingestion.archive import sha256_file
from maude.ingestion.download import download_artifact
from maude.ingestion.fda_catalog import HISTORICAL_ARCHIVES, fetch_historical_catalog
from maude.ingestion.http import UrlOpener, open_url
from maude.ingestion.manifests import write_json_model
from maude.storage.layout import RefreshLayout


def _prior_artifacts(
    data_root: Path, run_id: UUID, catalog_sha256: str
) -> tuple[dict[str, DownloadedArtifact], UUID | None]:
    """Find validated prior downloads tied to the identical preserved catalog."""
    manifest_root = data_root / "history-runs"
    reusable: dict[str, DownloadedArtifact] = {}
    source_run_id: UUID | None = None
    paths = sorted(
        (path for path in manifest_root.glob("*.json") if not path.name.startswith("._")),
        key=lambda path: path.stat().st_mtime,
    )
    for path in paths:
        try:
            prior = HistoricalAcquisitionRun.model_validate_json(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        if (
            prior.run_id == run_id
            or prior.catalog is None
            or prior.catalog.sha256 != catalog_sha256
        ):
            continue
        for artifact in prior.artifacts:
            reusable[artifact.filename] = artifact
            source_run_id = prior.run_id
    return reusable, source_run_id


def _raw_artifact_matches(artifact: DownloadedArtifact, discovered: DiscoveredArtifact) -> bool:
    """Reuse only a prior source with matching catalog identity and verified bytes."""
    if (
        artifact.url != discovered.url
        or artifact.filename != discovered.filename
        or artifact.table is not discovered.table
        or artifact.role is not discovered.role
    ):
        return False
    path = Path(artifact.raw_path)
    try:
        return (
            path.is_file()
            and path.stat().st_size == artifact.byte_size
            and sha256_file(path) == artifact.sha256
        )
    except OSError:
        return False


def download_fda_history(
    settings: Settings,
    opener: UrlOpener | None = None,
    now: Callable[[], datetime] | None = None,
) -> HistoricalAcquisitionRun:
    """Download and preserve FDA core MAUDE archives through 2025.

    The command does not publish or change the current-year snapshot. It stores
    validated archives in the existing content-addressed raw store and records
    per-file provenance in a separate history-runs manifest.
    """
    clock = now or (lambda: datetime.now(UTC))
    http_open = opener or open_url
    run_id = uuid4()
    layout = RefreshLayout(settings.data_root, run_id)
    manifest_path = settings.data_root / "history-runs" / f"{run_id}.json"
    running = HistoricalAcquisitionRun(
        run_id=run_id,
        started_at=clock(),
        finished_at=None,
        status="running",
        expected_archive_count=len(HISTORICAL_ARCHIVES),
    )
    write_json_model(manifest_path, running)
    phase = "catalog"
    active_artifact: DiscoveredArtifact | None = None
    try:
        catalog, discovered = fetch_historical_catalog(http_open, layout, clock())
        running = running.model_copy(update={"catalog": catalog})
        write_json_model(manifest_path, running)
        reusable, source_run_id = _prior_artifacts(
            settings.data_root, run_id, catalog.sha256
        )
        phase = "download"
        downloaded: list[DownloadedArtifact] = []
        resumed_from: UUID | None = None
        for active_artifact in discovered:
            prior = reusable.get(active_artifact.filename)
            if prior is not None and _raw_artifact_matches(prior, active_artifact):
                downloaded.append(prior)
                resumed_from = source_run_id
            else:
                downloaded.append(download_artifact(active_artifact, layout, http_open, clock()))
            running = running.model_copy(
                update={
                    "artifacts": tuple(downloaded),
                    "resumed_from": resumed_from,
                }
            )
            write_json_model(manifest_path, running)
        completed = running.model_copy(
            update={"finished_at": clock(), "status": "complete"}
        )
        write_json_model(manifest_path, completed)
        return completed
    except Exception as error:
        failed = running.model_copy(
            update={
                "finished_at": clock(),
                "status": "failed",
                "failure": HistoricalAcquisitionFailure(
                    phase=phase,
                    url=active_artifact.url if active_artifact is not None else None,
                    error_type=type(error).__name__,
                    message=str(error),
                ),
            }
        )
        write_json_model(manifest_path, failed)
        return failed
