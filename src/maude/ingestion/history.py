"""Acquisition of immutable historical FDA MAUDE source archives."""

from collections.abc import Callable
from datetime import UTC, datetime
from uuid import uuid4

from maude.config import Settings
from maude.domain.models import (
    DiscoveredArtifact,
    DownloadedArtifact,
    HistoricalAcquisitionFailure,
    HistoricalAcquisitionRun,
)
from maude.ingestion.download import download_artifact
from maude.ingestion.fda_catalog import HISTORICAL_ARCHIVES, fetch_historical_catalog
from maude.ingestion.http import UrlOpener, open_url
from maude.ingestion.manifests import write_json_model
from maude.storage.layout import RefreshLayout


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
        phase = "download"
        downloaded: list[DownloadedArtifact] = []
        for active_artifact in discovered:
            downloaded.append(download_artifact(active_artifact, layout, http_open, clock()))
            running = running.model_copy(update={"artifacts": tuple(downloaded)})
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
