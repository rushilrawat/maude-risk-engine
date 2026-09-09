from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

import pytest
from typer.testing import CliRunner

from maude import cli
from maude.domain.enums import RefreshOutcome, SourceRole, TableKind
from maude.domain.models import (
    CatalogEvidence,
    ReconciliationStats,
    RefreshFailure,
    RefreshRunResult,
)

NOW = datetime(2026, 9, 9, tzinfo=UTC)
SNAPSHOT_ID = "fda-current-0123456789abcdef"


def _result(
    outcome: RefreshOutcome,
    *,
    failure: RefreshFailure | None = None,
) -> RefreshRunResult:
    return RefreshRunResult(
        run_id=UUID(int=1),
        started_at=NOW,
        finished_at=NOW,
        outcome=outcome,
        catalog=CatalogEvidence(
            catalog_url="https://www.fda.gov/catalog",
            retrieved_at=NOW,
            final_url="https://www.fda.gov/catalog",
            sha256="a" * 64,
            html_path="/data/raw/catalogs/catalog.html",
        ),
        reconciliation=(
            ReconciliationStats(
                table=TableKind.MASTER,
                role=SourceRole.BASE,
                inserted=2,
                updated=0,
                unchanged=0,
                superseded=0,
            ),
        ),
        refresh_fingerprint="b" * 64,
        snapshot_id=SNAPSHOT_ID,
        failure=failure,
    )


@pytest.mark.parametrize(
    ("outcome", "prefix"),
    (
        (RefreshOutcome.PROMOTED, f"PROMOTED {SNAPSHOT_ID}"),
        (RefreshOutcome.UNCHANGED, f"UNCHANGED {SNAPSHOT_ID}"),
    ),
)
def test_refresh_command_reports_success(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    outcome: RefreshOutcome,
    prefix: str,
) -> None:
    monkeypatch.setenv("MAUDE_DATA_ROOT", str(tmp_path / "data"))
    monkeypatch.setattr(cli, "refresh_fda", lambda settings: _result(outcome), raising=False)

    result = CliRunner().invoke(cli.app, ["refresh-fda"])

    assert result.exit_code == 0
    assert result.stdout.startswith(prefix)
    assert "catalog=2026-09-09T00:00:00+00:00" in result.stdout
    assert "master=2" in result.stdout


def test_refresh_command_reports_failure_manifest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("MAUDE_DATA_ROOT", str(tmp_path / "data"))
    failure = RefreshFailure(
        phase="download",
        table=TableKind.MASTER,
        role=SourceRole.BASE,
        url="https://www.accessdata.fda.gov/MAUDE/ftparea/mdrfoi.zip",
        error_type="DownloadError",
        message="download failed",
    )
    monkeypatch.setattr(
        cli,
        "refresh_fda",
        lambda settings: _result(RefreshOutcome.FAILED, failure=failure),
        raising=False,
    )

    result = CliRunner().invoke(cli.app, ["refresh-fda"])

    assert result.exit_code == 1
    assert result.stdout.startswith("FAILED download ")
    assert "refresh-runs/00000000-0000-0000-0000-000000000001.json" in result.stdout
