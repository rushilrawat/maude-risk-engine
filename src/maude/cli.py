from pathlib import Path

import typer

from maude import __version__
from maude.config import Settings
from maude.domain.enums import QualityLevel, RefreshOutcome, RunStatus, TableKind
from maude.domain.models import LocalAuditItem, LocalAuditResult, RefreshRunResult
from maude.ingestion.manifests import load_manifest
from maude.ingestion.pipeline import ingest_snapshot
from maude.ingestion.refresh import refresh_fda
from maude.quality.report import render_quality_markdown
from maude.service.audit import audit_local_sources, blocking_findings, selected_archives
from maude.storage.layout import SnapshotLayout

app = typer.Typer(no_args_is_help=True)


@app.callback()
def main() -> None:
    """MAUDE reporting-signal analysis and triage commands."""


@app.command()
def version() -> None:
    """Print the installed application version."""
    typer.echo(__version__)


def _print_audit(audit: LocalAuditResult) -> None:
    for item in audit.items:
        schema = next(check for check in item.quality if check.check == "schema_detection")
        typer.echo(
            "ARCHIVE "
            f"{Path(item.archive_path).name} sha256={item.sha256[:12]} "
            f"member={item.archive_member} "
            f"encoding={item.encoding} table={item.table.value} header={schema.metrics['header']}"
        )
        if item.converted_path is not None:
            conversion_ratio = item.conversion_row_ratio
            ratio = "n/a" if conversion_ratio is None else f"{conversion_ratio:.3f}"
            typer.echo(
                f"CONVERSION {Path(item.converted_path).name} ratio={ratio} "
                f"result={'passed' if not blocking_findings_for_item(item) else 'blocking'}"
            )
        for check in item.quality:
            if not check.passed:
                typer.echo(f"{check.level.value.upper()} {check.check}: {check.message}")
    for check in audit.quality:
        outcome = "PASS" if check.passed else check.level.value.upper()
        typer.echo(f"{outcome} {check.check}: {check.message}")


def blocking_findings_for_item(item: LocalAuditItem) -> tuple[object, ...]:
    return tuple(
        check for check in item.quality if check.level is QualityLevel.BLOCKING and not check.passed
    )


def _current_points_at_snapshot(layout: SnapshotLayout, snapshot_id: str) -> bool:
    if not layout.current_manifest.is_file():
        return False
    try:
        current = load_manifest(layout.current_manifest)
    except (OSError, ValueError):
        return False
    return current.snapshot_id == snapshot_id and current.status is RunStatus.PROMOTED


def _selected_counts(result: RefreshRunResult) -> str:
    counts = {table: 0 for table in TableKind}
    for stats in result.reconciliation:
        counts[stats.table] += stats.inserted + stats.updated + stats.unchanged - stats.superseded
    return ", ".join(f"{table.value}={counts[table]}" for table in TableKind)


@app.command("refresh-fda")
def refresh_fda_command() -> None:
    """Download and publish the latest official FDA MAUDE current-year files."""
    settings = Settings.model_validate({})
    result = refresh_fda(settings)
    if result.outcome in {RefreshOutcome.PROMOTED, RefreshOutcome.UNCHANGED}:
        catalog_time = (
            result.catalog.retrieved_at.isoformat() if result.catalog is not None else "unknown"
        )
        typer.echo(
            f"{result.outcome.value.upper()} {result.snapshot_id or 'unknown'} "
            f"catalog={catalog_time} selected: {_selected_counts(result)}"
        )
        return
    failure = result.failure
    phase = failure.phase if failure is not None else "unknown"
    detail = "unknown failure" if failure is None else f"{failure.error_type}: {failure.message}"
    evidence = settings.data_root / "refresh-runs" / f"{result.run_id}.json"
    typer.echo(f"FAILED {phase} {detail} evidence={evidence}")
    raise typer.Exit(1)


@app.command("audit-local")
def audit_local(
    data_root: Path = typer.Option(  # noqa: B008
        ..., help="Directory containing local MAUDE source archives."
    ),
) -> None:
    """Read and validate local canonical archives without modifying them."""
    result = audit_local_sources(data_root)
    _print_audit(result)
    if result.has_blocking_failure:
        raise typer.Exit(1)


@app.command("ingest-local")
def ingest_local(
    snapshot_id: str,
    source_root: Path = typer.Option(  # noqa: B008
        ..., help="Directory containing local MAUDE source archives."
    ),
    data_root: Path | None = typer.Option(  # noqa: B008
        None, help="Destination data root; defaults to MAUDE_DATA_ROOT."
    ),
    allow_archive_only: bool = typer.Option(
        False,
        help="Ignore failed hand-converted UTF-8 siblings after canonical archive audit passes.",
    ),
) -> None:
    """Audit then ingest one local MAUDE archive snapshot."""
    audit = audit_local_sources(source_root)
    _print_audit(audit)
    findings = blocking_findings(audit, include_conversions=not allow_archive_only)
    if findings:
        typer.echo("REFUSED local ingestion: blocking audit findings remain")
        raise typer.Exit(1)
    if allow_archive_only and blocking_findings(audit, include_conversions=True):
        typer.echo("INFO ignored invalid conversion; ingesting canonical archives only")

    settings = Settings.model_validate({}) if data_root is None else Settings(data_root=data_root)
    snapshot = ingest_snapshot(snapshot_id, selected_archives(audit), settings)
    layout = SnapshotLayout(settings.data_root, snapshot_id)
    report_path = layout.manifest.parent / "quality-report.md"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(render_quality_markdown(snapshot), encoding="utf-8")
    counts = ", ".join(
        f"{table.table.value}={table.stats.rows_accepted}/{table.stats.rows_rejected}"
        for table in snapshot.tables
    )
    if (
        snapshot.status is RunStatus.PROMOTED
        and snapshot.promotion is None
        and _current_points_at_snapshot(layout, snapshot.snapshot_id)
    ):
        typer.echo(f"PROMOTED {snapshot.snapshot_id} {snapshot.promoted_path} counts: {counts}")
        return
    if snapshot.status is RunStatus.PROMOTED:
        phase = snapshot.promotion.phase if snapshot.promotion is not None else "unverified_current"
        typer.echo(
            f"INCOMPLETE {snapshot.snapshot_id} promotion is recoverable; "
            f"current pointer is not finalized (phase={phase}) counts: {counts}"
        )
        raise typer.Exit(1)
    typer.echo(f"FAILED {snapshot.snapshot_id} counts: {counts}")
    raise typer.Exit(1)
