"""Deterministic human-readable quality report rendering."""

import json

from maude.domain.enums import QualityLevel
from maude.domain.models import QualityResult, SnapshotResult


def _checks(snapshot: SnapshotResult) -> tuple[QualityResult, ...]:
    checks = list(snapshot.quality)
    for table in snapshot.tables:
        checks.extend(table.quality)
    unique: dict[tuple[object, ...], QualityResult] = {}
    for check in checks:
        key = (
            check.check,
            check.level,
            check.passed,
            check.message,
            json.dumps(dict(check.metrics), sort_keys=True),
        )
        unique[key] = check
    return tuple(
        sorted(
            unique.values(),
            key=lambda result: (0 if result.level is QualityLevel.BLOCKING else 1, result.check),
        )
    )


def _metrics(result: QualityResult) -> str:
    return json.dumps(dict(sorted(result.metrics.items())), sort_keys=True, separators=(",", ":"))


def render_quality_markdown(snapshot: SnapshotResult) -> str:
    """Render a stable Markdown report for a validated or failed snapshot."""
    lines = [
        f"# MAUDE quality report: {snapshot.snapshot_id}",
        "",
        f"- Run ID: `{snapshot.run_id}`",
        f"- Snapshot ID: `{snapshot.snapshot_id}`",
        f"- Status: `{snapshot.status.value.upper()}`",
        f"- Parser version: `{snapshot.parser_version}`",
        "",
        "## Tables",
        "",
        "| Table | Source file | SHA-256 | Rows seen | Accepted | Rejected |",
        "|---|---|---|---:|---:|---:|",
    ]
    for table in sorted(snapshot.tables, key=lambda value: value.table.value):
        lines.append(
            f"| {table.table.value} | {table.source.filename} | `{table.source.sha256}` | "
            f"{table.stats.rows_seen} | {table.stats.rows_accepted} | {table.stats.rows_rejected} |"
        )
    lines.extend(
        [
            "",
            "## Quality checks",
            "",
            "| Check | Level | Passed | Message | Metrics |",
            "|---|---|---:|---|---|",
        ]
    )
    for result in _checks(snapshot):
        lines.append(
            f"| {result.check} | {result.level.value} | {str(result.passed).lower()} | "
            f"{result.message} | `{_metrics(result)}` |"
        )
    blocking_failure = any(
        result.level is QualityLevel.BLOCKING and not result.passed for result in _checks(snapshot)
    )
    outcome = (
        "not promoted (blocking quality failure)"
        if blocking_failure
        else (
            f"promoted at `{snapshot.promoted_path}`" if snapshot.promoted_path else "not promoted"
        )
    )
    lines.extend(["", "## Promotion outcome", "", outcome, ""])
    return "\n".join(lines)
