"""Deterministic human-readable quality report rendering."""

import json

from maude.domain.enums import QualityLevel
from maude.domain.models import QualityResult, SnapshotResult


def _checks(snapshot: SnapshotResult) -> tuple[tuple[str, QualityResult], ...]:
    checks: list[tuple[str, QualityResult]] = [("snapshot", result) for result in snapshot.quality]
    for table in sorted(snapshot.tables, key=lambda value: value.table.value):
        context = f"table:{table.table.value} source:{table.source.filename}"
        checks.extend((context, result) for result in table.quality)
    return tuple(
        sorted(
            checks,
            key=lambda item: (
                0 if item[1].level is QualityLevel.BLOCKING else 1,
                item[1].check,
                item[0],
                json.dumps(dict(sorted(item[1].metrics.items())), sort_keys=True),
                item[1].message,
                item[1].passed,
            ),
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
            "| Context | Check | Level | Passed | Message | Metrics |",
            "|---|---|---:|---:|---|---|",
        ]
    )
    for context, result in _checks(snapshot):
        lines.append(
            f"| {context} | {result.check} | {result.level.value} | {str(result.passed).lower()} | "
            f"{result.message} | `{_metrics(result)}` |"
        )
    blocking_failure = any(
        result.level is QualityLevel.BLOCKING and not result.passed
        for _, result in _checks(snapshot)
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
