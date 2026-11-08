"""Versioned, research-corpus-bound report-volume indicators."""

import hashlib
import json
import os
import re
import tempfile
from datetime import UTC, date, datetime
from pathlib import Path
from statistics import median
from typing import Any

import duckdb
import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]

from maude.domain.enums import RunStatus
from maude.domain.models import ModelingCorpusManifest, SignalFeatureManifest
from maude.features.documents import NARRATIVE_CLEANING_VERSION
from maude.ingestion.manifests import fsync_directory, load_manifest, write_json_model

SIGNAL_FEATURE_VERSION = "reported-severity-volume-v1"
_RESEARCH_CORPUS_ID = re.compile(r"fda-research-[0-9a-f]{16}\Z")
_SEVERITY_MAPPING = {
    "DEATH": "death",
    "INJURY": "injury",
    "MALFUNCTION": "malfunction",
    "OTHER": "other",
}
_RECENT_MONTHS = 3
_BASELINE_MONTHS = 12
_MINIMUM_ADDITIONAL_REPORTS = 5
_ROBUST_Z_THRESHOLD = 3.0
_ROBUST_Z_SCALE_FACTOR = 1.4826
_MAD_ZERO_SCALE_FLOOR = 1.0
_ARTIFACTS = (
    "report_indicators.parquet",
    "monthly_cohorts.parquet",
    "volume_change.parquet",
)
_VOLUME_CHANGE_SCHEMA = pa.schema(
    [
        ("product_code", pa.string()),
        ("baseline_window_start", pa.date32()),
        ("baseline_window_end", pa.date32()),
        ("recent_window_start", pa.date32()),
        ("recent_window_end", pa.date32()),
        ("baseline_median_monthly_reports", pa.float64()),
        ("baseline_mad_monthly_reports", pa.float64()),
        ("recent_total_reports", pa.int64()),
        ("recent_average_monthly_reports", pa.float64()),
        ("additional_reports_vs_baseline", pa.float64()),
        ("robust_z_score", pa.float64()),
        ("minimum_count_increase_met", pa.bool_()),
        ("report_volume_change_flag", pa.bool_()),
    ]
)


def _sql_literal(value: str | Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _month_start(value: date) -> date:
    return date(value.year, value.month, 1)


def _shift_month(value: date, offset: int) -> date:
    month_index = value.year * 12 + value.month - 1 + offset
    return date(month_index // 12, month_index % 12 + 1, 1)


def _artifact_hashes(directory: Path) -> dict[str, str]:
    return {name: _sha256_file(directory / name) for name in _ARTIFACTS}


def _load_existing(output_directory: Path) -> SignalFeatureManifest:
    manifest_path = output_directory / "manifest.json"
    if manifest_path.is_symlink():
        raise ValueError("signal-feature manifest must not be a symlink")
    manifest = SignalFeatureManifest.model_validate_json(manifest_path.read_text(encoding="utf-8"))
    if Path(manifest.output_directory) != output_directory:
        raise ValueError("signal-feature manifest points to an unexpected output directory")
    if set(manifest.artifact_sha256) != set(_ARTIFACTS):
        raise ValueError("signal-feature manifest has an unexpected artifact set")
    for name, expected_hash in manifest.artifact_sha256.items():
        artifact = output_directory / name
        if (
            artifact.is_symlink()
            or not artifact.is_file()
            or _sha256_file(artifact) != expected_hash
        ):
            raise ValueError(f"signal-feature artifact checksum mismatch: {name}")
    return manifest


def _volume_change_rows(
    monthly_rows: list[tuple[str | None, date, int]], latest_complete_month: date
) -> list[dict[str, Any]]:
    recent_start = _shift_month(latest_complete_month, -(_RECENT_MONTHS - 1))
    baseline_start = _shift_month(recent_start, -_BASELINE_MONTHS)
    baseline_months = tuple(
        _shift_month(baseline_start, offset) for offset in range(_BASELINE_MONTHS)
    )
    recent_months = tuple(_shift_month(recent_start, offset) for offset in range(_RECENT_MONTHS))
    counts_by_cohort: dict[str | None, dict[date, int]] = {}
    for product_code, month, report_count in monthly_rows:
        counts_by_cohort.setdefault(product_code, {})[month] = report_count

    results: list[dict[str, Any]] = []
    for product_code, counts in sorted(
        counts_by_cohort.items(), key=lambda item: (item[0] is not None, item[0] or "")
    ):
        baseline = [counts.get(month, 0) for month in baseline_months]
        recent_total = sum(counts.get(month, 0) for month in recent_months)
        baseline_median = float(median(baseline))
        baseline_mad = float(median(abs(count - baseline_median) for count in baseline))
        recent_average = recent_total / _RECENT_MONTHS
        scale = max(_ROBUST_Z_SCALE_FACTOR * baseline_mad, _MAD_ZERO_SCALE_FLOOR)
        robust_z = (recent_average - baseline_median) / scale
        additional_reports = recent_total - baseline_median * _RECENT_MONTHS
        count_increase_met = additional_reports >= _MINIMUM_ADDITIONAL_REPORTS
        results.append(
            {
                "product_code": product_code,
                "baseline_window_start": baseline_start,
                "baseline_window_end": _shift_month(recent_start, -1),
                "recent_window_start": recent_start,
                "recent_window_end": latest_complete_month,
                "baseline_median_monthly_reports": baseline_median,
                "baseline_mad_monthly_reports": baseline_mad,
                "recent_total_reports": recent_total,
                "recent_average_monthly_reports": recent_average,
                "additional_reports_vs_baseline": additional_reports,
                "robust_z_score": robust_z,
                "minimum_count_increase_met": count_increase_met,
                "report_volume_change_flag": (
                    robust_z >= _ROBUST_Z_THRESHOLD and count_increase_met
                ),
            }
        )
    return results


def build_signal_features(data_root: Path, research_corpus_id: str) -> SignalFeatureManifest:
    """Build indicators from the disjoint snapshots in a combined research corpus."""
    if not _RESEARCH_CORPUS_ID.fullmatch(research_corpus_id):
        raise ValueError("research_corpus_id must be a generated fda-research corpus ID")

    root = Path(data_root).resolve()
    corpus_directory = root / "corpora" / research_corpus_id / NARRATIVE_CLEANING_VERSION
    corpus_manifest_path = corpus_directory / "manifest.json"
    if corpus_manifest_path.is_symlink():
        raise ValueError("research-corpus manifest must not be a symlink")
    corpus = ModelingCorpusManifest.model_validate_json(
        corpus_manifest_path.read_text(encoding="utf-8")
    )
    source_ids = tuple(corpus.source_snapshot_ids)
    source_corpus_hashes = dict(corpus.source_corpus_sha256)
    expected_corpus_output = corpus_directory / "documents.jsonl"
    if (
        corpus.snapshot_id != research_corpus_id
        or corpus.corpus_format_version != "1"
        or corpus.cleaning_version != NARRATIVE_CLEANING_VERSION
        or corpus.promoted_snapshot_path is not None
        or len(source_ids) < 2
        or source_ids != tuple(sorted(source_ids))
        or len(set(source_ids)) != len(source_ids)
        or tuple(source_corpus_hashes) != source_ids
        or len(corpus.source_corpus_sha256) != len(source_ids)
        or Path(corpus.manifest_path) != corpus_manifest_path
        or Path(corpus.output_path) != expected_corpus_output
        or expected_corpus_output.is_symlink()
        or not expected_corpus_output.is_file()
    ):
        raise ValueError("input must be a valid composed research corpus")
    identity = {
        "corpus_format_version": corpus.corpus_format_version,
        "cleaning_version": corpus.cleaning_version,
        "source_corpora": corpus.source_corpus_sha256,
    }
    encoded_identity = json.dumps(identity, sort_keys=True, separators=(",", ":")).encode()
    expected_research_id = f"fda-research-{hashlib.sha256(encoded_identity).hexdigest()[:16]}"
    if expected_research_id != research_corpus_id:
        raise ValueError("research-corpus ID does not match its recorded source corpus hashes")

    source_manifest_hashes: dict[str, str] = {}
    master_paths: list[Path] = []
    device_paths: list[Path] = []
    snapshot_finished_at: list[datetime] = []
    expected_source_paths = tuple(str(root / "silver" / source_id) for source_id in source_ids)
    if corpus.source_snapshot_paths != expected_source_paths:
        raise ValueError("research corpus source paths do not match this data root")

    for source_id in source_ids:
        source_corpus_directory = root / "corpora" / source_id / NARRATIVE_CLEANING_VERSION
        source_corpus_manifest_path = source_corpus_directory / "manifest.json"
        if source_corpus_manifest_path.is_symlink():
            raise ValueError(f"source-corpus manifest must not be a symlink: {source_id}")
        source_corpus = ModelingCorpusManifest.model_validate_json(
            source_corpus_manifest_path.read_text(encoding="utf-8")
        )
        expected_source_path = root / "silver" / source_id
        if (
            source_corpus.snapshot_id != source_id
            or source_corpus.cleaning_version != NARRATIVE_CLEANING_VERSION
            or Path(source_corpus.promoted_snapshot_path or "") != expected_source_path
            or source_corpus.source_snapshot_ids
            or source_corpus.output_sha256 != source_corpus_hashes[source_id]
        ):
            raise ValueError(f"research corpus has invalid source-corpus provenance: {source_id}")

        source_manifest_path = root / "manifests" / f"{source_id}.json"
        if source_manifest_path.is_symlink():
            raise ValueError(f"snapshot manifest must not be a symlink: {source_id}")
        source = load_manifest(source_manifest_path)
        if source.snapshot_id != source_id or source.status is not RunStatus.PROMOTED:
            raise ValueError(f"source snapshot is not promoted: {source_id}")
        if source.promotion is not None or source.finished_at is None:
            raise ValueError(f"source snapshot is incomplete: {source_id}")
        if Path(source.promoted_path or "") != expected_source_path:
            raise ValueError(f"source snapshot path does not match its manifest: {source_id}")

        master_path = expected_source_path / "master.parquet"
        device_path = expected_source_path / "device.parquet"
        if (
            expected_source_path.is_symlink()
            or not expected_source_path.is_dir()
            or any(path.is_symlink() or not path.is_file() for path in (master_path, device_path))
        ):
            raise FileNotFoundError(
                f"source snapshot is missing master or device data: {source_id}"
            )
        required_columns = {
            master_path: {"mdr_report_key", "date_received", "event_type", "dataset_snapshot_id"},
            device_path: {
                "mdr_report_key",
                "device_report_product_code",
                "dataset_snapshot_id",
            },
        }
        for path, required in required_columns.items():
            missing = required - set(pq.read_schema(path).names)
            if missing:
                raise ValueError(
                    f"{source_id}/{path.name} is missing required columns: "
                    f"{', '.join(sorted(missing))}"
                )
        source_manifest_hashes[source_id] = _sha256_file(source_manifest_path)
        master_paths.append(master_path)
        device_paths.append(device_path)
        snapshot_finished_at.append(source.finished_at)

    output_directory = root / "features" / research_corpus_id / SIGNAL_FEATURE_VERSION
    if output_directory.exists():
        if output_directory.is_symlink():
            raise ValueError("signal-feature output directory must not be a symlink")
        existing = _load_existing(output_directory)
        if (
            existing.research_corpus_id != research_corpus_id
            or existing.source_snapshot_ids != source_ids
            or existing.research_corpus_manifest_sha256 != _sha256_file(corpus_manifest_path)
            or dict(existing.source_snapshot_manifest_sha256) != source_manifest_hashes
        ):
            raise ValueError("existing signal features have different inputs or configuration")
        return existing

    latest_source_snapshot_finished_at = max(snapshot_finished_at)
    latest_snapshot_complete_month = _shift_month(
        _month_start(latest_source_snapshot_finished_at.date()), -1
    )
    output_directory.parent.mkdir(parents=True, exist_ok=True)
    scratch_root = root / "staging"
    scratch_root.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory(
        prefix=".signal-features-", dir=output_directory.parent
    ) as stage:
        staging = Path(stage)
        with tempfile.TemporaryDirectory(prefix="maude-signals-", dir=scratch_root) as scratch:
            connection = duckdb.connect()
            try:
                connection.execute("SET memory_limit = '2GB'")
                connection.execute("SET threads = 1")
                connection.execute("SET preserve_insertion_order = false")
                connection.execute("SET temp_directory = ?", [scratch])
                master_source_list = (
                    "[" + ", ".join(_sql_literal(path) for path in master_paths) + "]"
                )
                device_source_list = (
                    "[" + ", ".join(_sql_literal(path) for path in device_paths) + "]"
                )
                connection.execute(
                    "CREATE VIEW reports AS SELECT * FROM read_parquet("
                    + master_source_list
                    + ", union_by_name = true)"
                )
                connection.execute(
                    "CREATE VIEW devices AS SELECT * FROM read_parquet("
                    + device_source_list
                    + ", union_by_name = true)"
                )
                connection.execute(
                    """COPY (
                        SELECT DISTINCT
                            r.dataset_snapshot_id,
                            CAST(r.mdr_report_key AS VARCHAR) AS report_id,
                            TRY_CAST(r.date_received AS DATE) AS date_received,
                            NULLIF(UPPER(TRIM(CAST(d.device_report_product_code AS VARCHAR))), '')
                                AS product_code,
                            CAST(r.event_type AS VARCHAR) AS reported_event_type_raw,
                            CASE UPPER(TRIM(COALESCE(CAST(r.event_type AS VARCHAR), '')))
                                WHEN 'DEATH' THEN 'death'
                                WHEN 'INJURY' THEN 'injury'
                                WHEN 'MALFUNCTION' THEN 'malfunction'
                                WHEN 'OTHER' THEN 'other'
                                WHEN '' THEN 'missing'
                                ELSE 'unrecognized'
                            END AS reported_severity_category
                        FROM reports AS r
                        LEFT JOIN devices AS d
                          ON r.mdr_report_key = d.mdr_report_key
                    ) TO """
                    + _sql_literal(staging / _ARTIFACTS[0])
                    + " (FORMAT PARQUET)"
                )
                indicator_path = _sql_literal(staging / _ARTIFACTS[0])
                connection.execute(
                    """COPY (
                        SELECT product_code,
                               DATE_TRUNC('month', date_received)::DATE AS month,
                               COUNT(*)::BIGINT AS report_count,
                               COUNT(*) FILTER (WHERE reported_severity_category = 'death')::BIGINT
                                   AS reported_death_count,
                               COUNT(*) FILTER (WHERE reported_severity_category = 'injury')::BIGINT
                                   AS reported_injury_count,
                               COUNT(*) FILTER (
                                   WHERE reported_severity_category = 'malfunction'
                               )::BIGINT AS reported_malfunction_count,
                               COUNT(*) FILTER (
                                   WHERE reported_severity_category = 'other'
                               )::BIGINT AS reported_other_count,
                               COUNT(*) FILTER (
                                   WHERE reported_severity_category = 'missing'
                               )::BIGINT AS missing_event_type_count,
                               COUNT(*) FILTER (
                                   WHERE reported_severity_category = 'unrecognized'
                               )::BIGINT AS unrecognized_event_type_count
                        FROM read_parquet("""
                    + indicator_path
                    + ") "
                    + "WHERE date_received IS NOT NULL "
                    + "GROUP BY product_code, month) TO "
                    + _sql_literal(staging / _ARTIFACTS[1])
                    + " (FORMAT PARQUET)"
                )
                counts = connection.execute(
                    "SELECT COUNT(*), "
                    "COUNT(*) FILTER (WHERE date_received IS NULL), "
                    "MIN(date_received), MAX(date_received) "
                    f"FROM read_parquet({indicator_path})"
                ).fetchone()
                if counts is None or counts[2] is None or counts[3] is None:
                    raise ValueError("research corpus contains no dated report indicators")
                earliest_received_date = counts[2]
                latest_received_date = counts[3]
                latest_complete_month = min(
                    _month_start(latest_received_date),
                    latest_snapshot_complete_month,
                )
                recent_window_start = _shift_month(latest_complete_month, -(_RECENT_MONTHS - 1))
                baseline_window_start = _shift_month(recent_window_start, -_BASELINE_MONTHS)
                monthly_count = connection.execute(
                    f"SELECT COUNT(*) FROM read_parquet({_sql_literal(staging / _ARTIFACTS[1])})"
                ).fetchone()
                monthly_rows = connection.execute(
                    "SELECT product_code, month, report_count "
                    f"FROM read_parquet({_sql_literal(staging / _ARTIFACTS[1])}) "
                    "WHERE month BETWEEN ? AND ? ORDER BY product_code, month",
                    [baseline_window_start, latest_complete_month],
                ).fetchall()
            finally:
                connection.close()

            if _month_start(earliest_received_date) > baseline_window_start:
                raise ValueError("research corpus does not cover the full 12-month volume baseline")
            expected_window_months = {
                _shift_month(baseline_window_start, offset)
                for offset in range(_BASELINE_MONTHS + _RECENT_MONTHS)
            }
            observed_window_months = {month for _, month, _ in monthly_rows}
            missing_window_months = sorted(expected_window_months - observed_window_months)
            if missing_window_months:
                missing_month_text = ", ".join(month.isoformat() for month in missing_window_months)
                raise ValueError(
                    "research corpus is missing monthly reports in the signal window: "
                    + missing_month_text
                )

            volume_rows = _volume_change_rows(monthly_rows, latest_complete_month)
            volume_table = pa.Table.from_pylist(volume_rows, schema=_VOLUME_CHANGE_SCHEMA)
            pq.write_table(volume_table, staging / _ARTIFACTS[2], compression="zstd")

        hashes = _artifact_hashes(staging)
        manifest = SignalFeatureManifest(
            feature_version=SIGNAL_FEATURE_VERSION,
            research_corpus_id=research_corpus_id,
            source_snapshot_ids=source_ids,
            research_corpus_manifest_sha256=_sha256_file(corpus_manifest_path),
            source_snapshot_manifest_sha256=source_manifest_hashes,
            output_directory=str(output_directory),
            cohort_dimension="FDA device report product code",
            report_cohort_rows=int(counts[0] or 0),
            undated_report_cohort_rows=int(counts[1] or 0),
            monthly_cohort_rows=int(monthly_count[0] or 0) if monthly_count is not None else 0,
            volume_cohort_rows=len(volume_rows),
            earliest_received_date=earliest_received_date,
            latest_received_date=latest_received_date,
            latest_source_snapshot_finished_at=latest_source_snapshot_finished_at,
            latest_complete_month=latest_complete_month,
            recent_window_start=recent_window_start,
            baseline_window_start=baseline_window_start,
            recent_months=_RECENT_MONTHS,
            baseline_months=_BASELINE_MONTHS,
            minimum_additional_reports=_MINIMUM_ADDITIONAL_REPORTS,
            robust_z_threshold=_ROBUST_Z_THRESHOLD,
            robust_z_scale_factor=_ROBUST_Z_SCALE_FACTOR,
            mad_zero_scale_floor=_MAD_ZERO_SCALE_FLOOR,
            reported_severity_mapping=_SEVERITY_MAPPING,
            unknown_event_type_policy=(
                "Keep the raw event type; classify blank values as missing and other "
                "unmapped values as unrecognized."
            ),
            volume_change_formula=(
                "robust_z=(recent_3_month_average-baseline_12_month_median)/"
                "max(1.4826*baseline_MAD,1.0); flag when robust_z>=3 and "
                "recent_3_month_total-3*baseline_median>=5."
            ),
            artifact_sha256=hashes,
            completed_at=datetime.now(UTC),
        )
        write_json_model(staging / "manifest.json", manifest)
        os.replace(staging, output_directory)
        fsync_directory(output_directory.parent)
        return manifest
