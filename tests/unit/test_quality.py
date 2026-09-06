from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4
from zipfile import ZIP_DEFLATED, ZipFile

import pyarrow as pa
import pyarrow.parquet as pq

import maude.quality.checks as checks
from maude.domain.enums import QualityLevel, RunStatus, TableKind
from maude.domain.models import (
    ParseStats,
    QualityResult,
    SnapshotResult,
    SourceIdentity,
    TableResult,
)
from maude.quality.checks import (
    business_key_uniqueness,
    has_blocking_failure,
    orphan_fraction,
    reject_fraction,
    required_table_set,
    row_conservation,
    row_count_drift,
    truncation_comparison,
)
from maude.quality.report import render_quality_markdown


def test_row_conservation_blocks_unaccounted_rows() -> None:
    result = row_conservation(rows_seen=100, accepted=98, rejected=1)
    assert result.level is QualityLevel.BLOCKING
    assert result.passed is False


def test_row_count_drop_of_more_than_ten_percent_blocks() -> None:
    result = row_count_drift(current=89, previous=100)
    assert result.passed is False
    assert has_blocking_failure((result,)) is True


def test_row_count_drop_at_ten_percent_is_warning_not_failure() -> None:
    result = row_count_drift(current=90, previous=100)
    assert result.passed is True


def test_reject_fraction_has_exact_boundary_semantics() -> None:
    assert reject_fraction(rows_seen=200, rejected=1).passed is False
    warning = reject_fraction(rows_seen=201, rejected=1)
    assert warning.level is QualityLevel.WARNING
    assert warning.passed is False
    blocking = reject_fraction(rows_seen=100, rejected=1)
    assert blocking.level is QualityLevel.BLOCKING
    assert blocking.passed is False


def test_business_key_uniqueness_blocks_duplicates() -> None:
    result = business_key_uniqueness([("1",), ("1",), ("2",)])
    assert result.level is QualityLevel.BLOCKING
    assert result.passed is False
    assert result.metrics["numerator"] == 1


def test_required_table_set_requires_all_four_tables() -> None:
    result = required_table_set((TableKind.MASTER, TableKind.DEVICE, TableKind.PATIENT))
    assert result.level is QualityLevel.BLOCKING
    assert result.passed is False
    assert result.metrics["denominator"] == 4


def test_orphan_fraction_has_exact_boundary_semantics() -> None:
    assert orphan_fraction(["1"] * 100, {"1"}).passed is True
    warning = orphan_fraction(["1"] * 100 + ["missing"], {"1"})
    assert warning.level is QualityLevel.WARNING
    assert warning.passed is False
    blocking = orphan_fraction(["1"] * 99 + ["missing", "missing"], {"1"})
    assert blocking.level is QualityLevel.BLOCKING
    assert blocking.passed is False


def test_truncation_comparison_streams_archive_and_converted_file(tmp_path: Path) -> None:
    archive = tmp_path / "patient.zip"
    with ZipFile(archive, "w", ZIP_DEFLATED) as handle:
        handle.writestr("patient.txt", "row\n" * 100)
    converted = tmp_path / "patient_UTF8.txt"
    converted.write_text("row\n" * 2, encoding="utf-8")

    result = truncation_comparison(archive, converted, member="patient.txt")
    assert result.passed is False
    assert result.level is QualityLevel.BLOCKING
    assert result.metrics["observed"] == 0.02


def _table(kind: TableKind, rows: int = 2) -> TableResult:
    return TableResult(
        table=kind,
        source=SourceIdentity(
            path=f"{kind.value}.zip",
            filename=f"{kind.value}.txt",
            sha256="a" * 64,
            byte_size=10,
            archive_member=f"{kind.value}.txt",
            encoding="utf-8",
        ),
        bronze_path=f"bronze/{kind.value}.parquet",
        silver_path=f"silver/{kind.value}.parquet",
        reject_path=f"rejects/{kind.value}.jsonl",
        stats=ParseStats(rows_seen=rows, rows_accepted=rows, rows_rejected=0, columns=()),
        quality=(),
    )


def test_quality_markdown_is_deterministic_and_contains_promotion_details() -> None:
    snapshot = SnapshotResult(
        run_id=uuid4(),
        snapshot_id="snapshot-1",
        started_at=datetime(2026, 1, 1, tzinfo=UTC),
        finished_at=datetime(2026, 1, 1, 0, 1, tzinfo=UTC),
        status=RunStatus.PROMOTED,
        tables=(_table(TableKind.PATIENT), _table(TableKind.MASTER)),
        quality=(
            QualityResult(
                check="warning-check",
                level=QualityLevel.WARNING,
                passed=True,
                message="warning",
                metrics={"observed": 0.1},
            ),
            QualityResult(
                check="blocking-check",
                level=QualityLevel.BLOCKING,
                passed=True,
                message="ok",
            ),
        ),
        promoted_path="/data/silver/snapshot-1",
        parser_version="1.0.0",
    )
    rendered = render_quality_markdown(snapshot)
    assert "snapshot-1" in rendered
    assert str(snapshot.run_id) in rendered
    assert "PROMOTED" in rendered
    assert "patient.txt" in rendered
    assert "blocking-check" in rendered
    assert "Promotion outcome" in rendered
    assert rendered.index("blocking-check") < rendered.index("warning-check")


def test_quality_markdown_preserves_identical_table_results_and_table_order() -> None:
    table_quality = QualityResult(
        check="same-check",
        level=QualityLevel.WARNING,
        passed=False,
        message="same warning",
        metrics={"numerator": 1, "denominator": 100, "threshold": 0.01, "observed": 0.01},
    )
    master = _table(TableKind.MASTER)
    patient = _table(TableKind.PATIENT)
    master = master.model_copy(update={"quality": (table_quality,)})
    patient = patient.model_copy(update={"quality": (table_quality,)})
    base = dict(
        run_id=uuid4(),
        snapshot_id="snapshot-context",
        started_at=datetime(2026, 1, 1, tzinfo=UTC),
        finished_at=None,
        status=RunStatus.VALIDATED,
        quality=(),
        promoted_path=None,
        parser_version="1.0.0",
    )
    first = SnapshotResult(tables=(master, patient), **base)
    second = SnapshotResult(tables=(patient, master), **base)
    first_report = render_quality_markdown(first)
    assert first_report == render_quality_markdown(second)
    assert first_report.count("same-check") == 2
    assert "table:master source:master.txt" in first_report
    assert "table:patient source:patient.txt" in first_report


def test_parquet_rows_closes_reader_when_generator_is_closed(tmp_path: Path, monkeypatch) -> None:
    path = tmp_path / "keys.parquet"
    pq.write_table(pa.table({"key": ["one", "two"]}), path)
    original = pq.ParquetFile
    readers = []

    class SpyReader:
        def __init__(self, source: Path) -> None:
            self.inner = original(source)
            self.closed = False
            readers.append(self)

        def __enter__(self):
            return self

        def __exit__(self, *args: object) -> None:
            self.closed = True
            self.inner.close()

        def iter_batches(self, **kwargs: object):
            return self.inner.iter_batches(**kwargs)

    monkeypatch.setattr(checks.pq, "ParquetFile", SpyReader)
    rows = checks._parquet_rows(path, ("key",))
    next(rows)
    rows.close()
    assert readers[0].closed is True
