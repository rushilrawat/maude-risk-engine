from __future__ import annotations

import multiprocessing
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4
from zipfile import ZIP_DEFLATED, ZipFile

import pytest

import maude.ingestion.pipeline as pipeline
from maude.config import Settings
from maude.domain.enums import QualityLevel, RunStatus, TableKind
from maude.domain.models import PromotionIntent, SnapshotResult
from maude.ingestion.manifests import load_manifest, write_manifest
from maude.ingestion.pipeline import SnapshotConflict, ingest_snapshot
from maude.storage.layout import SnapshotLayout


def _write_zip(path: Path, member: str, content: str) -> Path:
    with ZipFile(path, "w", ZIP_DEFLATED) as archive:
        archive.writestr(member, content)
    return path


def _sources(
    root: Path,
    *,
    narrative_header: bool = True,
    second_master_event: str = "M",
    first_master_date: str = "01/02/2025",
) -> tuple[Path, ...]:
    root.mkdir(parents=True, exist_ok=True)
    master = _write_zip(
        root / "mdrfoi.zip",
        "mdrfoi.txt",
        "MDR_REPORT_KEY|DATE_RECEIVED|EVENT_TYPE\n"
        f"1|{first_master_date}|D\n"
        f"2|01/03/2025|{second_master_event}\n",
    )
    device = _write_zip(
        root / "device.zip",
        "device.txt",
        "MDR_REPORT_KEY|DEVICE_SEQUENCE_NO|DEVICE_REPORT_PRODUCT_CODE\n1|1|ABC\n",
    )
    patient = _write_zip(
        root / "patient.zip",
        "patient.txt",
        "MDR_REPORT_KEY|PATIENT_SEQUENCE_NUMBER\n2|1\n",
    )
    narrative_columns = "MDR_REPORT_KEY|MDR_TEXT_KEY|TEXT_TYPE_CODE|DATE_REPORT"
    if narrative_header:
        narrative_columns += "|FOI_TEXT"
    narrative_row = "1|10|N|01/04/2025"
    if narrative_header:
        narrative_row += "|Pump stopped unexpectedly"
    narrative = _write_zip(
        root / "foitext.zip", "foitext.txt", f"{narrative_columns}\n{narrative_row}\n"
    )
    return (narrative, patient, device, master)


def _settings(tmp_path: Path) -> Settings:
    return Settings(data_root=tmp_path / "data", batch_rows=1, parser_version="test-parser")


def _ingest_worker(
    data_root: str,
    sources: tuple[str, ...],
    snapshot_id: str,
    ready: object,
    results: object,
    claim_entered: object | None = None,
    claim_release: object | None = None,
) -> None:
    ready.wait()  # type: ignore[union-attr]
    if claim_entered is not None and claim_release is not None:

        def hold_first_claim() -> None:
            if not claim_entered.is_set():  # type: ignore[union-attr]
                claim_entered.set()  # type: ignore[union-attr]
                assert claim_release.wait(timeout=10)  # type: ignore[union-attr]

        pipeline._CLAIM_HOOK = hold_first_claim
    try:
        result = ingest_snapshot(
            snapshot_id,
            tuple(Path(path) for path in sources),
            Settings(data_root=Path(data_root), batch_rows=1, parser_version="test-parser"),
        )
        results.put(("result", result.status.value))  # type: ignore[union-attr]
    except Exception as error:
        results.put(("error", type(error).__name__))  # type: ignore[union-attr]
    finally:
        pipeline._CLAIM_HOOK = None


def _run_contended_workers(
    settings: Settings, source_sets: tuple[tuple[Path, ...], tuple[Path, ...]]
) -> list[tuple[str, str]]:
    context = multiprocessing.get_context("spawn")
    entered = context.Event()
    release = context.Event()
    ready = context.Event()
    ready.set()
    results = context.Queue()

    workers = [
        context.Process(
            target=_ingest_worker,
            args=(
                str(settings.data_root),
                tuple(map(str, inputs)),
                "fixture-2025-06",
                ready,
                results,
                entered,
                release,
            ),
        )
        for inputs in source_sets
    ]
    try:
        # The worker gate is already set, while the claim hook holds the first
        # process inside the actual exclusive critical section.
        workers[0].start()
        assert entered.wait(timeout=10)
        workers[1].start()
        release.set()
        for worker in workers:
            worker.join(timeout=20)
            if worker.is_alive():
                worker.terminate()
                worker.join(timeout=5)
                pytest.fail("contended worker did not finish")
            assert worker.exitcode == 0
        return [results.get(timeout=5), results.get(timeout=5)]
    finally:
        release.set()
        for worker in workers:
            if worker.is_alive():
                worker.terminate()
                worker.join(timeout=5)
        results.close()
        results.join_thread()


def test_ingest_promotes_validated_snapshot_and_persists_manifest(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    result = ingest_snapshot("fixture-2025-06", _sources(tmp_path), settings)
    layout = SnapshotLayout(settings.data_root, "fixture-2025-06")

    assert result.status is RunStatus.PROMOTED
    assert result.promoted_path is not None
    assert Path(result.promoted_path).exists()
    assert not layout.staging.exists()
    assert load_manifest(layout.manifest).model_dump() == result.model_dump()
    assert load_manifest(layout.current_manifest).snapshot_id == "fixture-2025-06"
    assert [table.table for table in result.tables] == list(TableKind)
    assert all(table.quality[0].level is QualityLevel.WARNING for table in result.tables)
    assert all(
        Path(table.silver_path).parent == Path(result.promoted_path) for table in result.tables
    )


def test_failed_snapshot_keeps_staging_evidence_and_current_pointer(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    first = ingest_snapshot("fixture-2025-06", _sources(tmp_path / "first"), settings)
    first_layout = SnapshotLayout(settings.data_root, first.snapshot_id)

    failed = ingest_snapshot(
        "fixture-2025-07", _sources(tmp_path / "failed", narrative_header=False), settings
    )
    failed_layout = SnapshotLayout(settings.data_root, failed.snapshot_id)

    assert failed.status is RunStatus.FAILED
    assert failed_layout.staging.exists()
    assert any(failed_layout.rejects.glob("*.jsonl"))
    failed_manifest = load_manifest(failed_layout.manifest)
    assert failed_manifest.status is RunStatus.FAILED
    assert [table.table for table in failed_manifest.tables] == [
        TableKind.MASTER,
        TableKind.DEVICE,
        TableKind.PATIENT,
    ]
    assert failed_manifest.failure is not None
    assert failed_manifest.failure.table is TableKind.NARRATIVE
    assert failed_manifest.failure.source_path.endswith("foitext.zip")
    assert failed_manifest.failure.error_type == "SchemaMismatch"
    assert load_manifest(first_layout.current_manifest).snapshot_id == first.snapshot_id
    assert first_layout.promoted.exists()


def test_unparseable_normalization_dates_are_nonblocking_table_warnings(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    result = ingest_snapshot(
        "fixture-2025-06",
        _sources(tmp_path, first_master_date="13/40/2025"),
        settings,
    )
    master = next(table for table in result.tables if table.table is TableKind.MASTER)

    assert result.status is RunStatus.PROMOTED
    assert master.quality[0].check == "date_parse_failure_count"
    assert master.quality[0].level is QualityLevel.WARNING
    assert master.quality[0].passed is False
    assert master.quality[0].metrics["numerator"] == 1


def test_identical_checksum_rerun_returns_promoted_manifest_without_rewriting_parquet(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    sources = _sources(tmp_path)
    first = ingest_snapshot("fixture-2025-06", sources, settings)
    promoted = Path(first.promoted_path or "")
    before = {path.name: path.stat().st_mtime_ns for path in promoted.glob("*.parquet")}

    rerun = ingest_snapshot("fixture-2025-06", sources, settings)

    assert rerun.model_dump() == first.model_dump()
    assert {path.name: path.stat().st_mtime_ns for path in promoted.glob("*.parquet")} == before


def test_reusing_snapshot_id_with_different_source_checksum_raises_conflict(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    sources = _sources(tmp_path)
    ingest_snapshot("fixture-2025-06", sources, settings)
    changed_sources = _sources(tmp_path / "changed", second_master_event="E")

    with pytest.raises(SnapshotConflict, match="different source checksums"):
        ingest_snapshot("fixture-2025-06", changed_sources, settings)


def test_cleanup_failure_does_not_rollback_an_already_published_pointer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = _settings(tmp_path)

    def fail_cleanup(path: Path) -> None:
        raise OSError("simulated cleanup failure")

    monkeypatch.setattr(pipeline, "rmtree", fail_cleanup)
    result = ingest_snapshot("fixture-2025-06", _sources(tmp_path), settings)
    layout = SnapshotLayout(settings.data_root, result.snapshot_id)

    assert result.status is RunStatus.PROMOTED
    assert Path(result.promoted_path or "").exists()
    assert load_manifest(layout.current_manifest).status is RunStatus.PROMOTED


@pytest.mark.parametrize(
    "snapshot_id", (".", "..", "../escape", "contains space", "slash/name", "")
)
def test_snapshot_id_must_stay_inside_snapshot_layout(tmp_path: Path, snapshot_id: str) -> None:
    with pytest.raises(ValueError, match="snapshot_id"):
        ingest_snapshot(snapshot_id, (), _settings(tmp_path))


def test_rejects_symlinked_snapshot_staging_path_before_writing(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    layout = SnapshotLayout(settings.data_root, "fixture-2025-06")
    layout.staging.parent.mkdir(parents=True)
    outside = tmp_path / "outside"
    outside.mkdir()
    layout.staging.symlink_to(outside, target_is_directory=True)

    with pytest.raises(ValueError, match="symlink"):
        ingest_snapshot(layout.snapshot_id, _sources(tmp_path / "sources"), settings)

    assert list(outside.iterdir()) == []


def test_rejects_symlinked_manifests_root_before_writing(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    settings.data_root.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    settings.data_root.joinpath("manifests").symlink_to(outside, target_is_directory=True)

    with pytest.raises(ValueError, match="symlink"):
        ingest_snapshot("fixture-2025-06", _sources(tmp_path / "sources"), settings)

    assert list(outside.iterdir()) == []


@pytest.mark.parametrize("metadata", ("", '{"pid":'))
def test_ownerless_or_truncated_advisory_claim_is_recovered_safely(
    tmp_path: Path, metadata: str
) -> None:
    settings = _settings(tmp_path)
    stale = settings.data_root / "locks" / "fixture-2025-06.lock"
    stale.parent.mkdir(parents=True)
    stale.write_text(metadata, encoding="utf-8")

    result = ingest_snapshot("fixture-2025-06", _sources(tmp_path / "sources"), settings)

    assert result.status is RunStatus.PROMOTED
    assert stale.exists()


def test_advisory_claim_never_probes_or_deletes_pid_metadata_to_infer_ownership(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = _settings(tmp_path)
    claim = settings.data_root / "locks" / "fixture-2025-06.lock"
    claim.parent.mkdir(parents=True)
    claim.write_text('{"pid":1}', encoding="utf-8")

    def permission_denied(_pid: int, _signal: int) -> None:
        raise PermissionError("simulated foreign live owner")

    monkeypatch.setattr(pipeline.os, "kill", permission_denied)

    result = ingest_snapshot("fixture-2025-06", _sources(tmp_path / "sources"), settings)

    assert result.status is RunStatus.PROMOTED
    assert claim.exists()


def test_staging_setup_failure_persists_failed_manifest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = _settings(tmp_path)
    layout = SnapshotLayout(settings.data_root, "fixture-2025-06")
    original_mkdir = Path.mkdir

    def fail_staging(self: Path, *args: object, **kwargs: object) -> None:
        if self == layout.staging:
            raise OSError("simulated staging mkdir failure")
        original_mkdir(self, *args, **kwargs)

    monkeypatch.setattr(Path, "mkdir", fail_staging)
    result = ingest_snapshot(layout.snapshot_id, _sources(tmp_path / "sources"), settings)

    assert result.status is RunStatus.FAILED
    assert result.failure is not None
    assert result.failure.phase == "staging_setup"
    assert load_manifest(layout.manifest).status is RunStatus.FAILED


def test_retry_recovers_after_promoted_manifest_write_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = _settings(tmp_path)
    sources = _sources(tmp_path / "sources")
    layout = SnapshotLayout(settings.data_root, "fixture-2025-06")
    original_write = pipeline.write_manifest
    calls = 0

    def fail_once(path: Path, snapshot: SnapshotResult) -> None:
        nonlocal calls
        if path == layout.manifest and snapshot.status is RunStatus.PROMOTED and calls == 0:
            calls += 1
            raise OSError("simulated promoted manifest failure")
        original_write(path, snapshot)  # type: ignore[arg-type]

    monkeypatch.setattr(pipeline, "write_manifest", fail_once)
    first = ingest_snapshot(layout.snapshot_id, sources, settings)
    assert first.status is RunStatus.FAILED
    assert layout.staging.exists()

    monkeypatch.setattr(pipeline, "write_manifest", original_write)
    recovered = ingest_snapshot(layout.snapshot_id, sources, settings)
    assert recovered.status is RunStatus.PROMOTED
    assert load_manifest(layout.current_manifest).snapshot_id == layout.snapshot_id


def test_retry_recovers_when_pointer_publication_fails_without_overwriting_current(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = _settings(tmp_path)
    prior = ingest_snapshot("fixture-2025-05", _sources(tmp_path / "prior"), settings)
    layout = SnapshotLayout(settings.data_root, "fixture-2025-06")
    sources = _sources(tmp_path / "sources")
    original_write = pipeline.write_manifest
    failed = False

    def fail_pointer(path: Path, snapshot: object) -> None:
        nonlocal failed
        if path == layout.current_manifest and not failed:
            failed = True
            raise OSError("simulated pointer failure")
        original_write(path, snapshot)  # type: ignore[arg-type]

    monkeypatch.setattr(pipeline, "write_manifest", fail_pointer)
    first = ingest_snapshot(layout.snapshot_id, sources, settings)
    assert first.status is RunStatus.PROMOTED
    assert load_manifest(layout.current_manifest).snapshot_id == prior.snapshot_id

    monkeypatch.setattr(pipeline, "write_manifest", original_write)
    recovered = ingest_snapshot(layout.snapshot_id, sources, settings)
    assert recovered.status is RunStatus.PROMOTED
    assert load_manifest(layout.current_manifest).snapshot_id == layout.snapshot_id


def test_directory_rename_failure_persists_failed_snapshot_and_keeps_current(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = _settings(tmp_path)
    prior = ingest_snapshot("fixture-2025-05", _sources(tmp_path / "prior"), settings)
    layout = SnapshotLayout(settings.data_root, "fixture-2025-06")
    original_replace = Path.replace

    def fail_promotion(source: Path, target: Path) -> Path:
        if source == layout.silver and target == layout.promoted:
            raise OSError("simulated promotion rename failure")
        return original_replace(source, target)

    monkeypatch.setattr(Path, "replace", fail_promotion)
    result = ingest_snapshot(layout.snapshot_id, _sources(tmp_path / "sources"), settings)

    assert result.status is RunStatus.FAILED
    assert result.failure is not None
    assert result.failure.phase == "promotion"
    assert load_manifest(layout.current_manifest).snapshot_id == prior.snapshot_id
    assert layout.staging.joinpath("silver").exists()


def test_rollback_failure_leaves_recoverable_validated_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = _settings(tmp_path)
    layout = SnapshotLayout(settings.data_root, "fixture-2025-06")
    original_replace = Path.replace
    original_write = pipeline.write_manifest
    sources = _sources(tmp_path / "sources")

    def fail_promoted_manifest(path: Path, snapshot: SnapshotResult) -> None:
        if path == layout.manifest and snapshot.status is RunStatus.PROMOTED:
            raise OSError("simulated promoted manifest failure")
        original_write(path, snapshot)  # type: ignore[arg-type]

    def fail_rollback(source: Path, target: Path) -> Path:
        if source == layout.promoted and target == layout.silver:
            raise OSError("simulated rollback rename failure")
        return original_replace(source, target)

    monkeypatch.setattr(pipeline, "write_manifest", fail_promoted_manifest)
    monkeypatch.setattr(Path, "replace", fail_rollback)
    interrupted = ingest_snapshot(layout.snapshot_id, sources, settings)

    assert interrupted.status is RunStatus.VALIDATED
    assert interrupted.promotion is not None
    assert interrupted.promotion.phase == "silver_renamed"
    assert layout.promoted.exists()

    monkeypatch.setattr(pipeline, "write_manifest", original_write)
    monkeypatch.setattr(Path, "replace", original_replace)
    recovered = ingest_snapshot(layout.snapshot_id, sources, settings)
    assert recovered.status is RunStatus.PROMOTED


def test_promotion_directory_rename_fsyncs_parent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = _settings(tmp_path)
    layout = SnapshotLayout(settings.data_root, "fixture-2025-06")
    synced: list[Path] = []
    original_sync = pipeline.fsync_directory
    monkeypatch.setattr(pipeline, "fsync_directory", synced.append)

    result = ingest_snapshot(layout.snapshot_id, _sources(tmp_path / "sources"), settings)

    monkeypatch.setattr(pipeline, "fsync_directory", original_sync)
    assert result.status is RunStatus.PROMOTED
    assert layout.promoted.parent in synced


def test_same_snapshot_concurrent_runs_are_serialized_and_conflicts_are_deterministic(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    sources = _sources(tmp_path / "sources")
    changed = _sources(tmp_path / "changed", second_master_event="E")
    observed = _run_contended_workers(settings, (sources, changed))
    assert sorted(observed) == [("error", "SnapshotConflict"), ("result", "promoted")]


def test_identical_same_snapshot_concurrent_runs_share_one_promoted_result(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    sources = _sources(tmp_path / "sources")
    observed = _run_contended_workers(settings, (sources, sources))
    assert sorted(observed) == [("result", "promoted"), ("result", "promoted")]


@pytest.mark.parametrize("target_kind", ("file", "directory"))
def test_new_run_rejects_stale_promoted_target_without_advancing_current(
    tmp_path: Path, target_kind: str
) -> None:
    settings = _settings(tmp_path)
    prior = ingest_snapshot("fixture-2025-05", _sources(tmp_path / "prior"), settings)
    layout = SnapshotLayout(settings.data_root, "fixture-2025-06")
    layout.promoted.parent.mkdir(parents=True, exist_ok=True)
    if target_kind == "file":
        layout.promoted.write_text("stale", encoding="utf-8")
    else:
        layout.promoted.mkdir()
        layout.promoted.joinpath("unrelated.parquet").write_text("stale", encoding="utf-8")

    result = ingest_snapshot(layout.snapshot_id, _sources(tmp_path / "sources"), settings)

    assert result.status is RunStatus.FAILED
    assert result.failure is not None
    assert result.failure.phase == "promotion"
    assert load_manifest(layout.current_manifest).snapshot_id == prior.snapshot_id
    assert layout.silver.exists()


@pytest.mark.parametrize("leaf", ("bronze", "rejects", "silver"))
def test_rejects_symlinked_staging_leaf_before_any_external_write(
    tmp_path: Path, leaf: str
) -> None:
    settings = _settings(tmp_path)
    layout = SnapshotLayout(settings.data_root, "fixture-2025-06")
    layout.staging.mkdir(parents=True)
    outside = tmp_path / "outside"
    outside.mkdir()
    layout.staging.joinpath(leaf).symlink_to(outside, target_is_directory=True)

    result = ingest_snapshot(layout.snapshot_id, _sources(tmp_path / "sources"), settings)

    assert result.status is RunStatus.FAILED
    assert result.failure is not None
    assert result.failure.phase == "staging_setup"
    assert list(outside.iterdir()) == []


def test_interrupted_running_with_same_inputs_becomes_durable_failure(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    sources = _sources(tmp_path / "sources")
    layout = SnapshotLayout(settings.data_root, "fixture-2025-06")
    candidates = pipeline._sources_by_table(sources)
    running = SnapshotResult(
        run_id=uuid4(),
        snapshot_id=layout.snapshot_id,
        started_at=datetime.now(UTC),
        finished_at=None,
        status=RunStatus.RUNNING,
        tables=[],
        quality=[],
        promoted_path=None,
        parser_version=settings.parser_version,
        inputs=[candidate.source for candidate in candidates.values()],
        promotion=PromotionIntent(phase="running"),
    )
    layout.manifest.parent.mkdir(parents=True)
    write_manifest(layout.manifest, running)

    recovered = ingest_snapshot(layout.snapshot_id, sources, settings)

    assert recovered.status is RunStatus.FAILED
    assert recovered.failure is not None
    assert recovered.failure.phase == "interrupted_running"


def test_interrupted_running_with_different_inputs_is_a_conflict(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    sources = _sources(tmp_path / "sources")
    layout = SnapshotLayout(settings.data_root, "fixture-2025-06")
    candidates = pipeline._sources_by_table(sources)
    running = SnapshotResult(
        run_id=uuid4(),
        snapshot_id=layout.snapshot_id,
        started_at=datetime.now(UTC),
        finished_at=None,
        status=RunStatus.RUNNING,
        tables=[],
        quality=[],
        promoted_path=None,
        parser_version=settings.parser_version,
        inputs=[candidate.source for candidate in candidates.values()],
        promotion=PromotionIntent(phase="running"),
    )
    layout.manifest.parent.mkdir(parents=True)
    write_manifest(layout.manifest, running)

    with pytest.raises(SnapshotConflict, match="different source checksums"):
        ingest_snapshot(
            layout.snapshot_id,
            _sources(tmp_path / "changed", second_master_event="E"),
            settings,
        )
