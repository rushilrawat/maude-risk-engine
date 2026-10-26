import hashlib
import json
import sqlite3
from contextlib import suppress
from datetime import UTC, date, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from maude.domain.models import ModelingCorpusManifest, ModelingDocument, ModelingNarrative
from maude.ingestion.manifests import write_json_model


def _document(report_id: str, received: date | None, text: str) -> ModelingDocument:
    narrative = ModelingNarrative(
        report_id=report_id,
        dataset_snapshot_id="fixture-2026",
        evidence_id=f"narrative:{report_id}",
        text_type_code="N",
        source_filename="fixture.txt",
        source_line_number=1,
        source_text_sha256=hashlib.sha256(text.encode()).hexdigest(),
        modeling_text=text,
        cleaning_version="nfc-whitespace-v1",
    )
    return ModelingDocument(
        report_id=report_id,
        dataset_snapshot_id="fixture-2026",
        reported_event_type="D",
        date_received=received,
        product_codes=("ABC",),
        brand_names=(),
        manufacturers=(),
        narratives=(narrative,),
        modeling_text=text,
        cleaning_version="nfc-whitespace-v1",
    )


def _write_corpus(data_root: Path, documents: tuple[ModelingDocument, ...] | None = None) -> Path:
    if documents is None:
        near_duplicate = " ".join(f"token{number}" for number in range(40))
        altered_near_duplicate = near_duplicate.replace("token20", "changed20")
        documents = (
            _document("train-exact", date(2024, 12, 31), "Pump stalled and caused injury"),
            _document("validation-exact", date(2025, 1, 1), "Pump stalled and caused injury"),
            _document("train-near", date(2024, 6, 1), near_duplicate),
            _document("validation-near", date(2025, 6, 1), altered_near_duplicate),
            _document("repeated-key", date(2024, 4, 1), "Earlier conflicting report record"),
            _document("repeated-key", date(2025, 4, 1), "Later conflicting report record"),
            _document("undated", None, "Narrative without a received date"),
        )
    corpus_dir = data_root / "corpora" / "fixture-2026" / "nfc-whitespace-v1"
    corpus_dir.mkdir(parents=True)
    output_path = corpus_dir / "documents.jsonl"
    payload = "".join(f"{document.model_dump_json()}\n" for document in documents).encode()
    output_path.write_bytes(payload)
    manifest_path = corpus_dir / "manifest.json"
    write_json_model(
        manifest_path,
        ModelingCorpusManifest(
            corpus_format_version="1",
            cleaning_version="nfc-whitespace-v1",
            snapshot_id="fixture-2026",
            promoted_snapshot_path=str(data_root / "silver" / "fixture-2026"),
            output_path=str(output_path),
            manifest_path=str(manifest_path),
            output_sha256=hashlib.sha256(payload).hexdigest(),
            reports_seen=len(documents),
            documents_written=len(documents),
            narratives_written=len(documents),
            blank_narratives_omitted=0,
            completed_at=datetime.now(UTC),
        ),
    )
    return output_path


def _patch_disk_usage(monkeypatch: pytest.MonkeyPatch, free_bytes: list[int]) -> None:
    import shutil

    remaining = iter(free_bytes)
    last_free = free_bytes[-1]

    def fake_disk_usage(path: str | Path) -> SimpleNamespace:
        nonlocal last_free
        with suppress(StopIteration):
            last_free = next(remaining)
        return SimpleNamespace(total=100 * 1024**3, used=100 * 1024**3 - last_free, free=last_free)

    monkeypatch.setattr(shutil, "disk_usage", fake_disk_usage)


def test_audit_batches_narrative_index_inserts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import maude.features.duplicates as duplicates

    corpus_path = tmp_path / "documents.jsonl"
    documents = (
        _document("train-1", date(2024, 1, 1), "Pump stopped during use"),
        _document("train-2", date(2024, 2, 1), "Pump stalled during use"),
        _document("validation-1", date(2025, 1, 1), "Pump stopped during use"),
    )
    corpus_path.write_text("".join(f"{item.model_dump_json()}\n" for item in documents))
    monkeypatch.setattr(duplicates, "_INDEX_BATCH_SIZE", 2, raising=False)

    class RecordingConnection:
        def __init__(self, connection: sqlite3.Connection) -> None:
            self.connection = connection
            self.narrative_batch_sizes: list[int] = []

        def __getattr__(self, name: str):
            return getattr(self.connection, name)

        def executemany(self, sql: str, rows) -> sqlite3.Cursor:
            if sql.lstrip().startswith("INSERT INTO narratives"):
                self.narrative_batch_sizes.append(len(rows))
            return self.connection.executemany(sql, rows)

    connection = RecordingConnection(sqlite3.connect(":memory:"))
    try:
        duplicates._index_schema(connection)
        duplicates._index_corpus(connection, corpus_path, "fixture-2026", "nfc-whitespace-v1")
    finally:
        connection.close()

    assert connection.narrative_batch_sizes == [2, 1]


def test_audit_flags_exact_and_near_duplicates_across_temporal_splits(
    tmp_path: Path,
) -> None:
    from maude.features.duplicates import build_duplicate_audit

    _write_corpus(tmp_path)

    manifest = build_duplicate_audit(tmp_path, "fixture-2026")

    assert manifest.narratives_scanned == 7
    assert manifest.split_version == "received-calendar-year-2024-2025-2026plus-v1"
    assert manifest.missing_date_narratives == 1
    assert manifest.exact_duplicate_groups == 1
    assert manifest.exact_cross_split_groups == 1
    assert manifest.report_key_cross_split_groups == 1
    assert manifest.near_cross_split_pairs == 1
    assert manifest.algorithm_version == "sqlite-ppjoin-jaccard-v1"
    assert manifest.near_candidate_overflow_documents == 0
    assert manifest.near_candidate_search_is_exhaustive is True
    exact_groups = [
        json.loads(line) for line in Path(manifest.exact_groups_path).read_text().splitlines()
    ]
    assert exact_groups[0]["cross_temporal_split"] is True
    exact_members = [
        json.loads(line) for line in Path(manifest.exact_members_path).read_text().splitlines()
    ]
    assert [member["report_id"] for member in exact_members] == [
        "train-exact",
        "validation-exact",
    ]
    report_key_overlaps = [
        json.loads(line)
        for line in Path(manifest.report_key_overlaps_path).read_text().splitlines()
    ]
    assert report_key_overlaps == [
        {
            "narrative_count": 2,
            "report_id": "repeated-key",
            "split_counts": {"train": 1, "validation": 1},
        }
    ]
    near_pairs = [
        json.loads(line) for line in Path(manifest.near_pairs_path).read_text().splitlines()
    ]
    assert near_pairs == [
        {
            "left_evidence_id": "narrative:train-near",
            "left_report_id": "train-near",
            "left_split": "train",
            "right_evidence_id": "narrative:validation-near",
            "right_report_id": "validation-near",
            "right_split": "validation",
            "jaccard_similarity": pytest.approx(35 / 41),
        }
    ]


def test_v2_audit_has_no_fixed_near_candidate_cap(tmp_path: Path) -> None:
    from maude.features.duplicates import build_duplicate_audit

    common_text = " ".join(f"shared{number}" for number in range(50))
    documents = (
        *(
            _document(f"train-{index:03}", date(2024, 6, 1), f"{common_text} unique{index}")
            for index in range(251)
        ),
        _document("validation-target", date(2025, 6, 1), f"{common_text} target"),
    )
    _write_corpus(tmp_path, documents)

    manifest = build_duplicate_audit(tmp_path, "fixture-2026")

    near_pairs = Path(manifest.near_pairs_path).read_text(encoding="utf-8").splitlines()
    assert len(near_pairs) == 251
    assert manifest.near_cross_split_pairs == 251
    assert manifest.near_candidate_overflow_documents == 0
    assert manifest.near_candidate_search_is_exhaustive is True


def test_audit_finds_near_duplicates_when_corpus_order_reverses_temporal_order(
    tmp_path: Path,
) -> None:
    from maude.features.duplicates import build_duplicate_audit

    forward_root = tmp_path / "forward"
    reverse_root = tmp_path / "reverse"
    _write_corpus(forward_root)
    corpus_path = _write_corpus(reverse_root)
    documents = [
        ModelingDocument.model_validate_json(line)
        for line in corpus_path.read_text(encoding="utf-8").splitlines()
    ]
    documents.sort(key=lambda item: item.date_received or date.min, reverse=True)
    payload = "".join(f"{item.model_dump_json()}\n" for item in documents).encode()
    corpus_path.write_bytes(payload)
    manifest_path = corpus_path.parent / "manifest.json"
    manifest = ModelingCorpusManifest.model_validate_json(manifest_path.read_text(encoding="utf-8"))
    write_json_model(
        manifest_path,
        manifest.model_copy(update={"output_sha256": hashlib.sha256(payload).hexdigest()}),
    )

    forward = build_duplicate_audit(forward_root, "fixture-2026")
    result = build_duplicate_audit(reverse_root, "fixture-2026")

    assert result.near_cross_split_pairs == 1
    assert Path(forward.near_pairs_path).read_text(encoding="utf-8") == Path(
        result.near_pairs_path
    ).read_text(encoding="utf-8")
    near_pairs = [
        json.loads(line) for line in Path(result.near_pairs_path).read_text().splitlines()
    ]
    assert near_pairs[0]["left_report_id"] == "train-near"
    assert near_pairs[0]["left_split"] == "train"
    assert near_pairs[0]["right_report_id"] == "validation-near"
    assert near_pairs[0]["right_split"] == "validation"


def test_audit_artifact_order_is_stable_for_repeated_identifiers(tmp_path: Path) -> None:
    from maude.features.duplicates import build_duplicate_audit

    base = " ".join(f"stable{number}" for number in range(40))
    altered = base.replace("stable20", "changed20")

    def with_evidence_id(
        report_id: str, received: date, text: str, evidence_id: str
    ) -> ModelingDocument:
        document = _document(report_id, received, text)
        narrative = document.narratives[0].model_copy(update={"evidence_id": evidence_id})
        return document.model_copy(update={"narratives": (narrative,)})

    documents = (
        with_evidence_id("near-left", date(2024, 12, 1), base, "same-left-evidence"),
        with_evidence_id("near-left", date(2025, 2, 1), base, "same-left-evidence"),
        with_evidence_id("near-right", date(2025, 3, 1), altered, "same-right-evidence"),
        with_evidence_id("near-right", date(2026, 3, 1), altered, "same-right-evidence"),
        with_evidence_id(
            "exact-repeat", date(2024, 1, 1), "same exact narrative words", "same-exact-evidence"
        ),
        with_evidence_id(
            "exact-repeat", date(2025, 1, 1), "same exact narrative words", "same-exact-evidence"
        ),
        with_evidence_id(
            "exact-anchor", date(2024, 2, 1), "same exact narrative words", "anchor-evidence"
        ),
    )

    def build(root: Path, ordered: tuple[ModelingDocument, ...]) -> dict[str, bytes]:
        corpus_path = _write_corpus(root, ordered)
        manifest_path = corpus_path.parent / "manifest.json"
        manifest = ModelingCorpusManifest.model_validate_json(
            manifest_path.read_text(encoding="utf-8")
        )
        payload = corpus_path.read_bytes()
        write_json_model(
            manifest_path,
            manifest.model_copy(update={"output_sha256": hashlib.sha256(payload).hexdigest()}),
        )
        audit = build_duplicate_audit(root, "fixture-2026")
        return {
            "exact_members": Path(audit.exact_members_path).read_bytes(),
            "near_pairs": Path(audit.near_pairs_path).read_bytes(),
        }

    assert build(tmp_path / "forward", documents) == build(
        tmp_path / "reverse", tuple(reversed(documents))
    )


def test_near_pair_output_query_uses_an_index_instead_of_a_temp_sort() -> None:
    import maude.features.duplicates as duplicates

    connection = sqlite3.connect(":memory:")
    try:
        duplicates._index_schema(connection)
        plan = connection.execute(
            """EXPLAIN QUERY PLAN
               SELECT left_report_id, left_evidence_id, left_split,
                      right_report_id, right_evidence_id, right_split, similarity
               FROM near_pairs
               ORDER BY left_report_id, left_evidence_id,
                        right_report_id, right_evidence_id,
                        left_split, right_split, similarity"""
        ).fetchall()
        exact_members_plan = connection.execute(
            """EXPLAIN QUERY PLAN
               SELECT n.exact_sha256, n.report_id, n.evidence_id,
                      n.received_date, n.split
               FROM narratives AS n
               WHERE n.exact_sha256 IN (
                   SELECT exact_sha256 FROM narratives GROUP BY exact_sha256
                   HAVING COUNT(DISTINCT report_id) > 1
               )
               ORDER BY n.exact_sha256, n.report_id, n.evidence_id,
                        n.received_date, n.split"""
        ).fetchall()
    finally:
        connection.close()

    assert not any("USE TEMP B-TREE FOR ORDER BY" in str(row[3]) for row in plan)
    assert not any("USE TEMP B-TREE FOR ORDER BY" in str(row[3]) for row in exact_members_plan)


def test_audit_is_immutable_and_rejects_modified_results(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import maude.features.duplicates as duplicates
    from maude.features.duplicates import build_duplicate_audit

    _write_corpus(tmp_path)
    first = build_duplicate_audit(tmp_path, "fixture-2026")
    assert first.audit_version == "narrative-duplicate-audit-v2"
    assert first.algorithm_version == "sqlite-ppjoin-jaccard-v1"

    def unexpected_rebuild(connection: sqlite3.Connection) -> int:
        pytest.fail("an immutable v2 audit must be reopened without rerunning the join")

    monkeypatch.setattr(duplicates, "_run_exact_similarity_join", unexpected_rebuild)
    second = build_duplicate_audit(tmp_path, "fixture-2026")

    assert second.model_dump() == first.model_dump()
    Path(first.near_pairs_path).write_text("tampered\n")

    with pytest.raises(ValueError, match="checksum"):
        build_duplicate_audit(tmp_path, "fixture-2026")


def test_default_duplicate_audit_publishes_v2(tmp_path: Path) -> None:
    from maude.features.duplicates import build_duplicate_audit

    _write_corpus(tmp_path)

    manifest = build_duplicate_audit(tmp_path, "fixture-2026")

    assert manifest.audit_version == "narrative-duplicate-audit-v2"
    assert Path(manifest.output_directory).name == "narrative-duplicate-audit-v2"


def test_v2_audit_is_exhaustive_and_leaves_v1_directory_untouched(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import maude.features.duplicates as duplicates
    from maude.features.duplicates import build_duplicate_audit

    _write_corpus(tmp_path)
    monkeypatch.setattr(duplicates, "DUPLICATE_AUDIT_VERSION", "narrative-duplicate-audit-v1")
    legacy = build_duplicate_audit(tmp_path, "fixture-2026")
    sentinel = Path(legacy.output_directory) / "preserve.txt"
    sentinel.write_text("immutable v1 artifact", encoding="utf-8")

    monkeypatch.setattr(duplicates, "DUPLICATE_AUDIT_VERSION", "narrative-duplicate-audit-v2")
    manifest = build_duplicate_audit(tmp_path, "fixture-2026")

    assert manifest.audit_version == "narrative-duplicate-audit-v2"
    assert manifest.algorithm_version == "sqlite-ppjoin-jaccard-v1"
    assert manifest.near_candidate_search_is_exhaustive is True
    assert manifest.near_candidate_overflow_documents == 0
    assert sentinel.read_text(encoding="utf-8") == "immutable v1 artifact"


def test_audit_stops_at_preflight_without_publishing_or_staging(
    tmp_path: Path, monkeypatch
) -> None:
    from maude.features.duplicates import build_duplicate_audit

    _write_corpus(tmp_path)
    minimum_free = 10 * 1024**3
    _patch_disk_usage(monkeypatch, [minimum_free - 1])

    with pytest.raises(RuntimeError, match="phase=preflight") as error:
        build_duplicate_audit(tmp_path, "fixture-2026")

    assert "last_completed_phase=preflight" in str(error.value)
    assert f"minimum_free_bytes={minimum_free - 1}" in str(error.value)
    output_parent = tmp_path / "audits" / "fixture-2026"
    assert not (output_parent / "narrative-duplicate-audit-v2").exists()
    assert list(output_parent.glob(".duplicate-audit-*")) == []


def test_audit_cleans_staging_when_disk_guard_stops_mid_run(tmp_path: Path, monkeypatch) -> None:
    import maude.features.duplicates as duplicates
    from maude.features.duplicates import build_duplicate_audit

    _write_corpus(tmp_path)
    minimum_free = 10 * 1024**3
    _patch_disk_usage(
        monkeypatch,
        [100 * 1024**3, 100 * 1024**3, 100 * 1024**3, minimum_free - 1],
    )
    monkeypatch.setattr(duplicates, "_INDEX_BATCH_SIZE", 1)
    flushed_batch_sizes: list[int] = []
    flush_batch = duplicates._flush_index_batch

    def record_flush(
        connection: sqlite3.Connection,
        narratives: list[tuple[int, str, str, str | None, str | None, str, bytes]],
        disk_guard: duplicates._DiskSpaceGuard | None = None,
    ) -> None:
        batch_size = len(narratives)
        flush_batch(connection, narratives, disk_guard)
        flushed_batch_sizes.append(batch_size)

    monkeypatch.setattr(duplicates, "_flush_index_batch", record_flush)

    with pytest.raises(RuntimeError, match="phase=corpus-index") as error:
        build_duplicate_audit(tmp_path, "fixture-2026")

    assert "phase=corpus-index" in str(error.value)
    assert "last_completed_phase=corpus-index" in str(error.value)
    assert f"minimum_free_bytes={minimum_free - 1}" in str(error.value)
    assert flushed_batch_sizes == [1]
    output_parent = tmp_path / "audits" / "fixture-2026"
    assert not (output_parent / "narrative-duplicate-audit-v2").exists()
    assert list(output_parent.glob(".duplicate-audit-*")) == []


def test_audit_cli_reports_disk_guard_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from typer.testing import CliRunner

    from maude.cli import app

    _write_corpus(tmp_path)
    _patch_disk_usage(monkeypatch, [10 * 1024**3 - 1])

    result = CliRunner().invoke(
        app,
        ["audit-duplicates", "fixture-2026", "--data-root", str(tmp_path)],
    )

    assert result.exit_code == 2
    assert "last_completed_phase=preflight" in result.output
    assert "minimum_free_bytes=" in result.output


def test_audit_rejects_corpus_checksum_mismatch(tmp_path: Path) -> None:
    from maude.features.duplicates import build_duplicate_audit

    corpus_path = _write_corpus(tmp_path)
    corpus_path.write_text(corpus_path.read_text() + "\n")

    with pytest.raises(ValueError, match="checksum"):
        build_duplicate_audit(tmp_path, "fixture-2026")


def test_audit_duplicates_cli_reports_the_generated_audit(tmp_path: Path) -> None:
    from typer.testing import CliRunner

    from maude.cli import app

    _write_corpus(tmp_path)
    result = CliRunner().invoke(
        app,
        ["audit-duplicates", "fixture-2026", "--data-root", str(tmp_path)],
    )

    assert result.exit_code == 0, result.output
    assert "AUDITED fixture-2026 narratives=7" in result.output
    assert "near_cross_split=1" in result.output
