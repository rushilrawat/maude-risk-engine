import hashlib
import json
import sqlite3
from datetime import UTC, date, datetime
from pathlib import Path

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


def _write_corpus(data_root: Path) -> Path:
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


def test_audit_skips_candidate_search_for_training_documents(
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
    searched_splits: list[str] = []

    def record_candidate_search(postings_by_split, candidate_narratives, candidates_for, split):
        searched_splits.append(split)
        return [], False

    monkeypatch.setattr(duplicates, "_candidate_records", record_candidate_search)
    connection = sqlite3.connect(":memory:")
    try:
        duplicates._index_schema(connection)
        duplicates._index_corpus(connection, corpus_path, "fixture-2026", "nfc-whitespace-v1")
    finally:
        connection.close()

    assert searched_splits == ["validation"]


def test_audit_does_not_unpack_narratives_without_candidates(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import maude.features.duplicates as duplicates

    corpus_path = tmp_path / "documents.jsonl"
    documents = (
        _document("train-1", date(2024, 1, 1), "alpha beta gamma delta"),
        _document("validation-1", date(2025, 1, 1), "one two three four"),
    )
    corpus_path.write_text("".join(f"{item.model_dump_json()}\n" for item in documents))
    unpacked: list[bytes] = []
    original_unpack = duplicates._unpack_shingles

    def record_unpack(packed: bytes) -> tuple[int, ...]:
        unpacked.append(packed)
        return original_unpack(packed)

    monkeypatch.setattr(duplicates, "_unpack_shingles", record_unpack)
    connection = sqlite3.connect(":memory:")
    try:
        duplicates._index_schema(connection)
        duplicates._index_corpus(connection, corpus_path, "fixture-2026", "nfc-whitespace-v1")
    finally:
        connection.close()

    assert unpacked == []


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
    assert manifest.near_candidate_search_is_exhaustive is False
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


def test_audit_finds_near_duplicates_when_corpus_order_reverses_temporal_order(
    tmp_path: Path,
) -> None:
    from maude.features.duplicates import build_duplicate_audit

    corpus_path = _write_corpus(tmp_path)
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

    result = build_duplicate_audit(tmp_path, "fixture-2026")

    assert result.near_cross_split_pairs == 1
    near_pairs = [
        json.loads(line) for line in Path(result.near_pairs_path).read_text().splitlines()
    ]
    assert near_pairs[0]["left_report_id"] == "train-near"
    assert near_pairs[0]["left_split"] == "train"
    assert near_pairs[0]["right_report_id"] == "validation-near"
    assert near_pairs[0]["right_split"] == "validation"


def test_audit_is_immutable_and_rejects_modified_results(tmp_path: Path) -> None:
    from maude.features.duplicates import build_duplicate_audit

    _write_corpus(tmp_path)
    first = build_duplicate_audit(tmp_path, "fixture-2026")
    second = build_duplicate_audit(tmp_path, "fixture-2026")

    assert second.model_dump() == first.model_dump()
    Path(first.near_pairs_path).write_text("tampered\n")

    with pytest.raises(ValueError, match="checksum"):
        build_duplicate_audit(tmp_path, "fixture-2026")


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
