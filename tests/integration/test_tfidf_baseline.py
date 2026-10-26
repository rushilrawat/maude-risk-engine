import hashlib
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from maude.domain.models import ModelingCorpusManifest, ModelingDocument, ModelingNarrative
from maude.ingestion.manifests import write_json_model


def _document(report_id: str, received: date, text: str) -> ModelingDocument:
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
    corpus_dir = data_root / "corpora" / "fixture-2026" / "nfc-whitespace-v1"
    corpus_dir.mkdir(parents=True)
    documents = (
        _document("1", date(2023, 1, 1), "pump motor stalls unexpectedly"),
        _document("2", date(2023, 6, 1), "pump motor stops unexpectedly"),
        _document("3", date(2024, 1, 1), "catheter leakage causes injury"),
        _document("4", date(2024, 6, 1), "catheter leakage and device damage"),
        _document("5", date(2025, 3, 1), "qzxvalidationtoken battery issue"),
        _document("6", date(2026, 2, 1), "wwxtestonlytoken electrical fault"),
    )
    output_path = corpus_dir / "documents.jsonl"
    payload = "".join(f"{item.model_dump_json()}\n" for item in documents).encode()
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
    return manifest_path


def test_baseline_fits_train_only_and_writes_batched_sparse_features(tmp_path: Path) -> None:
    from maude.features.tfidf import build_tfidf_baseline

    _write_corpus(tmp_path)

    manifest = build_tfidf_baseline(tmp_path, "fixture-2026", batch_rows=2, fit_sample_size=4)

    import joblib
    from scipy.sparse import load_npz

    assert manifest.split_document_counts == {"train": 4, "validation": 1, "test": 1}
    assert manifest.split_version == "received-calendar-year-2024-2025-2026plus-v1"
    assert manifest.fit_sample_size == 4
    assert manifest.training_end == date(2024, 12, 31)
    assert manifest.validation_start == date(2025, 1, 1)
    assert manifest.validation_end == date(2025, 12, 31)
    assert manifest.test_start == date(2026, 1, 1)
    assert manifest.matrix_shard_counts == {"train": 2, "validation": 1, "test": 1}
    word = joblib.load(Path(manifest.output_directory) / "word-vectorizer.joblib")
    character = joblib.load(Path(manifest.output_directory) / "character-vectorizer.joblib")
    assert "qzxvalidationtoken" not in word.vocabulary_
    assert "wwxtestonlytoken" not in word.vocabulary_
    assert "qzx" not in character.vocabulary_
    assert "wwx" not in character.vocabulary_

    train_word_shards = sorted(
        (Path(manifest.output_directory) / "matrices/word/train").glob("*.npz")
    )
    validation_char_shards = sorted(
        (Path(manifest.output_directory) / "matrices/character/validation").glob("*.npz")
    )
    assert [load_npz(path).shape[0] for path in train_word_shards] == [2, 2]
    assert [load_npz(path).shape[0] for path in validation_char_shards] == [1]


def test_baseline_rerun_verifies_and_reuses_immutable_artifacts(tmp_path: Path) -> None:
    from maude.features.tfidf import build_tfidf_baseline

    _write_corpus(tmp_path)

    first = build_tfidf_baseline(tmp_path, "fixture-2026", batch_rows=2, fit_sample_size=4)
    second = build_tfidf_baseline(tmp_path, "fixture-2026", batch_rows=3, fit_sample_size=4)

    assert second.model_dump() == first.model_dump()
    assert second.artifact_sha256 == first.artifact_sha256


def test_baseline_rejects_corpus_checksum_mismatch(tmp_path: Path) -> None:
    from maude.features.tfidf import build_tfidf_baseline

    _write_corpus(tmp_path)
    output_path = tmp_path / "corpora/fixture-2026/nfc-whitespace-v1/documents.jsonl"
    output_path.write_text(output_path.read_text() + "\n")

    with pytest.raises(ValueError, match="checksum"):
        build_tfidf_baseline(tmp_path, "fixture-2026", fit_sample_size=4)


def test_baseline_rejects_modified_feature_artifact_on_rerun(tmp_path: Path) -> None:
    from maude.features.tfidf import build_tfidf_baseline

    _write_corpus(tmp_path)
    result = build_tfidf_baseline(tmp_path, "fixture-2026", fit_sample_size=4)
    vectorizer = Path(result.output_directory) / "word-vectorizer.joblib"
    vectorizer.write_bytes(vectorizer.read_bytes() + b"modified")

    with pytest.raises(ValueError, match="checksum"):
        build_tfidf_baseline(tmp_path, "fixture-2026", fit_sample_size=4)


def test_baseline_rejects_unsafe_snapshot_identifier(tmp_path: Path) -> None:
    from maude.features.tfidf import build_tfidf_baseline

    with pytest.raises(ValueError, match="snapshot_id"):
        build_tfidf_baseline(tmp_path, "../outside")
