"""Leakage-safe, bounded-memory word and character TF-IDF baselines."""

import hashlib
import heapq
import json
import os
import re
import tempfile
from collections.abc import Iterator
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import TypedDict

import joblib  # type: ignore[import-untyped]
import numpy as np
from scipy import sparse  # type: ignore[import-untyped]
from sklearn.feature_extraction.text import TfidfVectorizer  # type: ignore[import-untyped]

from maude.domain.models import (
    ModelingCorpusManifest,
    ModelingDocument,
    TfidfBaselineManifest,
)
from maude.features.splits import (
    TEMPORAL_SPLIT_VERSION,
    TRAINING_END,
    VALIDATION_END,
    split_for_received_date,
)
from maude.ingestion.manifests import fsync_directory, write_json_model

TFIDF_FEATURE_VERSION = "tfidf-wordchar-v1"
_SNAPSHOT_ID = re.compile(r"[a-zA-Z0-9._-]+")


class VectorizerConfig(TypedDict):
    analyzer: str
    ngram_range: tuple[int, int]
    min_df: int
    max_df: float
    max_features: int
    sublinear_tf: bool
    dtype: str
    norm: str


_VECTOR_CONFIG: dict[str, VectorizerConfig] = {
    "word": {
        "analyzer": "word",
        "ngram_range": (1, 2),
        "min_df": 2,
        "max_df": 0.98,
        "max_features": 200_000,
        "sublinear_tf": True,
        "dtype": "float32",
        "norm": "l2",
    },
    "character": {
        "analyzer": "char_wb",
        "ngram_range": (3, 5),
        "min_df": 2,
        "max_df": 0.98,
        "max_features": 250_000,
        "sublinear_tf": True,
        "dtype": "float32",
        "norm": "l2",
    },
}


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _read_documents(path: Path) -> Iterator[ModelingDocument]:
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                yield ModelingDocument.model_validate_json(line)


def _select_training_sample(
    corpus_path: Path, training_end: date, sample_limit: int
) -> tuple[list[ModelingDocument], str]:
    """Keep the lowest stable report-ID hashes among training-period documents."""
    selected: list[tuple[int, str, ModelingDocument]] = []
    for document in _read_documents(corpus_path):
        if document.date_received is None or document.date_received > training_end:
            continue
        rank = int.from_bytes(hashlib.sha256(document.report_id.encode()).digest(), "big")
        entry = (-rank, document.report_id, document)
        if len(selected) < sample_limit:
            heapq.heappush(selected, entry)
        elif entry > selected[0]:
            heapq.heapreplace(selected, entry)

    if not selected:
        raise ValueError(
            f"corpus has no dated training documents through {training_end.isoformat()}"
        )

    sample = [entry[2] for entry in sorted(selected, key=lambda item: item[1])]
    ids_digest = hashlib.sha256("\n".join(item.report_id for item in sample).encode()).hexdigest()
    return sample, ids_digest


def _new_vectorizer(name: str) -> TfidfVectorizer:
    config = _VECTOR_CONFIG[name]
    return TfidfVectorizer(
        analyzer=str(config["analyzer"]),
        ngram_range=config["ngram_range"],
        min_df=config["min_df"],
        max_df=config["max_df"],
        max_features=config["max_features"],
        sublinear_tf=config["sublinear_tf"],
        dtype=np.float32,
        norm=str(config["norm"]),
    )


def _manifest_vectorizer_config() -> dict[str, object]:
    return {
        name: {**config, "ngram_range": list(config["ngram_range"])}
        for name, config in _VECTOR_CONFIG.items()
    }


def _artifact_hashes(root: Path) -> dict[str, str]:
    return {
        path.relative_to(root).as_posix(): _sha256_file(path)
        for path in sorted(root.rglob("*"))
        if path.is_file() and path.name != "manifest.json"
    }


def _load_existing(output_directory: Path) -> TfidfBaselineManifest:
    manifest = TfidfBaselineManifest.model_validate_json(
        (output_directory / "manifest.json").read_text(encoding="utf-8")
    )
    if Path(manifest.output_directory) != output_directory:
        raise ValueError(f"TF-IDF manifest points to an unexpected output: {output_directory}")
    for relative_path, expected_hash in manifest.artifact_sha256.items():
        artifact = output_directory / relative_path
        if Path(relative_path).is_absolute() or ".." in Path(relative_path).parts:
            raise ValueError(f"unsafe artifact path in TF-IDF manifest: {relative_path}")
        if (
            artifact.is_symlink()
            or not artifact.resolve().is_relative_to(output_directory.resolve())
            or not artifact.is_file()
            or _sha256_file(artifact) != expected_hash
        ):
            raise ValueError(f"TF-IDF artifact checksum mismatch: {relative_path}")
    return manifest


def build_tfidf_baseline(
    data_root: Path,
    snapshot_id: str,
    batch_rows: int = 5_000,
    fit_sample_size: int = 10_000,
) -> TfidfBaselineManifest:
    """Fit both vectorizers on a bounded training-only sample and batch-transform all periods."""
    if batch_rows < 1:
        raise ValueError("batch_rows must be at least 1")
    if fit_sample_size < 1:
        raise ValueError("fit_sample_size must be at least 1")
    if not _SNAPSHOT_ID.fullmatch(snapshot_id) or snapshot_id in {".", ".."}:
        raise ValueError(
            "snapshot_id must contain only letters, numbers, dots, underscores, or hyphens"
        )

    root = Path(data_root)
    corpus_directory = root / "corpora" / snapshot_id / "nfc-whitespace-v1"
    corpus_manifest_path = corpus_directory / "manifest.json"
    if corpus_manifest_path.is_symlink():
        raise ValueError("corpus manifest must not be a symlink")
    corpus_manifest = ModelingCorpusManifest.model_validate_json(
        corpus_manifest_path.read_text(encoding="utf-8")
    )
    if corpus_manifest.snapshot_id != snapshot_id:
        raise ValueError("corpus snapshot ID does not match requested snapshot")
    if (
        corpus_manifest.corpus_format_version != "1"
        or corpus_manifest.cleaning_version != "nfc-whitespace-v1"
    ):
        raise ValueError("unsupported corpus format or cleaning version")
    corpus_path = Path(corpus_manifest.output_path)
    if (
        corpus_path != corpus_directory / "documents.jsonl"
        or corpus_path.is_symlink()
        or not corpus_path.is_file()
    ):
        raise ValueError("corpus manifest points outside the expected snapshot corpus directory")
    if _sha256_file(corpus_path) != corpus_manifest.output_sha256:
        raise ValueError("corpus output checksum does not match its manifest")

    output_directory = root / "features" / snapshot_id / TFIDF_FEATURE_VERSION
    if output_directory.exists():
        if output_directory.is_symlink():
            raise ValueError("TF-IDF output directory must not be a symlink")
        existing = _load_existing(output_directory)
        if (
            existing.snapshot_id != snapshot_id
            or existing.feature_version != TFIDF_FEATURE_VERSION
            or existing.corpus_sha256 != corpus_manifest.output_sha256
            or existing.fit_sample_limit != fit_sample_size
        ):
            raise ValueError("existing TF-IDF output has different inputs or configuration")
        return existing

    repository_root = Path(__file__).resolve().parents[3]
    lock_path = repository_root / "uv.lock"
    if not lock_path.is_file():
        raise FileNotFoundError(f"dependency lockfile not found: {lock_path}")
    lock_sha256 = _sha256_file(lock_path)

    sample, sample_ids_sha256 = _select_training_sample(corpus_path, TRAINING_END, fit_sample_size)
    vectorizers = {name: _new_vectorizer(name) for name in _VECTOR_CONFIG}
    fit_texts = [document.modeling_text for document in sample]
    for vectorizer in vectorizers.values():
        vectorizer.fit(fit_texts)

    output_directory.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(
        prefix=".tfidf-", dir=output_directory.parent
    ) as temporary_directory:
        staging = Path(temporary_directory)
        for name, vectorizer in vectorizers.items():
            joblib.dump(vectorizer, staging / f"{name}-vectorizer.joblib", compress=3)

        split_document_counts = {"train": 0, "validation": 0, "test": 0}
        shard_counts = {"train": 0, "validation": 0, "test": 0}
        buffers: dict[str, list[ModelingDocument]] = {
            "train": [],
            "validation": [],
            "test": [],
        }
        missing_date_documents = 0

        def flush(split: str) -> None:
            documents = buffers[split]
            if not documents:
                return
            shard = shard_counts[split]
            rows = [document.modeling_text for document in documents]
            row_metadata = staging / "rows" / split / f"{shard:06d}.jsonl"
            row_metadata.parent.mkdir(parents=True, exist_ok=True)
            row_metadata.write_text(
                "".join(
                    json.dumps(
                        {
                            "report_id": document.report_id,
                            "date_received": document.date_received.isoformat()
                            if document.date_received
                            else None,
                        },
                        sort_keys=True,
                    )
                    + "\n"
                    for document in documents
                ),
                encoding="utf-8",
            )
            for name, vectorizer in vectorizers.items():
                matrix_path = staging / "matrices" / name / split / f"{shard:06d}.npz"
                matrix_path.parent.mkdir(parents=True, exist_ok=True)
                sparse.save_npz(matrix_path, vectorizer.transform(rows), compressed=True)
            shard_counts[split] += 1
            documents.clear()

        for document in _read_documents(corpus_path):
            received = document.date_received
            split = split_for_received_date(received)
            if split is None:
                missing_date_documents += 1
                continue
            buffers[split].append(document)
            split_document_counts[split] += 1
            if len(buffers[split]) >= batch_rows:
                flush(split)
        for split_name in buffers:
            flush(split_name)

        artifact_sha256 = _artifact_hashes(staging)
        manifest = TfidfBaselineManifest(
            feature_version=TFIDF_FEATURE_VERSION,
            snapshot_id=snapshot_id,
            cleaning_version=corpus_manifest.cleaning_version,
            output_directory=str(output_directory),
            corpus_sha256=corpus_manifest.output_sha256,
            uv_lock_sha256=lock_sha256,
            training_end=TRAINING_END,
            split_version=TEMPORAL_SPLIT_VERSION,
            validation_start=TRAINING_END + timedelta(days=1),
            validation_end=VALIDATION_END,
            test_start=VALIDATION_END + timedelta(days=1),
            fit_sample_limit=fit_sample_size,
            fit_sample_size=len(sample),
            fit_sample_report_ids_sha256=sample_ids_sha256,
            split_document_counts=split_document_counts,
            matrix_shard_counts=shard_counts,
            missing_date_documents=missing_date_documents,
            feature_dimensions={
                name: len(vectorizer.vocabulary_) for name, vectorizer in vectorizers.items()
            },
            vectorizer_config=_manifest_vectorizer_config(),
            artifact_sha256=artifact_sha256,
            completed_at=datetime.now(UTC),
        )
        write_json_model(staging / "manifest.json", manifest)
        os.replace(staging, output_directory)
        fsync_directory(output_directory.parent)
        return manifest
