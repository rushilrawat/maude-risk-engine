"""Audit repeated report keys and narrative duplicates across temporal splits."""

import hashlib
import heapq
import json
import os
import re
import sqlite3
import struct
import tempfile
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

from maude.domain.models import (
    DuplicateAuditManifest,
    ModelingCorpusManifest,
    ModelingDocument,
)
from maude.features.documents import NARRATIVE_CLEANING_VERSION
from maude.features.splits import (
    TEMPORAL_SPLIT_VERSION,
    TRAINING_END,
    VALIDATION_END,
    split_for_received_date,
)
from maude.ingestion.manifests import fsync_directory, write_json_model

DUPLICATE_AUDIT_VERSION = "narrative-duplicate-audit-v1"
_SNAPSHOT_ID = re.compile(r"[a-zA-Z0-9._-]+")
_WORD = re.compile(r"\w+", re.UNICODE)
_SHINGLE_SIZE = 3
_SIGNATURE_SIZE = 16
_CANDIDATE_LIMIT = 250
_NEAR_THRESHOLD = 0.8
_INDEX_BATCH_SIZE = 5_000
_NEAR_STRATEGY = "bottom-16-word-trigram-overlap; exact-Jaccard-verification"
_ARTIFACTS = (
    "exact_groups.jsonl",
    "exact_members.jsonl",
    "report_key_overlaps.jsonl",
    "near_pairs.jsonl",
)


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


def _normalize_exact(text: str) -> str:
    return " ".join(text.casefold().split())


def _shingle_hashes(text: str) -> tuple[int, ...]:
    tokens = _WORD.findall(text.casefold())
    if len(tokens) < _SHINGLE_SIZE:
        return ()
    hashes = {
        int.from_bytes(
            hashlib.blake2b(
                " ".join(tokens[index : index + _SHINGLE_SIZE]).encode("utf-8"),
                digest_size=8,
                person=b"maude-shingle",
            ).digest(),
            "big",
        )
        for index in range(len(tokens) - _SHINGLE_SIZE + 1)
    }
    return tuple(sorted(hashes))


def _candidate_shingles(shingles: tuple[int, ...]) -> tuple[bytes, ...]:
    return tuple(value.to_bytes(8, "big") for value in shingles[:_SIGNATURE_SIZE])


def _pack_shingles(shingles: tuple[int, ...]) -> bytes:
    return struct.pack(f">{len(shingles)}Q", *shingles) if shingles else b""


def _unpack_shingles(value: bytes) -> tuple[int, ...]:
    return tuple(item[0] for item in struct.iter_unpack(">Q", value))


def _similarity(left: tuple[int, ...], right: tuple[int, ...]) -> float:
    left_set = set(left)
    right_set = set(right)
    union_size = len(left_set | right_set)
    return len(left_set & right_set) / union_size if union_size else 0.0


def _index_schema(connection: sqlite3.Connection) -> None:
    # This disposable scratch index is rebuilt after failure; keep its cache bounded.
    connection.execute("PRAGMA journal_mode = OFF")
    connection.execute("PRAGMA synchronous = OFF")
    connection.execute("PRAGMA temp_store = FILE")
    connection.execute("PRAGMA cache_size = -65536")
    connection.executescript(
        """
        CREATE TABLE narratives (
            id INTEGER PRIMARY KEY,
            report_id TEXT NOT NULL,
            evidence_id TEXT NOT NULL,
            received_date TEXT,
            split TEXT,
            exact_sha256 TEXT NOT NULL,
            shingles BLOB NOT NULL,
            candidate_signature BLOB NOT NULL
        );
        CREATE INDEX narratives_exact ON narratives(exact_sha256, report_id);
        CREATE INDEX narratives_report ON narratives(report_id, split);
        CREATE TABLE near_pairs (
            left_id INTEGER NOT NULL REFERENCES narratives(id),
            right_id INTEGER NOT NULL REFERENCES narratives(id),
            similarity REAL NOT NULL,
            PRIMARY KEY (left_id, right_id)
        );
        """
    )


def _candidate_records(
    postings_by_split: dict[str, dict[bytes, list[int]]],
    candidate_narratives: dict[int, tuple[str, str, bytes]],
    candidates_for: tuple[bytes, ...],
    split: str,
) -> tuple[list[tuple[int, str, str, bytes]], bool]:
    prior_splits = {"validation": ("train",), "test": ("train", "validation")}.get(split, ())
    if not candidates_for or not prior_splits:
        return [], False
    posting_lists = [
        shingle_postings[signature]
        for prior_split in prior_splits
        if (shingle_postings := postings_by_split.get(prior_split)) is not None
        for signature in candidates_for
        if signature in shingle_postings
    ]
    selected_ids: list[int] = []
    previous_id = 0
    overflow = False
    for candidate_id in heapq.merge(*posting_lists):
        if candidate_id == previous_id:
            continue
        previous_id = candidate_id
        if len(selected_ids) == _CANDIDATE_LIMIT:
            overflow = True
            break
        selected_ids.append(candidate_id)
    return [
        (candidate_id, *candidate_narratives[candidate_id]) for candidate_id in selected_ids
    ], overflow


def _flush_index_batch(
    connection: sqlite3.Connection,
    narratives: list[tuple[int, str, str, str | None, str | None, str, bytes, bytes]],
) -> None:
    if not narratives:
        return
    connection.executemany(
        """INSERT INTO narratives
           (id, report_id, evidence_id, received_date, split, exact_sha256, shingles,
            candidate_signature)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        narratives,
    )
    narratives.clear()


def _index_corpus(
    connection: sqlite3.Connection,
    corpus_path: Path,
    snapshot_id: str,
    cleaning_version: str,
) -> tuple[int, int, int]:
    narratives_scanned = 0
    missing_date_narratives = 0
    near_candidate_overflow_documents = 0
    narrative_batch: list[tuple[int, str, str, str | None, str | None, str, bytes, bytes]] = []
    postings_by_split: dict[str, dict[bytes, list[int]]] = {}
    candidate_narratives: dict[int, tuple[str, str, bytes]] = {}

    for document in _read_documents(corpus_path):
        if document.dataset_snapshot_id != snapshot_id:
            raise ValueError("corpus document snapshot ID does not match its manifest")
        split = split_for_received_date(document.date_received)
        for narrative in document.narratives:
            if narrative.report_id != document.report_id:
                raise ValueError("corpus narrative report ID does not match its document")
            if narrative.cleaning_version != cleaning_version:
                raise ValueError("corpus narrative cleaning version does not match its manifest")
            normalized = _normalize_exact(narrative.modeling_text)
            if not normalized:
                raise ValueError("corpus contains a blank narrative; rebuild the corpus")

            exact_sha256 = hashlib.sha256(normalized.encode("utf-8")).hexdigest()
            shingles = _shingle_hashes(normalized)
            candidates_for = _candidate_shingles(shingles) if split is not None else ()
            packed_shingles = _pack_shingles(shingles)
            if split is None:
                missing_date_narratives += 1

            narrative_id = narratives_scanned + 1
            if split in {"train", "validation"}:
                candidate_narratives[narrative_id] = (
                    document.report_id,
                    exact_sha256,
                    packed_shingles,
                )
                split_postings = postings_by_split.setdefault(split, {})
                for signature in candidates_for:
                    split_postings.setdefault(signature, []).append(narrative_id)

            narrative_batch.append(
                (
                    narrative_id,
                    document.report_id,
                    narrative.evidence_id,
                    document.date_received.isoformat() if document.date_received else None,
                    split,
                    exact_sha256,
                    packed_shingles,
                    b"".join(candidates_for),
                ),
            )
            narratives_scanned += 1
            if len(narrative_batch) >= _INDEX_BATCH_SIZE:
                _flush_index_batch(connection, narrative_batch)

    _flush_index_batch(connection, narrative_batch)
    connection.commit()

    cursor = connection.execute(
        """SELECT id, report_id, split, exact_sha256, shingles, candidate_signature
           FROM narratives
           WHERE split IN ('validation', 'test')"""
    )
    while rows := cursor.fetchmany(1_000):
        for (
            narrative_id,
            report_id,
            split,
            exact_sha256,
            packed_shingles,
            packed_candidate_signature,
        ) in rows:
            signature_bytes = bytes(packed_candidate_signature)
            candidates_for = tuple(
                signature_bytes[index : index + 8] for index in range(0, len(signature_bytes), 8)
            )
            candidate_records, overflow = _candidate_records(
                postings_by_split, candidate_narratives, candidates_for, str(split)
            )
            near_candidate_overflow_documents += int(overflow)
            eligible_candidates = [
                candidate
                for candidate in candidate_records
                if candidate[1] != report_id and candidate[2] != exact_sha256
            ]
            if not eligible_candidates:
                continue

            shingles = _unpack_shingles(bytes(packed_shingles))
            for candidate_id, _, _, candidate_shingles in eligible_candidates:
                similarity = _similarity(shingles, _unpack_shingles(candidate_shingles))
                if similarity >= _NEAR_THRESHOLD:
                    connection.execute(
                        "INSERT OR IGNORE INTO near_pairs VALUES (?, ?, ?)",
                        (candidate_id, narrative_id, round(similarity, 6)),
                    )

    connection.commit()
    return narratives_scanned, missing_date_narratives, near_candidate_overflow_documents


def _write_exact_outputs(connection: sqlite3.Connection, staging: Path) -> tuple[int, int]:
    exact_groups_path = staging / "exact_groups.jsonl"
    exact_members_path = staging / "exact_members.jsonl"
    group_count = 0
    cross_split_count = 0
    duplicate_groups = connection.execute(
        """SELECT exact_sha256, COUNT(*), COUNT(DISTINCT report_id)
           FROM narratives GROUP BY exact_sha256
           HAVING COUNT(DISTINCT report_id) > 1 ORDER BY exact_sha256"""
    )
    with exact_groups_path.open("w", encoding="utf-8", newline="\n") as groups_file:
        for exact_sha256, narrative_count, report_count in duplicate_groups:
            split_counts = {
                str(split) if split is not None else "undated": int(count)
                for split, count in connection.execute(
                    """SELECT split, COUNT(*) FROM narratives WHERE exact_sha256 = ?
                       GROUP BY split ORDER BY COALESCE(split, 'undated')""",
                    (exact_sha256,),
                )
            }
            cross_temporal_split = len(set(split_counts) - {"undated"}) > 1
            group_count += 1
            cross_split_count += int(cross_temporal_split)
            groups_file.write(
                json.dumps(
                    {
                        "cross_temporal_split": cross_temporal_split,
                        "narrative_count": int(narrative_count),
                        "normalized_text_sha256": str(exact_sha256),
                        "report_count": int(report_count),
                        "split_counts": split_counts,
                    },
                    sort_keys=True,
                )
                + "\n"
            )

    with exact_members_path.open("w", encoding="utf-8", newline="\n") as members_file:
        members = connection.execute(
            """SELECT n.exact_sha256, n.report_id, n.evidence_id, n.received_date, n.split
               FROM narratives AS n
               WHERE n.exact_sha256 IN (
                   SELECT exact_sha256 FROM narratives GROUP BY exact_sha256
                   HAVING COUNT(DISTINCT report_id) > 1
               )
               ORDER BY n.exact_sha256, n.report_id, n.evidence_id"""
        )
        for exact_sha256, report_id, evidence_id, received_date, split in members:
            members_file.write(
                json.dumps(
                    {
                        "evidence_id": str(evidence_id),
                        "normalized_text_sha256": str(exact_sha256),
                        "received_date": received_date,
                        "report_id": str(report_id),
                        "split": str(split) if split is not None else "undated",
                    },
                    sort_keys=True,
                )
                + "\n"
            )
    return group_count, cross_split_count


def _write_report_key_overlaps(connection: sqlite3.Connection, path: Path) -> int:
    groups = connection.execute(
        """SELECT COUNT(*) FROM (
               SELECT report_id FROM narratives WHERE split IS NOT NULL
               GROUP BY report_id HAVING COUNT(DISTINCT split) > 1
           )"""
    ).fetchone()
    group_count = int(groups[0] or 0) if groups else 0
    rows = connection.execute(
        """SELECT report_id, split, COUNT(*) FROM narratives
           WHERE report_id IN (
               SELECT report_id FROM narratives WHERE split IS NOT NULL
               GROUP BY report_id HAVING COUNT(DISTINCT split) > 1
           )
           GROUP BY report_id, split ORDER BY report_id, COALESCE(split, 'undated')"""
    )
    with path.open("w", encoding="utf-8", newline="\n") as output:
        report_id: str | None = None
        split_counts: dict[str, int] = {}

        def flush() -> None:
            if report_id is None:
                return
            output.write(
                json.dumps(
                    {
                        "narrative_count": sum(split_counts.values()),
                        "report_id": report_id,
                        "split_counts": split_counts,
                    },
                    sort_keys=True,
                )
                + "\n"
            )

        for current_id, split, count in rows:
            current_id = str(current_id)
            if report_id is not None and current_id != report_id:
                flush()
                split_counts = {}
            report_id = current_id
            split_counts[str(split) if split is not None else "undated"] = int(count)
        flush()
    return group_count


def _write_near_pairs(connection: sqlite3.Connection, path: Path) -> int:
    rows = connection.execute(
        """SELECT l.report_id, l.evidence_id, l.split,
                  r.report_id, r.evidence_id, r.split, p.similarity
           FROM near_pairs AS p
           JOIN narratives AS l ON l.id = p.left_id
           JOIN narratives AS r ON r.id = p.right_id
           ORDER BY l.report_id, l.evidence_id, r.report_id, r.evidence_id"""
    )
    pair_count = 0
    with path.open("w", encoding="utf-8", newline="\n") as output:
        for (
            left_report,
            left_evidence,
            left_split,
            right_report,
            right_evidence,
            right_split,
            score,
        ) in rows:
            output.write(
                json.dumps(
                    {
                        "jaccard_similarity": float(score),
                        "left_evidence_id": str(left_evidence),
                        "left_report_id": str(left_report),
                        "left_split": str(left_split),
                        "right_evidence_id": str(right_evidence),
                        "right_report_id": str(right_report),
                        "right_split": str(right_split),
                    },
                    sort_keys=True,
                )
                + "\n"
            )
            pair_count += 1
    return pair_count


def _artifact_hashes(staging: Path) -> dict[str, str]:
    return {name: _sha256_file(staging / name) for name in _ARTIFACTS}


def _load_existing(output_directory: Path) -> DuplicateAuditManifest:
    manifest_path = output_directory / "manifest.json"
    if manifest_path.is_symlink():
        raise ValueError("duplicate-audit manifest must not be a symlink")
    manifest = DuplicateAuditManifest.model_validate_json(manifest_path.read_text(encoding="utf-8"))
    if Path(manifest.output_directory) != output_directory:
        raise ValueError("duplicate-audit manifest points to an unexpected output directory")
    expected_paths = {
        "exact_groups_path": output_directory / "exact_groups.jsonl",
        "exact_members_path": output_directory / "exact_members.jsonl",
        "report_key_overlaps_path": output_directory / "report_key_overlaps.jsonl",
        "near_pairs_path": output_directory / "near_pairs.jsonl",
    }
    if any(
        Path(getattr(manifest, field)) != expected for field, expected in expected_paths.items()
    ):
        raise ValueError("duplicate-audit manifest contains unexpected artifact paths")
    if set(manifest.artifact_sha256) != set(_ARTIFACTS):
        raise ValueError("duplicate-audit manifest has an unexpected artifact set")
    for relative_path, expected_hash in manifest.artifact_sha256.items():
        artifact = output_directory / relative_path
        if (
            Path(relative_path).is_absolute()
            or ".." in Path(relative_path).parts
            or artifact.is_symlink()
            or not artifact.resolve().is_relative_to(output_directory.resolve())
            or not artifact.is_file()
            or _sha256_file(artifact) != expected_hash
        ):
            raise ValueError(f"duplicate-audit artifact checksum mismatch: {relative_path}")
    return manifest


def build_duplicate_audit(data_root: Path, snapshot_id: str) -> DuplicateAuditManifest:
    """Build an immutable, disk-backed duplicate and cross-split leakage report."""
    if not _SNAPSHOT_ID.fullmatch(snapshot_id) or snapshot_id in {".", ".."}:
        raise ValueError(
            "snapshot_id must contain only letters, numbers, dots, underscores, or hyphens"
        )

    root = Path(data_root)
    corpus_directory = root / "corpora" / snapshot_id / NARRATIVE_CLEANING_VERSION
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
        or corpus_manifest.cleaning_version != NARRATIVE_CLEANING_VERSION
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

    output_directory = root / "audits" / snapshot_id / DUPLICATE_AUDIT_VERSION
    if output_directory.exists():
        if output_directory.is_symlink():
            raise ValueError("duplicate-audit output directory must not be a symlink")
        existing = _load_existing(output_directory)
        if (
            existing.audit_version != DUPLICATE_AUDIT_VERSION
            or existing.snapshot_id != snapshot_id
            or existing.corpus_sha256 != corpus_manifest.output_sha256
            or existing.split_version != TEMPORAL_SPLIT_VERSION
            or existing.cleaning_version != corpus_manifest.cleaning_version
            or existing.near_candidate_strategy != _NEAR_STRATEGY
            or existing.near_similarity_threshold != _NEAR_THRESHOLD
        ):
            raise ValueError("existing duplicate audit has different inputs or configuration")
        return existing

    output_directory.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(
        prefix=".duplicate-audit-", dir=output_directory.parent
    ) as temporary_directory:
        staging = Path(temporary_directory)
        connection = sqlite3.connect(staging / "index.sqlite")
        try:
            _index_schema(connection)
            narratives_scanned, missing_date_narratives, overflow_count = _index_corpus(
                connection,
                corpus_path,
                snapshot_id,
                corpus_manifest.cleaning_version,
            )
            if narratives_scanned != corpus_manifest.narratives_written:
                raise ValueError("corpus narrative count does not match its manifest")
            exact_groups, exact_cross_split_groups = _write_exact_outputs(connection, staging)
            report_key_cross_split_groups = _write_report_key_overlaps(
                connection, staging / "report_key_overlaps.jsonl"
            )
            near_cross_split_pairs = _write_near_pairs(connection, staging / "near_pairs.jsonl")
        finally:
            connection.close()

        (staging / "index.sqlite").unlink()
        manifest = DuplicateAuditManifest(
            audit_version=DUPLICATE_AUDIT_VERSION,
            snapshot_id=snapshot_id,
            cleaning_version=corpus_manifest.cleaning_version,
            corpus_sha256=corpus_manifest.output_sha256,
            output_directory=str(output_directory),
            split_version=TEMPORAL_SPLIT_VERSION,
            training_end=TRAINING_END,
            validation_start=TRAINING_END + timedelta(days=1),
            validation_end=VALIDATION_END,
            test_start=VALIDATION_END + timedelta(days=1),
            narratives_scanned=narratives_scanned,
            missing_date_narratives=missing_date_narratives,
            exact_duplicate_groups=exact_groups,
            exact_cross_split_groups=exact_cross_split_groups,
            report_key_cross_split_groups=report_key_cross_split_groups,
            near_cross_split_pairs=near_cross_split_pairs,
            near_candidate_overflow_documents=overflow_count,
            near_candidate_strategy=_NEAR_STRATEGY,
            near_similarity_threshold=_NEAR_THRESHOLD,
            near_candidate_search_is_exhaustive=False,
            exact_groups_path=str(output_directory / "exact_groups.jsonl"),
            exact_members_path=str(output_directory / "exact_members.jsonl"),
            report_key_overlaps_path=str(output_directory / "report_key_overlaps.jsonl"),
            near_pairs_path=str(output_directory / "near_pairs.jsonl"),
            artifact_sha256=_artifact_hashes(staging),
            completed_at=datetime.now(UTC),
        )
        write_json_model(staging / "manifest.json", manifest)
        os.replace(staging, output_directory)
        fsync_directory(output_directory.parent)
        return manifest
