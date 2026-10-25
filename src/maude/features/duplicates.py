"""Audit repeated report keys and narrative duplicates across temporal splits."""

import hashlib
import json
import os
import re
import shutil
import sqlite3
import struct
import tempfile
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from fractions import Fraction
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

DUPLICATE_AUDIT_VERSION = "narrative-duplicate-audit-v2"
DUPLICATE_ALGORITHM_VERSION = "sqlite-ppjoin-jaccard-v1"
_SNAPSHOT_ID = re.compile(r"[a-zA-Z0-9._-]+")
_WORD = re.compile(r"\w+", re.UNICODE)
_SHINGLE_SIZE = 3
_NEAR_THRESHOLD = 0.8
_INDEX_BATCH_SIZE = 5_000
_JOIN_TARGET_BATCH_SIZE = 250
_POSTING_BATCH_SIZE = 50_000
_NEAR_THRESHOLD_FRACTION = Fraction(str(_NEAR_THRESHOLD))
_MIN_FREE_DISK_BYTES = 10 * 1024**3
# Conservative entry sizes calibrated against a 4 KiB-page SQLite microbenchmark.
_HASH_INDEX_BYTES_PER_POSTING = 24
_TOKEN_ORDER_BYTES_PER_SHINGLE = 64
_TOKEN_RANK_UPDATE_BYTES_PER_POSTING = 12
_NARRATIVE_RANK_INDEX_BYTES_PER_POSTING = 40
_RANK_SPLIT_INDEX_BYTES_PER_POSTING = 32
_NEAR_STRATEGY = "sqlite-df-prefix-length-position-exact-jaccard-v1"
_ARTIFACTS = (
    "exact_groups.jsonl",
    "exact_members.jsonl",
    "report_key_overlaps.jsonl",
    "near_pairs.jsonl",
)


class InsufficientDiskSpaceError(RuntimeError):
    """The audit stopped before a bounded write could consume its disk reserve."""

    def __init__(
        self,
        phase: str,
        last_completed_phase: str,
        minimum_free_bytes: int,
        free_bytes: int,
        anticipated_write_bytes: int,
    ) -> None:
        self.phase = phase
        self.last_completed_phase = last_completed_phase
        self.minimum_free_bytes = minimum_free_bytes
        self.free_bytes = free_bytes
        self.anticipated_write_bytes = anticipated_write_bytes
        super().__init__(
            "insufficient disk space: "
            f"phase={phase} last_completed_phase={last_completed_phase} "
            f"minimum_free_bytes={minimum_free_bytes} free_bytes={free_bytes} "
            f"anticipated_write_bytes={anticipated_write_bytes} "
            f"reserve_bytes={_MIN_FREE_DISK_BYTES}"
        )


class _DiskSpaceGuard:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.minimum_free_bytes: int | None = None
        self.last_completed_phase = "none"

    def check(self, phase: str, anticipated_write_bytes: int = 0) -> None:
        free_bytes = shutil.disk_usage(self.path).free
        if self.minimum_free_bytes is None or free_bytes < self.minimum_free_bytes:
            self.minimum_free_bytes = free_bytes
        if free_bytes - anticipated_write_bytes < _MIN_FREE_DISK_BYTES:
            raise InsufficientDiskSpaceError(
                phase=phase,
                last_completed_phase=self.last_completed_phase,
                minimum_free_bytes=self.minimum_free_bytes,
                free_bytes=free_bytes,
                anticipated_write_bytes=anticipated_write_bytes,
            )

    def complete(self, phase: str) -> None:
        self.last_completed_phase = phase


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


def _pack_shingles(shingles: tuple[int, ...]) -> bytes:
    return struct.pack(f">{len(shingles)}Q", *shingles) if shingles else b""


def _unpack_shingles(value: bytes) -> tuple[int, ...]:
    return tuple(item[0] for item in struct.iter_unpack(">Q", value))


def _reusable_database_bytes(connection: sqlite3.Connection) -> int:
    free_pages = int(connection.execute("PRAGMA freelist_count").fetchone()[0])
    page_size = int(connection.execute("PRAGMA page_size").fetchone()[0])
    return free_pages * page_size


def _build_exact_join_index(
    connection: sqlite3.Connection,
    disk_guard: _DiskSpaceGuard | None = None,
) -> None:
    if disk_guard is not None:
        disk_guard.check("posting-index", 1024 * 1024)
    connection.execute(
        """CREATE TABLE shingle_postings (
               narrative_id INTEGER NOT NULL,
               shingle_hash BLOB NOT NULL,
               split TEXT,
               token_rank INTEGER,
               token_position INTEGER
           )"""
    )
    posting_batch: list[tuple[int, bytes, str | None]] = []
    posting_count = 0
    prior_posting_count = 0
    narratives = connection.execute("SELECT id, split, shingles FROM narratives ORDER BY id")
    for narrative_id, split, packed_shingles in narratives:
        if split not in {"train", "validation"}:
            continue
        prior_posting_count += len(bytes(packed_shingles)) // 8
        for shingle in sorted(_unpack_shingles(bytes(packed_shingles))):
            posting_batch.append(
                (int(narrative_id), shingle.to_bytes(8, "big"), str(split) if split else None)
            )
            posting_count += 1
        if len(posting_batch) >= _POSTING_BATCH_SIZE:
            if disk_guard is not None:
                disk_guard.check("posting-index", len(posting_batch) * 128)
            connection.executemany(
                "INSERT INTO shingle_postings (narrative_id, shingle_hash, split) VALUES (?, ?, ?)",
                posting_batch,
            )
            posting_batch.clear()
            connection.commit()
    if posting_batch:
        if disk_guard is not None:
            disk_guard.check("posting-index", len(posting_batch) * 128)
        connection.executemany(
            "INSERT INTO shingle_postings (narrative_id, shingle_hash, split) VALUES (?, ?, ?)",
            posting_batch,
        )
    connection.commit()

    if disk_guard is not None:
        disk_guard.check("posting-hash-index", posting_count * _HASH_INDEX_BYTES_PER_POSTING)
    connection.execute("CREATE INDEX shingle_postings_by_hash ON shingle_postings(shingle_hash)")
    distinct_shingles = connection.execute(
        """SELECT COUNT(*) FROM (
               SELECT shingle_hash FROM shingle_postings GROUP BY shingle_hash
           )"""
    ).fetchone()
    distinct_shingle_count = int(distinct_shingles[0] or 0)
    if disk_guard is not None:
        anticipated_database_growth = (
            distinct_shingle_count * _TOKEN_ORDER_BYTES_PER_SHINGLE
            + posting_count * _TOKEN_RANK_UPDATE_BYTES_PER_POSTING
            - _reusable_database_bytes(connection)
        )
        disk_guard.check("token-order", max(1024 * 1024, anticipated_database_growth))
    connection.execute(
        """CREATE TABLE shingle_order (
               shingle_hash BLOB PRIMARY KEY,
               document_frequency INTEGER NOT NULL,
               token_rank INTEGER NOT NULL UNIQUE
           ) WITHOUT ROWID"""
    )
    connection.execute(
        """INSERT INTO shingle_order (shingle_hash, document_frequency, token_rank)
           SELECT shingle_hash, document_frequency,
                  ROW_NUMBER() OVER (ORDER BY document_frequency, shingle_hash)
           FROM (
               SELECT shingle_hash, COUNT(*) AS document_frequency
               FROM shingle_postings GROUP BY shingle_hash
           )"""
    )
    connection.execute(
        """UPDATE shingle_postings
           SET token_rank = (
               SELECT token_rank FROM shingle_order
               WHERE shingle_order.shingle_hash = shingle_postings.shingle_hash
           )"""
    )
    connection.commit()
    connection.execute("DROP INDEX shingle_postings_by_hash")
    if disk_guard is not None:
        disk_guard.check(
            "position-index",
            max(
                1024 * 1024,
                posting_count * _NARRATIVE_RANK_INDEX_BYTES_PER_POSTING
                - _reusable_database_bytes(connection),
            ),
        )
    connection.execute(
        "CREATE INDEX shingle_postings_by_narrative_rank "
        "ON shingle_postings(narrative_id, token_rank)"
    )

    last_narrative_id = 0
    last_token_rank = 0
    current_narrative_id: int | None = None
    position = 0
    while rows := connection.execute(
        """SELECT rowid, narrative_id, token_rank FROM shingle_postings
           WHERE (narrative_id, token_rank) > (?, ?)
           ORDER BY narrative_id, token_rank LIMIT ?""",
        (last_narrative_id, last_token_rank, _POSTING_BATCH_SIZE),
    ).fetchall():
        if disk_guard is not None:
            disk_guard.check("posting-positions", len(rows) * 16)
        position_updates: list[tuple[int, int]] = []
        for row_id, narrative_id, _token_rank in rows:
            narrative_id = int(narrative_id)
            if narrative_id != current_narrative_id:
                current_narrative_id = narrative_id
                position = 0
            position_updates.append((position, int(row_id)))
            position += 1
        connection.executemany(
            "UPDATE shingle_postings SET token_position = ? WHERE rowid = ?",
            position_updates,
        )
        last_narrative_id = int(rows[-1][1])
        last_token_rank = int(rows[-1][2])
        connection.commit()

    if disk_guard is not None:
        disk_guard.check(
            "ranked-postings-index",
            max(
                1024 * 1024,
                prior_posting_count * _RANK_SPLIT_INDEX_BYTES_PER_POSTING
                - _reusable_database_bytes(connection),
            ),
        )
    connection.execute(
        "CREATE INDEX shingle_postings_by_rank_split "
        "ON shingle_postings(token_rank, split, narrative_id, token_position) "
        "WHERE split IN ('train', 'validation')"
    )
    connection.commit()
    if disk_guard is not None:
        disk_guard.complete("shingle-postings")


def _run_exact_similarity_join(
    connection: sqlite3.Connection,
    disk_guard: _DiskSpaceGuard | None = None,
) -> int:
    """Insert every eligible cross-split Jaccard match into the disk-backed pair table."""
    _build_exact_join_index(connection, disk_guard)
    connection.execute(
        """CREATE TEMP TABLE target_shingles (
               target_id INTEGER NOT NULL,
               shingle_hash BLOB NOT NULL,
               PRIMARY KEY (target_id, shingle_hash)
           ) WITHOUT ROWID"""
    )
    connection.execute(
        """CREATE TEMP TABLE target_prefix (
               target_id INTEGER NOT NULL,
               token_rank INTEGER NOT NULL,
               token_position INTEGER NOT NULL,
               PRIMARY KEY (target_id, token_rank)
           ) WITHOUT ROWID"""
    )
    connection.execute(
        "CREATE INDEX target_prefix_by_rank ON target_prefix(token_rank, target_id, token_position)"
    )
    connection.execute(
        """CREATE TEMP TABLE candidate_bounds (
               target_id INTEGER NOT NULL,
               narrative_id INTEGER NOT NULL,
               maximum_overlap INTEGER NOT NULL,
               PRIMARY KEY (target_id, narrative_id)
           ) WITHOUT ROWID"""
    )

    threshold_numerator = _NEAR_THRESHOLD_FRACTION.numerator
    threshold_denominator = _NEAR_THRESHOLD_FRACTION.denominator
    threshold_scale = threshold_numerator + threshold_denominator
    last_target_id = 0
    while target_rows := connection.execute(
        """SELECT id, shingles FROM narratives
           WHERE id > ? AND split IN ('validation', 'test')
           ORDER BY id LIMIT ?""",
        (last_target_id, _JOIN_TARGET_BATCH_SIZE),
    ).fetchall():
        last_target_id = int(target_rows[-1][0])
        connection.execute("DELETE FROM target_shingles")
        connection.execute("DELETE FROM target_prefix")
        connection.execute("DELETE FROM candidate_bounds")
        target_shingle_batch: list[tuple[int, bytes]] = []
        for target_id_value, packed_shingles in target_rows:
            target_id = int(target_id_value)
            for shingle in _unpack_shingles(bytes(packed_shingles)):
                target_shingle_batch.append((target_id, shingle.to_bytes(8, "big")))
                if len(target_shingle_batch) >= _POSTING_BATCH_SIZE:
                    if disk_guard is not None:
                        disk_guard.check("exact-join-target-index", len(target_shingle_batch) * 32)
                    connection.executemany(
                        "INSERT INTO target_shingles (target_id, shingle_hash) VALUES (?, ?)",
                        target_shingle_batch,
                    )
                    target_shingle_batch.clear()
        if target_shingle_batch:
            if disk_guard is not None:
                disk_guard.check("exact-join-target-index", len(target_shingle_batch) * 32)
            connection.executemany(
                "INSERT INTO target_shingles (target_id, shingle_hash) VALUES (?, ?)",
                target_shingle_batch,
            )

        connection.execute(
            """INSERT INTO target_prefix (target_id, token_rank, token_position)
               SELECT ranked.target_id, ranked.token_rank, ranked.token_position
               FROM (
                   SELECT targets.target_id,
                          token_order.token_rank,
                          ROW_NUMBER() OVER (
                              PARTITION BY targets.target_id
                              ORDER BY COALESCE(token_order.document_frequency, 0),
                                       targets.shingle_hash
                          ) - 1 AS token_position,
                          COUNT(*) OVER (PARTITION BY targets.target_id) AS target_size
                   FROM target_shingles AS targets
                   LEFT JOIN shingle_order AS token_order
                     ON token_order.shingle_hash = targets.shingle_hash
               ) AS ranked
               WHERE ranked.token_rank IS NOT NULL
                 AND ranked.token_position < ranked.target_size
                     - ((? * ranked.target_size + ? - 1) / ?) + 1""",
            (threshold_numerator, threshold_denominator, threshold_denominator),
        )
        if disk_guard is not None:
            estimated_posting_matches = connection.execute(
                """SELECT COALESCE(SUM(token_order.document_frequency), 0)
                   FROM target_prefix AS prefixes
                   JOIN shingle_order AS token_order
                     ON token_order.token_rank = prefixes.token_rank"""
            ).fetchone()
            disk_guard.check(
                "exact-join-candidates",
                max(64, int(estimated_posting_matches[0] or 0) * 128),
            )
        connection.execute(
            """INSERT INTO candidate_bounds (target_id, narrative_id, maximum_overlap)
                SELECT prefixes.target_id, postings.narrative_id,
                       MAX(
                           MIN(prefixes.token_position, postings.token_position) + 1
                           + MIN(
                               length(target.shingles) / 8 - prefixes.token_position - 1,
                               length(prior.shingles) / 8 - postings.token_position - 1
                           )
                       )
                FROM target_prefix AS prefixes
                JOIN narratives AS target ON target.id = prefixes.target_id
                JOIN shingle_postings AS postings INDEXED BY shingle_postings_by_rank_split
                  ON postings.token_rank = prefixes.token_rank
                JOIN narratives AS prior ON prior.id = postings.narrative_id
                WHERE ((target.split = 'validation' AND prior.split = 'train')
                       OR (target.split = 'test' AND prior.split IN ('train', 'validation')))
                  AND postings.split IN ('train', 'validation')
                  AND length(prior.shingles) / 8 >=
                      (? * (length(target.shingles) / 8) + ? - 1) / ?
                  AND length(prior.shingles) / 8 <=
                      (? * (length(target.shingles) / 8)) / ?
                  AND prior.report_id <> target.report_id
                  AND prior.exact_sha256 <> target.exact_sha256
                GROUP BY prefixes.target_id, postings.narrative_id""",
            (
                threshold_numerator,
                threshold_denominator,
                threshold_denominator,
                threshold_denominator,
                threshold_numerator,
            ),
        )
        if disk_guard is not None:
            candidate_storage = connection.execute(
                """SELECT COUNT(*), COALESCE(SUM(
                       length(CAST(prior.report_id AS BLOB))
                       + length(CAST(prior.evidence_id AS BLOB))
                       + length(CAST(prior.split AS BLOB))
                       + length(CAST(target.report_id AS BLOB))
                       + length(CAST(target.evidence_id AS BLOB))
                       + length(CAST(target.split AS BLOB))
                   ), 0)
                   FROM candidate_bounds AS candidates
                   JOIN narratives AS prior ON prior.id = candidates.narrative_id
                   JOIN narratives AS target ON target.id = candidates.target_id"""
            ).fetchone()
            candidate_count = int(candidate_storage[0] or 0)
            candidate_key_bytes = int(candidate_storage[1] or 0)
            disk_guard.check(
                "exact-join-pairs",
                max(64, candidate_count * 512 + candidate_key_bytes * 2),
            )
        connection.execute(
            """WITH viable_candidates AS (
                   SELECT candidates.target_id, candidates.narrative_id
                   FROM candidate_bounds AS candidates
                   JOIN narratives AS target ON target.id = candidates.target_id
                   JOIN narratives AS prior ON prior.id = candidates.narrative_id
                   WHERE candidates.maximum_overlap * ? >=
                         ? * (length(target.shingles) / 8 + length(prior.shingles) / 8)
               ),
               overlaps AS (
                   SELECT candidates.narrative_id AS prior_id,
                          candidates.target_id,
                          COUNT(*) AS overlap_size
                   FROM viable_candidates AS candidates
                   JOIN shingle_postings AS prior_postings
                     ON prior_postings.narrative_id = candidates.narrative_id
                   JOIN target_shingles AS target_tokens
                     ON target_tokens.target_id = candidates.target_id
                    AND target_tokens.shingle_hash = prior_postings.shingle_hash
                   GROUP BY candidates.narrative_id, candidates.target_id
               )
               INSERT OR IGNORE INTO near_pairs (
                   left_id, right_id, left_report_id, left_evidence_id, left_split,
                   right_report_id, right_evidence_id, right_split, similarity
               )
               SELECT overlaps.prior_id, overlaps.target_id,
                      prior.report_id, prior.evidence_id, prior.split,
                      target.report_id, target.evidence_id, target.split,
                      ROUND(
                          1.0 * overlaps.overlap_size
                          / (length(prior.shingles) / 8
                             + length(target.shingles) / 8 - overlaps.overlap_size),
                          6
                      )
               FROM overlaps
               JOIN narratives AS prior ON prior.id = overlaps.prior_id
               JOIN narratives AS target ON target.id = overlaps.target_id
               WHERE overlaps.overlap_size * ? >=
                     ? * (length(target.shingles) / 8 + length(prior.shingles) / 8)""",
            (
                threshold_scale,
                threshold_numerator,
                threshold_scale,
                threshold_numerator,
            ),
        )
        connection.commit()
        if disk_guard is not None:
            disk_guard.complete("exact-join-batches")
    connection.commit()
    count_row = connection.execute("SELECT COUNT(*) FROM near_pairs").fetchone()
    connection.execute("DROP TABLE shingle_order")
    connection.commit()
    return int(count_row[0] or 0)


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
            shingles BLOB NOT NULL
        );
        CREATE INDEX narratives_exact_members
            ON narratives(exact_sha256, report_id, evidence_id, received_date, split);
        CREATE INDEX narratives_report ON narratives(report_id, split);
        CREATE TABLE near_pairs (
            left_id INTEGER NOT NULL REFERENCES narratives(id),
            right_id INTEGER NOT NULL REFERENCES narratives(id),
            left_report_id TEXT NOT NULL,
            left_evidence_id TEXT NOT NULL,
            left_split TEXT NOT NULL,
            right_report_id TEXT NOT NULL,
            right_evidence_id TEXT NOT NULL,
            right_split TEXT NOT NULL,
            similarity REAL NOT NULL,
            PRIMARY KEY (left_id, right_id)
        );
        CREATE INDEX near_pairs_output_order
            ON near_pairs(
                left_report_id, left_evidence_id, right_report_id, right_evidence_id,
                left_split, right_split, similarity
            );
        """
    )


def _flush_index_batch(
    connection: sqlite3.Connection,
    narratives: list[tuple[int, str, str, str | None, str | None, str, bytes]],
    disk_guard: _DiskSpaceGuard | None = None,
) -> None:
    if not narratives:
        return
    if disk_guard is not None:
        anticipated_write_bytes = max(
            1024 * 1024,
            sum(2_048 + len(row[6]) * 4 for row in narratives),
        )
        disk_guard.check("corpus-index", anticipated_write_bytes)
    connection.executemany(
        """INSERT INTO narratives
           (id, report_id, evidence_id, received_date, split, exact_sha256, shingles)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        narratives,
    )
    narratives.clear()
    if disk_guard is not None:
        connection.commit()
        disk_guard.complete("corpus-index")


def _index_corpus(
    connection: sqlite3.Connection,
    corpus_path: Path,
    snapshot_id: str,
    cleaning_version: str,
    disk_guard: _DiskSpaceGuard | None = None,
) -> tuple[int, int]:
    narratives_scanned = 0
    missing_date_narratives = 0
    narrative_batch: list[tuple[int, str, str, str | None, str | None, str, bytes]] = []

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
            packed_shingles = _pack_shingles(shingles)
            if split is None:
                missing_date_narratives += 1

            narrative_id = narratives_scanned + 1
            narrative_batch.append(
                (
                    narrative_id,
                    document.report_id,
                    narrative.evidence_id,
                    document.date_received.isoformat() if document.date_received else None,
                    split,
                    exact_sha256,
                    packed_shingles,
                ),
            )
            narratives_scanned += 1
            if len(narrative_batch) >= _INDEX_BATCH_SIZE:
                _flush_index_batch(connection, narrative_batch, disk_guard)

    _flush_index_batch(connection, narrative_batch, disk_guard)
    connection.commit()
    return narratives_scanned, missing_date_narratives


def _write_exact_outputs(
    connection: sqlite3.Connection,
    staging: Path,
    disk_guard: _DiskSpaceGuard | None = None,
) -> tuple[int, int]:
    exact_groups_path = staging / "exact_groups.jsonl"
    exact_members_path = staging / "exact_members.jsonl"
    group_count = 0
    cross_split_count = 0
    duplicate_groups = connection.execute(
        """SELECT exact_sha256, COUNT(*), COUNT(DISTINCT report_id)
           FROM narratives GROUP BY exact_sha256
           HAVING COUNT(DISTINCT report_id) > 1 ORDER BY exact_sha256"""
    )
    if disk_guard is not None:
        disk_guard.check("exact-groups-output", 1024 * 1024)
    with exact_groups_path.open("w", encoding="utf-8", newline="\n") as groups_file:
        for exact_sha256, narrative_count, report_count in duplicate_groups:
            if disk_guard is not None and group_count and group_count % 1_000 == 0:
                groups_file.flush()
                disk_guard.check("exact-groups-output", 1024 * 1024)
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
        groups_file.flush()
    if disk_guard is not None:
        disk_guard.complete("exact-groups-output")

    member_count = 0
    if disk_guard is not None:
        disk_guard.check("exact-members-output", 1024 * 1024)
    with exact_members_path.open("w", encoding="utf-8", newline="\n") as members_file:
        members = connection.execute(
            """SELECT n.exact_sha256, n.report_id, n.evidence_id, n.received_date, n.split
               FROM narratives AS n
               WHERE n.exact_sha256 IN (
                   SELECT exact_sha256 FROM narratives GROUP BY exact_sha256
                   HAVING COUNT(DISTINCT report_id) > 1
               )
               ORDER BY n.exact_sha256, n.report_id, n.evidence_id,
                        n.received_date, n.split"""
        )
        for exact_sha256, report_id, evidence_id, received_date, split in members:
            if disk_guard is not None and member_count and member_count % 1_000 == 0:
                members_file.flush()
                disk_guard.check("exact-members-output", 1024 * 1024)
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
            member_count += 1
        members_file.flush()
    if disk_guard is not None:
        disk_guard.complete("exact-members-output")
    return group_count, cross_split_count


def _write_report_key_overlaps(
    connection: sqlite3.Connection,
    path: Path,
    disk_guard: _DiskSpaceGuard | None = None,
) -> int:
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
    if disk_guard is not None:
        disk_guard.check("report-key-output", 1024 * 1024)
    output_count = 0
    with path.open("w", encoding="utf-8", newline="\n") as output:
        report_id: str | None = None
        split_counts: dict[str, int] = {}

        def flush() -> None:
            nonlocal output_count
            if report_id is None:
                return
            if disk_guard is not None and output_count and output_count % 1_000 == 0:
                output.flush()
                disk_guard.check("report-key-output", 1024 * 1024)
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
            output_count += 1

        for current_id, split, count in rows:
            current_id = str(current_id)
            if report_id is not None and current_id != report_id:
                flush()
                split_counts = {}
            report_id = current_id
            split_counts[str(split) if split is not None else "undated"] = int(count)
        flush()
        output.flush()
    if disk_guard is not None:
        disk_guard.complete("report-key-output")
    return group_count


def _write_near_pairs(
    connection: sqlite3.Connection,
    path: Path,
    disk_guard: _DiskSpaceGuard | None = None,
) -> int:
    pair_count = 0
    if disk_guard is not None:
        disk_guard.check("near-pair-output", 1024 * 1024)
    rows = connection.execute(
        """SELECT left_report_id, left_evidence_id, left_split,
                  right_report_id, right_evidence_id, right_split, similarity
           FROM near_pairs
           ORDER BY left_report_id, left_evidence_id, right_report_id, right_evidence_id,
                    left_split, right_split, similarity"""
    )
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
            if disk_guard is not None and pair_count and pair_count % 1_000 == 0:
                output.flush()
                disk_guard.check("near-pair-output", 1024 * 1024)
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
        output.flush()
    if disk_guard is not None:
        disk_guard.complete("near-pair-output")
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
            or existing.algorithm_version != DUPLICATE_ALGORITHM_VERSION
            or existing.snapshot_id != snapshot_id
            or existing.corpus_sha256 != corpus_manifest.output_sha256
            or existing.split_version != TEMPORAL_SPLIT_VERSION
            or existing.cleaning_version != corpus_manifest.cleaning_version
            or existing.near_candidate_strategy != _NEAR_STRATEGY
            or existing.near_similarity_threshold != _NEAR_THRESHOLD
            or existing.near_candidate_overflow_documents != 0
            or not existing.near_candidate_search_is_exhaustive
        ):
            raise ValueError("existing duplicate audit has different inputs or configuration")
        return existing

    disk_guard = _DiskSpaceGuard(root)
    disk_guard.complete("preflight")
    disk_guard.check("preflight", 1024 * 1024)
    output_directory.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(
        prefix=".duplicate-audit-", dir=output_directory.parent
    ) as temporary_directory:
        staging = Path(temporary_directory)
        disk_guard.check("corpus-index", 1024 * 1024)
        connection = sqlite3.connect(staging / "index.sqlite")
        try:
            _index_schema(connection)
            narratives_scanned, missing_date_narratives = _index_corpus(
                connection,
                corpus_path,
                snapshot_id,
                corpus_manifest.cleaning_version,
                disk_guard,
            )
            if narratives_scanned != corpus_manifest.narratives_written:
                raise ValueError("corpus narrative count does not match its manifest")
            disk_guard.complete("corpus-index")
            exact_pair_count = _run_exact_similarity_join(connection, disk_guard)
            disk_guard.complete("exact-join")
            exact_groups, exact_cross_split_groups = _write_exact_outputs(
                connection, staging, disk_guard
            )
            report_key_cross_split_groups = _write_report_key_overlaps(
                connection, staging / "report_key_overlaps.jsonl", disk_guard
            )
            near_cross_split_pairs = _write_near_pairs(
                connection, staging / "near_pairs.jsonl", disk_guard
            )
            if near_cross_split_pairs != exact_pair_count:
                raise RuntimeError("exact join pair count does not match the published artifact")
        finally:
            connection.close()

        (staging / "index.sqlite").unlink()
        disk_guard.check("manifest", 1024 * 1024)
        manifest = DuplicateAuditManifest(
            audit_version=DUPLICATE_AUDIT_VERSION,
            algorithm_version=DUPLICATE_ALGORITHM_VERSION,
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
            near_candidate_overflow_documents=0,
            near_candidate_strategy=_NEAR_STRATEGY,
            near_similarity_threshold=_NEAR_THRESHOLD,
            near_candidate_search_is_exhaustive=True,
            exact_groups_path=str(output_directory / "exact_groups.jsonl"),
            exact_members_path=str(output_directory / "exact_members.jsonl"),
            report_key_overlaps_path=str(output_directory / "report_key_overlaps.jsonl"),
            near_pairs_path=str(output_directory / "near_pairs.jsonl"),
            artifact_sha256=_artifact_hashes(staging),
            completed_at=datetime.now(UTC),
        )
        write_json_model(staging / "manifest.json", manifest)
        disk_guard.complete("manifest")
        disk_guard.check("publish", 1024 * 1024)
        os.replace(staging, output_directory)
        fsync_directory(output_directory.parent)
        disk_guard.complete("publish")
        return manifest
