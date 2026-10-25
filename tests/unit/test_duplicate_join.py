import hashlib
import sqlite3
import struct
from dataclasses import dataclass
from datetime import date

import maude.features.duplicates as duplicates


@dataclass(frozen=True)
class _Case:
    evidence_id: str
    report_id: str
    received: date | None
    text: str
    shingles: tuple[int, ...]


def _case(
    evidence_id: str,
    report_id: str,
    received: date | None,
    text: str,
    shingles: range | tuple[int, ...],
) -> _Case:
    return _Case(evidence_id, report_id, received, text, tuple(shingles))


def _normalized(text: str) -> str:
    return " ".join(text.casefold().split())


def _split(received: date | None) -> str | None:
    if received is None:
        return None
    if received <= date(2024, 12, 31):
        return "train"
    if received <= date(2025, 12, 31):
        return "validation"
    return "test"


def _brute_force_pairs(cases: tuple[_Case, ...]) -> set[tuple[str, str, float]]:
    prior_splits = {"validation": {"train"}, "test": {"train", "validation"}}
    pairs: set[tuple[str, str, float]] = set()
    for prior in cases:
        prior_split = _split(prior.received)
        if prior_split not in {"train", "validation"}:
            continue
        for later in cases:
            later_split = _split(later.received)
            if prior_split not in prior_splits.get(later_split or "", set()):
                continue
            if prior.report_id == later.report_id:
                continue
            if _normalized(prior.text) == _normalized(later.text):
                continue
            left = set(prior.shingles)
            right = set(later.shingles)
            union_size = len(left | right)
            similarity = len(left & right) / union_size if union_size else 0.0
            if similarity >= 0.8:
                pairs.add((prior.evidence_id, later.evidence_id, round(similarity, 6)))
    return pairs


def _indexed_pairs(
    cases: tuple[_Case, ...],
) -> set[tuple[str, str, float]]:
    connection = sqlite3.connect(":memory:")
    try:
        duplicates._index_schema(connection)
        narratives = []
        for narrative_id, case in enumerate(cases, start=1):
            normalized = _normalized(case.text)
            shingles = tuple(sorted(set(case.shingles)))
            packed = struct.pack(f">{len(shingles)}Q", *shingles) if shingles else b""
            narratives.append(
                (
                    narrative_id,
                    case.report_id,
                    case.evidence_id,
                    case.received.isoformat() if case.received else None,
                    _split(case.received),
                    hashlib.sha256(normalized.encode("utf-8")).hexdigest(),
                    packed,
                )
            )
        connection.executemany(
            """INSERT INTO narratives
               (id, report_id, evidence_id, received_date, split, exact_sha256,
                shingles)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            narratives,
        )
        run_join = getattr(duplicates, "_run_exact_similarity_join", None)
        assert callable(run_join), "the exact SQLite join helper must be implemented"
        run_join(connection)
        rows = connection.execute(
            """SELECT left_n.evidence_id, right_n.evidence_id, pairs.similarity
               FROM near_pairs AS pairs
               JOIN narratives AS left_n ON left_n.id = pairs.left_id
               JOIN narratives AS right_n ON right_n.id = pairs.right_id
               ORDER BY left_n.evidence_id, right_n.evidence_id"""
        )
        return {
            (str(left_id), str(right_id), round(float(score), 6))
            for left_id, right_id, score in rows
        }
    finally:
        connection.close()


def test_rank_split_lookup_index_excludes_future_and_undated_postings() -> None:
    connection = sqlite3.connect(":memory:")
    try:
        duplicates._index_schema(connection)
        narratives = []
        for narrative_id, case in enumerate(_fixture_cases(), start=1):
            normalized = _normalized(case.text)
            shingles = tuple(sorted(set(case.shingles)))
            packed = struct.pack(f">{len(shingles)}Q", *shingles) if shingles else b""
            narratives.append(
                (
                    narrative_id,
                    case.report_id,
                    case.evidence_id,
                    case.received.isoformat() if case.received else None,
                    _split(case.received),
                    hashlib.sha256(normalized.encode("utf-8")).hexdigest(),
                    packed,
                )
            )
        connection.executemany(
            """INSERT INTO narratives
               (id, report_id, evidence_id, received_date, split, exact_sha256, shingles)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            narratives,
        )

        duplicates._build_exact_join_index(connection)

        future_postings = connection.execute(
            """SELECT COUNT(*) FROM shingle_postings
               WHERE split NOT IN ('train', 'validation') OR split IS NULL"""
        ).fetchone()[0]
        assert future_postings == 0

        index_sql = connection.execute(
            "SELECT sql FROM sqlite_master WHERE name = 'shingle_postings_by_rank_split'"
        ).fetchone()[0]
        assert "WHERE split IN ('train', 'validation')" in index_sql
    finally:
        connection.close()


def _fixture_cases() -> tuple[_Case, ...]:
    boundary = date(2025, 6, 1)
    training = date(2024, 6, 1)
    test_date = date(2026, 6, 1)
    cases = [
        # At exactly 0.8, the later set is smaller than its prior training set.
        _case(
            "prior-large",
            "prior-large",
            training,
            "prior larger set",
            (*range(1, 41), *range(10_000, 10_176)),
        ),
        _case(
            "later-small",
            "later-small",
            boundary,
            "later smaller set",
            (*range(10_000, 10_176), *range(20_000, 20_004)),
        ),
        # At exactly 0.8, the later set is larger than its prior training set.
        _case(
            "prior-small",
            "prior-small",
            training,
            "prior smaller set",
            (*range(30_000, 30_176), *range(40_000, 40_004)),
        ),
        _case(
            "later-large",
            "later-large",
            boundary,
            "later larger set",
            (*range(25_000, 25_040), *range(30_000, 30_176)),
        ),
        # Boundary controls: 159/201 < 0.8 and 161/199 > 0.8.
        _case(
            "below-train",
            "below-train",
            training,
            "below threshold prior",
            (*range(50_000, 50_159), *range(60_000, 60_021)),
        ),
        _case(
            "below-validation",
            "below-validation",
            boundary,
            "below threshold later",
            (*range(50_000, 50_159), *range(61_000, 61_021)),
        ),
        _case(
            "above-train",
            "above-train",
            training,
            "above threshold prior",
            (*range(70_000, 70_161), *range(71_000, 71_019)),
        ),
        _case(
            "above-validation",
            "above-validation",
            boundary,
            "above threshold later",
            (*range(70_000, 70_161), *range(72_000, 72_019)),
        ),
        # Future-only hashes sort before shared prior hashes and must still reach
        # a matching shared hash inside the target's PPJoin prefix.
        _case("novel-prior", "novel-prior", training, "novel prior", range(1, 101)),
        _case(
            "novel-test",
            "novel-test",
            test_date,
            "novel future",
            (*range(1, 90), *range(101, 112)),
        ),
        # Test-period records compare to both prior temporal splits.
        _case("test-train", "test-train", training, "test relation train", range(80_000, 80_010)),
        _case(
            "test-validation",
            "test-validation",
            boundary,
            "test relation validation",
            range(80_000, 80_010),
        ),
        _case("test-later", "test-later", test_date, "test relation later", range(80_000, 80_010)),
        # A high-similarity cross-split pair for one report is excluded.
        _case(
            "same-report-train", "same-report", training, "same report prior", range(90_000, 90_010)
        ),
        _case(
            "same-report-validation",
            "same-report",
            boundary,
            "same report later",
            range(90_000, 90_010),
        ),
        # Equal normalized text is reported by the exact audit, not as a near pair.
        _case(
            "normalized-train",
            "normalized-train",
            training,
            "same normalized narrative text",
            range(100_000, 100_010),
        ),
        _case(
            "normalized-validation",
            "normalized-validation",
            boundary,
            " SAME   normalized narrative text ",
            range(100_000, 100_010),
        ),
        # Same-split and undated records are not members of the temporal join.
        _case(
            "same-split-one",
            "same-split-one",
            training,
            "same split narrative one",
            range(110_000, 110_010),
        ),
        _case(
            "same-split-two",
            "same-split-two",
            training,
            "same split narrative two",
            range(110_000, 110_010),
        ),
        _case("undated", "undated", None, "undated test relation", range(80_000, 80_010)),
    ]
    return tuple(cases)


def test_sqlite_join_matches_brute_force_oracle_at_boundary_and_by_temporal_direction() -> None:
    cases = _fixture_cases()
    expected = _brute_force_pairs(cases)

    actual = _indexed_pairs(cases)

    assert expected == actual
    assert ("prior-large", "later-small", 0.8) in actual
    assert ("prior-small", "later-large", 0.8) in actual
    assert ("below-train", "below-validation", 0.8) not in actual
    assert ("above-train", "above-validation", round(161 / 199, 6)) in actual
    assert ("novel-prior", "novel-test", round(89 / 111, 6)) in actual
    assert ("test-train", "test-later", 1.0) in actual
    assert ("test-validation", "test-later", 1.0) in actual
    assert not any("same-report" in left or "same-report" in right for left, right, _ in actual)
    assert ("normalized-train", "normalized-validation", 1.0) not in actual
    assert not any("same-split" in left or "same-split" in right for left, right, _ in actual)
    assert not any("undated" in left or "undated" in right for left, right, _ in actual)


def test_sqlite_join_is_independent_of_corpus_order() -> None:
    cases = _fixture_cases()

    forward = _indexed_pairs(cases)
    reverse = _indexed_pairs(tuple(reversed(cases)))

    assert forward == reverse


def test_sqlite_join_fetches_targets_in_bounded_batches(monkeypatch) -> None:
    cases = _fixture_cases()
    connection = sqlite3.connect(":memory:")
    statements: list[str] = []
    try:
        duplicates._index_schema(connection)
        narratives = []
        for narrative_id, case in enumerate(cases, start=1):
            normalized = _normalized(case.text)
            shingles = tuple(sorted(set(case.shingles)))
            packed = struct.pack(f">{len(shingles)}Q", *shingles) if shingles else b""
            narratives.append(
                (
                    narrative_id,
                    case.report_id,
                    case.evidence_id,
                    case.received.isoformat() if case.received else None,
                    _split(case.received),
                    hashlib.sha256(normalized.encode("utf-8")).hexdigest(),
                    packed,
                )
            )
        connection.executemany(
            """INSERT INTO narratives
               (id, report_id, evidence_id, received_date, split, exact_sha256, shingles)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            narratives,
        )
        monkeypatch.setattr(duplicates, "_JOIN_TARGET_BATCH_SIZE", 2, raising=False)
        connection.set_trace_callback(statements.append)

        duplicates._run_exact_similarity_join(connection)
        actual_rows = connection.execute(
            """SELECT left_n.evidence_id, right_n.evidence_id, pairs.similarity
               FROM near_pairs AS pairs
               JOIN narratives AS left_n ON left_n.id = pairs.left_id
               JOIN narratives AS right_n ON right_n.id = pairs.right_id"""
        ).fetchall()
        actual_pairs = {
            (str(left_id), str(right_id), round(float(score), 6))
            for left_id, right_id, score in actual_rows
        }
    finally:
        connection.close()

    target_count = sum(_split(case.received) in {"validation", "test"} for case in cases)
    target_batch_queries = [
        statement
        for statement in statements
        if "SELECT id, shingles FROM narratives" in statement and "WHERE id >" in statement
    ]
    assert len(target_batch_queries) == (target_count + 1) // 2 + 1
    assert actual_pairs == _brute_force_pairs(cases)
