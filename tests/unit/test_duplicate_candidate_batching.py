from maude.features.duplicates import _candidate_records


def test_candidate_records_merges_prior_split_postings_in_id_order() -> None:
    postings = {
        "train": {b"shared": [1, 2, 4], b"other": [2, 3]},
        "validation": {b"shared": [10]},
    }
    records = {
        candidate_id: (f"report-{candidate_id}", "a" * 64, b"x")
        for candidate_id in (1, 2, 3, 4, 10)
    }

    candidates, overflow = _candidate_records(
        postings, records, (b"shared", b"other"), "validation"
    )

    assert candidates == [
        (1, "report-1", "a" * 64, b"x"),
        (2, "report-2", "a" * 64, b"x"),
        (3, "report-3", "a" * 64, b"x"),
        (4, "report-4", "a" * 64, b"x"),
    ]
    assert overflow is False


def test_candidate_records_caps_distinct_candidates_across_prior_splits(
    monkeypatch,
) -> None:
    import maude.features.duplicates as duplicates

    monkeypatch.setattr(duplicates, "_CANDIDATE_LIMIT", 2)
    postings = {
        "train": {b"shared": [1, 2], b"other": [2, 3]},
        "validation": {b"shared": [4]},
    }
    records = {
        candidate_id: (f"report-{candidate_id}", "b" * 64, b"y") for candidate_id in (1, 2, 3, 4)
    }

    candidates, overflow = _candidate_records(postings, records, (b"shared", b"other"), "test")

    assert [candidate[0] for candidate in candidates] == [1, 2]
    assert overflow is True
