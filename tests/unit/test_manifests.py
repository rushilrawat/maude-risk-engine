from datetime import UTC, date, datetime
from pathlib import Path
from uuid import uuid4

import maude.ingestion.manifests as manifests
from maude.domain.enums import RunStatus
from maude.domain.models import DuplicateAuditManifest, SnapshotResult
from maude.ingestion.manifests import load_manifest, write_json_model, write_manifest


def _snapshot() -> SnapshotResult:
    return SnapshotResult(
        run_id=uuid4(),
        snapshot_id="fixture-2025-06",
        started_at=datetime.now(UTC),
        finished_at=None,
        status=RunStatus.RUNNING,
        tables=[],
        quality=[],
        promoted_path=None,
        parser_version="test-parser",
    )


def _duplicate_audit_manifest_values() -> dict[str, object]:
    return {
        "audit_version": "narrative-duplicate-audit-v1",
        "snapshot_id": "fixture-2026",
        "cleaning_version": "nfc-whitespace-v1",
        "corpus_sha256": "a" * 64,
        "output_directory": "/data/audits/fixture-2026/narrative-duplicate-audit-v1",
        "split_version": "received-calendar-year-2024-2025-2026plus-v1",
        "training_end": date(2024, 12, 31),
        "validation_start": date(2025, 1, 1),
        "validation_end": date(2025, 12, 31),
        "test_start": date(2026, 1, 1),
        "narratives_scanned": 1,
        "missing_date_narratives": 0,
        "exact_duplicate_groups": 0,
        "exact_cross_split_groups": 0,
        "report_key_cross_split_groups": 0,
        "near_cross_split_pairs": 0,
        "near_candidate_overflow_documents": 0,
        "near_candidate_strategy": "bottom-16-word-trigram-overlap; exact-Jaccard-verification",
        "near_similarity_threshold": 0.8,
        "near_candidate_search_is_exhaustive": False,
        "exact_groups_path": "/data/audits/exact_groups.jsonl",
        "exact_members_path": "/data/audits/exact_members.jsonl",
        "report_key_overlaps_path": "/data/audits/report_key_overlaps.jsonl",
        "near_pairs_path": "/data/audits/near_pairs.jsonl",
        "artifact_sha256": {
            "exact_groups.jsonl": "b" * 64,
            "exact_members.jsonl": "c" * 64,
            "report_key_overlaps.jsonl": "d" * 64,
            "near_pairs.jsonl": "e" * 64,
        },
        "completed_at": datetime(2026, 10, 20, tzinfo=UTC),
    }


def test_manifest_replace_fsyncs_its_parent_directory(tmp_path: Path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    synced: list[Path] = []
    monkeypatch.setattr(manifests, "fsync_directory", synced.append)
    path = tmp_path / "manifests" / "snapshot.json"

    write_manifest(path, _snapshot())

    assert synced == [path.parent]
    assert load_manifest(path).snapshot_id == "fixture-2025-06"


def test_json_model_writer_preserves_strict_model_content(tmp_path: Path) -> None:
    path = tmp_path / "refresh-runs" / "run.json"
    snapshot = _snapshot()

    write_json_model(path, snapshot)

    restored = SnapshotResult.model_validate_json(path.read_text(encoding="utf-8"))
    assert restored.model_dump(mode="json") == snapshot.model_dump(mode="json")


def test_duplicate_audit_manifest_defaults_algorithm_for_legacy_v1() -> None:
    manifest = DuplicateAuditManifest.model_validate(_duplicate_audit_manifest_values())

    assert manifest.algorithm_version == "legacy-v1"


def test_duplicate_audit_manifest_preserves_explicit_algorithm_version() -> None:
    values = _duplicate_audit_manifest_values()
    values["algorithm_version"] = "sqlite-ppjoin-jaccard-v1"

    manifest = DuplicateAuditManifest.model_validate(values)

    assert manifest.algorithm_version == "sqlite-ppjoin-jaccard-v1"
