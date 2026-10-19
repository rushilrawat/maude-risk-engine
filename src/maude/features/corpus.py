"""Build immutable report-level modeling corpora from promoted snapshots."""

import hashlib
import os
import re
import tempfile
from datetime import UTC, datetime
from pathlib import Path

from maude.domain.enums import RunStatus
from maude.domain.models import ModelingCorpusManifest
from maude.features.documents import (
    NARRATIVE_CLEANING_VERSION,
    prepare_report_for_modeling,
)
from maude.ingestion.manifests import fsync_directory, load_manifest, write_json_model
from maude.storage.duckdb import iter_report_documents, open_snapshot
from maude.storage.layout import SnapshotLayout

_CORPUS_FORMAT_VERSION = "1"
_SNAPSHOT_ID = re.compile(r"[a-zA-Z0-9._-]+")


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _load_existing(output_directory: Path) -> ModelingCorpusManifest:
    manifest_path = output_directory / "manifest.json"
    manifest = ModelingCorpusManifest.model_validate_json(manifest_path.read_text(encoding="utf-8"))
    if Path(manifest.output_path) != output_directory / "documents.jsonl":
        raise ValueError(
            f"existing corpus manifest points to an unexpected output: {manifest_path}"
        )
    if _sha256_file(Path(manifest.output_path)) != manifest.output_sha256:
        raise ValueError(
            f"existing corpus output checksum does not match its manifest: {manifest_path}"
        )
    return manifest


def build_modeling_corpus(
    data_root: Path, snapshot_id: str, batch_rows: int = 10_000
) -> ModelingCorpusManifest:
    """Stream one promoted snapshot into a deterministic JSONL corpus.

    The snapshot and corpus version define an immutable output location. Repeating
    a successful build validates and returns its existing manifest.
    """
    if not _SNAPSHOT_ID.fullmatch(snapshot_id) or snapshot_id in {".", ".."}:
        raise ValueError(
            "snapshot_id must contain only letters, numbers, dots, underscores, or hyphens"
        )
    if batch_rows < 1:
        raise ValueError("batch_rows must be at least 1")

    layout = SnapshotLayout(Path(data_root), snapshot_id)
    snapshot_manifest = load_manifest(layout.manifest)
    if (
        snapshot_manifest.status is not RunStatus.PROMOTED
        or snapshot_manifest.promotion is not None
    ):
        raise ValueError(f"snapshot {snapshot_id!r} is not promoted")
    if snapshot_manifest.promoted_path is None:
        raise ValueError(f"snapshot {snapshot_id!r} has no promoted path")
    promoted_path = Path(snapshot_manifest.promoted_path)
    if promoted_path.resolve() != layout.promoted.resolve() or not promoted_path.is_dir():
        raise ValueError(
            f"snapshot {snapshot_id!r} manifest does not point to its promoted directory"
        )

    output_directory = layout.data_root / "corpora" / snapshot_id / NARRATIVE_CLEANING_VERSION
    if output_directory.exists():
        existing = _load_existing(output_directory)
        if (
            existing.snapshot_id != snapshot_id
            or existing.cleaning_version != NARRATIVE_CLEANING_VERSION
        ):
            raise ValueError(
                f"existing corpus manifest has mismatched provenance: {output_directory}"
            )
        return existing

    output_directory.parent.mkdir(parents=True, exist_ok=True)
    connection = open_snapshot(promoted_path)
    try:
        with tempfile.TemporaryDirectory(prefix="maude-corpus-") as spill_directory:
            connection.execute("SET memory_limit = '2GB'")
            connection.execute("SET threads = 1")
            connection.execute("SET preserve_insertion_order = false")
            connection.execute("SET temp_directory = ?", [spill_directory])
            with tempfile.TemporaryDirectory(
                prefix=".corpus-", dir=output_directory.parent
            ) as temporary_directory:
                staging_path = Path(temporary_directory)
                temporary_output = staging_path / "documents.jsonl"
                reports_seen = 0
                documents_written = 0
                narratives_written = 0
                blank_narratives_omitted = 0
                digest = hashlib.sha256()
                with temporary_output.open("wb") as handle:
                    for report in iter_report_documents(connection, batch_rows=batch_rows):
                        reports_seen += 1
                        document = prepare_report_for_modeling(report)
                        blank_narratives_omitted += len(report.narratives) - (
                            0 if document is None else len(document.narratives)
                        )
                        if document is None:
                            continue
                        encoded = (document.model_dump_json() + "\n").encode("utf-8")
                        handle.write(encoded)
                        digest.update(encoded)
                        documents_written += 1
                        narratives_written += len(document.narratives)
                    handle.flush()
                    os.fsync(handle.fileno())

                final_output = output_directory / "documents.jsonl"
                final_manifest = output_directory / "manifest.json"
                manifest = ModelingCorpusManifest(
                    corpus_format_version=_CORPUS_FORMAT_VERSION,
                    cleaning_version=NARRATIVE_CLEANING_VERSION,
                    snapshot_id=snapshot_id,
                    promoted_snapshot_path=str(promoted_path),
                    output_path=str(final_output),
                    manifest_path=str(final_manifest),
                    output_sha256=digest.hexdigest(),
                    reports_seen=reports_seen,
                    documents_written=documents_written,
                    narratives_written=narratives_written,
                    blank_narratives_omitted=blank_narratives_omitted,
                    completed_at=datetime.now(UTC),
                )
                write_json_model(staging_path / "manifest.json", manifest)
                os.replace(staging_path, output_directory)
                fsync_directory(output_directory.parent)
                return manifest
    finally:
        connection.close()
