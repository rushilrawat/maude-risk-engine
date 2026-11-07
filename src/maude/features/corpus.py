"""Build immutable report-level modeling corpora from promoted snapshots."""

import hashlib
import json
import os
import re
import tempfile
from collections.abc import Sequence
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


def _sql_literal(value: str | Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


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
                    source_snapshot_ids=(snapshot_id,),
                    source_snapshot_paths=(str(promoted_path),),
                    source_corpus_sha256=((snapshot_id, digest.hexdigest()),),
                    completed_at=datetime.now(UTC),
                )
                write_json_model(staging_path / "manifest.json", manifest)
                os.replace(staging_path, output_directory)
                fsync_directory(output_directory.parent)
                return manifest
    finally:
        connection.close()


def _master_artifact(promoted_path: Path) -> Path:
    for filename in ("master.parquet", "reports.parquet"):
        artifact = promoted_path / filename
        if artifact.is_file():
            return artifact
    raise FileNotFoundError(f"promoted snapshot is missing master data: {promoted_path}")


def _validate_disjoint_report_keys(
    sources: Sequence[tuple[str, ModelingCorpusManifest]], scratch_root: Path
) -> None:
    """Reject report-key overlaps between immutable source snapshots."""
    if len(sources) < 2:
        return
    import duckdb

    scratch_root.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(
        prefix="maude-corpus-overlap-", dir=scratch_root
    ) as temporary_directory:
        connection = duckdb.connect()
        try:
            connection.execute("SET memory_limit = '2GB'")
            connection.execute("SET threads = 1")
            connection.execute(f"SET temp_directory = {_sql_literal(temporary_directory)}")
            for left_index, (left_id, left_manifest) in enumerate(sources):
                if left_manifest.promoted_snapshot_path is None:
                    raise ValueError(f"source snapshot {left_id!r} has no promoted path")
                left_path = _master_artifact(Path(left_manifest.promoted_snapshot_path))
                for right_id, right_manifest in sources[left_index + 1 :]:
                    if right_manifest.promoted_snapshot_path is None:
                        raise ValueError(f"source snapshot {right_id!r} has no promoted path")
                    right_path = _master_artifact(Path(right_manifest.promoted_snapshot_path))
                    query = (
                        "SELECT lhs.mdr_report_key "
                        f"FROM read_parquet({_sql_literal(left_path)}) AS lhs "
                        f"INNER JOIN read_parquet({_sql_literal(right_path)}) AS rhs "
                        "ON lhs.mdr_report_key = rhs.mdr_report_key LIMIT 1"
                    )
                    overlap = connection.execute(query).fetchone()
                    if overlap is not None:
                        raise ValueError(
                            f"report key {overlap[0]!r} appears in source snapshots "
                            f"{left_id!r} and {right_id!r}; research corpus was not built"
                        )
        finally:
            connection.close()


def build_research_corpus(
    data_root: Path,
    snapshot_ids: Sequence[str],
    batch_rows: int = 10_000,
) -> ModelingCorpusManifest:
    """Combine disjoint promoted snapshots into one immutable temporal corpus."""
    source_ids = tuple(sorted(snapshot_ids))
    if len(source_ids) < 2:
        raise ValueError("research corpus requires at least two source snapshots")
    if len(set(source_ids)) != len(source_ids):
        raise ValueError("source snapshot IDs must be unique")
    if batch_rows < 1:
        raise ValueError("batch_rows must be at least 1")

    sources = tuple(
        (snapshot_id, build_modeling_corpus(data_root, snapshot_id, batch_rows=batch_rows))
        for snapshot_id in source_ids
    )
    if any(
        manifest.cleaning_version != NARRATIVE_CLEANING_VERSION
        or manifest.corpus_format_version != _CORPUS_FORMAT_VERSION
        for _, manifest in sources
    ):
        raise ValueError("source corpora use incompatible cleaning or document versions")
    source_corpus_sha256 = tuple(
        (snapshot_id, manifest.output_sha256) for snapshot_id, manifest in sources
    )
    identity = {
        "corpus_format_version": _CORPUS_FORMAT_VERSION,
        "cleaning_version": NARRATIVE_CLEANING_VERSION,
        "source_corpora": source_corpus_sha256,
    }
    encoded_identity = json.dumps(identity, sort_keys=True, separators=(",", ":")).encode()
    research_id = f"fda-research-{hashlib.sha256(encoded_identity).hexdigest()[:16]}"

    root = Path(data_root)
    output_directory = root / "corpora" / research_id / NARRATIVE_CLEANING_VERSION
    if output_directory.exists():
        existing = _load_existing(output_directory)
        if (
            existing.snapshot_id != research_id
            or existing.source_snapshot_ids != source_ids
            or existing.source_corpus_sha256 != source_corpus_sha256
        ):
            raise ValueError(f"existing research corpus has mismatched inputs: {output_directory}")
        return existing

    _validate_disjoint_report_keys(sources, root / "staging" / "corpus-validation")
    output_directory.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(
        prefix=".corpus-", dir=output_directory.parent
    ) as temporary_directory:
        staging_path = Path(temporary_directory)
        temporary_output = staging_path / "documents.jsonl"
        digest = hashlib.sha256()
        with temporary_output.open("wb") as handle:
            for _, source in sources:
                with Path(source.output_path).open("rb") as source_handle:
                    while block := source_handle.read(1024 * 1024):
                        handle.write(block)
                        digest.update(block)
            handle.flush()
            os.fsync(handle.fileno())

        final_output = output_directory / "documents.jsonl"
        final_manifest = output_directory / "manifest.json"
        manifest = ModelingCorpusManifest(
            corpus_format_version=_CORPUS_FORMAT_VERSION,
            cleaning_version=NARRATIVE_CLEANING_VERSION,
            snapshot_id=research_id,
            promoted_snapshot_path=None,
            output_path=str(final_output),
            manifest_path=str(final_manifest),
            output_sha256=digest.hexdigest(),
            reports_seen=sum(source.reports_seen for _, source in sources),
            documents_written=sum(source.documents_written for _, source in sources),
            narratives_written=sum(source.narratives_written for _, source in sources),
            blank_narratives_omitted=sum(
                source.blank_narratives_omitted for _, source in sources
            ),
            source_snapshot_ids=source_ids,
            source_snapshot_paths=tuple(
                source.promoted_snapshot_path for _, source in sources
                if source.promoted_snapshot_path is not None
            ),
            source_corpus_sha256=source_corpus_sha256,
            completed_at=datetime.now(UTC),
        )
        write_json_model(staging_path / "manifest.json", manifest)
        os.replace(staging_path, output_directory)
        fsync_directory(output_directory.parent)
        return manifest
