from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator

from maude.domain.enums import QualityLevel, RefreshOutcome, RunStatus, SourceRole, TableKind


class ContractModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class SourceIdentity(ContractModel):
    path: str
    filename: str
    sha256: str
    byte_size: int = Field(ge=0)
    archive_member: str | None
    encoding: str
    source_role: SourceRole | None = None


class ParseStats(ContractModel):
    rows_seen: int = Field(ge=0)
    rows_accepted: int = Field(ge=0)
    rows_rejected: int = Field(ge=0)
    columns: tuple[str, ...]


class QualityResult(ContractModel):
    check: str
    level: QualityLevel
    passed: bool
    message: str
    metrics: Mapping[str, int | float | str | None] = Field(default_factory=dict)


class TableResult(ContractModel):
    table: TableKind
    source: SourceIdentity
    bronze_path: str
    silver_path: str
    reject_path: str
    stats: ParseStats
    quality: Sequence[QualityResult]
    sources: tuple[SourceIdentity, ...] = ()


class CatalogEvidence(ContractModel):
    catalog_url: str
    retrieved_at: datetime
    final_url: str
    etag: str | None = None
    last_modified: str | None = None
    sha256: str
    html_path: str

    @field_validator("retrieved_at")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("catalog retrieval timestamp must be timezone-aware")
        return value


class DiscoveredArtifact(ContractModel):
    table: TableKind
    role: SourceRole
    filename: str
    url: str


class DownloadedArtifact(DiscoveredArtifact):
    started_at: datetime
    completed_at: datetime
    final_url: str
    etag: str | None = None
    last_modified: str | None = None
    byte_size: int = Field(ge=0)
    sha256: str
    raw_path: str
    archive_member: str
    encoding: str
    header: tuple[str, ...]

    @field_validator("started_at", "completed_at")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("download timestamps must be timezone-aware")
        return value


class ReconciliationStats(ContractModel):
    table: TableKind
    role: SourceRole
    inserted: int = Field(ge=0)
    updated: int = Field(ge=0)
    unchanged: int = Field(ge=0)
    superseded: int = Field(ge=0)
    duplicate_rows_collapsed: int = Field(default=0, ge=0)
    conflicting_rows_quarantined: int = Field(default=0, ge=0)


class RefreshFailure(ContractModel):
    phase: str
    table: TableKind | None = None
    role: SourceRole | None = None
    url: str | None = None
    error_type: str
    message: str


class RefreshRunResult(ContractModel):
    run_id: UUID
    started_at: datetime
    finished_at: datetime | None
    outcome: RefreshOutcome
    catalog: CatalogEvidence | None = None
    artifacts: Sequence[DownloadedArtifact] = ()
    reconciliation: Sequence[ReconciliationStats] = ()
    refresh_fingerprint: str | None = None
    snapshot_id: str | None = None
    failure: RefreshFailure | None = None

    @field_validator("started_at", "finished_at")
    @classmethod
    def require_timezone(cls, value: datetime | None) -> datetime | None:
        if value is not None and value.tzinfo is None:
            raise ValueError("refresh run timestamps must be timezone-aware")
        return value


class FailureEvidence(ContractModel):
    """Structured provenance retained when a snapshot cannot be published."""

    phase: str
    table: TableKind | None = None
    source_path: str | None = None
    source: SourceIdentity | None = None
    error_type: str
    message: str


class PromotionIntent(ContractModel):
    """Durable publication state used to reconcile an interrupted promotion."""

    phase: str


class SnapshotResult(ContractModel):
    run_id: UUID
    snapshot_id: str
    started_at: datetime
    finished_at: datetime | None
    status: RunStatus
    tables: Sequence[TableResult]
    quality: Sequence[QualityResult]
    promoted_path: str | None
    parser_version: str
    inputs: Sequence[SourceIdentity] = ()
    reconciliation: Sequence[ReconciliationStats] = Field(default_factory=list)
    failure: FailureEvidence | None = None
    promotion: PromotionIntent | None = None

    @field_validator("started_at", "finished_at")
    @classmethod
    def require_timezone(cls, value: datetime | None) -> datetime | None:
        if value is not None and value.tzinfo is None:
            raise ValueError("run timestamps must be timezone-aware")
        return value


@dataclass(frozen=True)
class InspectedSource:
    path: Path
    member: str | None
    sha256: str
    byte_size: int
    sample: bytes


class BronzeResult(ContractModel):
    table: TableKind
    source: SourceIdentity
    bronze_path: str
    reject_path: str
    stats: ParseStats


class NormalizationResult(ContractModel):
    silver_path: str
    date_parse_failure_count: int = Field(ge=0)


class LocalAuditItem(ContractModel):
    table: TableKind
    archive_path: str
    archive_member: str
    sha256: str
    encoding: str
    converted_path: str | None
    conversion_row_ratio: float | None
    quality: Sequence[QualityResult]


class LocalAuditResult(ContractModel):
    source_root: str
    items: Sequence[LocalAuditItem]
    quality: Sequence[QualityResult]
    has_blocking_failure: bool


class NarrativeEvidence(ContractModel):
    """One narrative row with enough source information to cite it exactly."""

    model_config = ConfigDict(frozen=True)

    evidence_id: str
    text_type_code: str | None
    text: str
    source_filename: str
    source_line_number: int


class ModelingNarrative(ContractModel):
    """Cleaned narrative text linked back to its immutable source evidence."""

    model_config = ConfigDict(frozen=True)

    report_id: str
    dataset_snapshot_id: str
    evidence_id: str
    text_type_code: str | None
    source_filename: str
    source_line_number: int = Field(ge=1)
    source_text_sha256: str
    modeling_text: str
    cleaning_version: str


class ModelingDocument(ContractModel):
    """Report-level modeling text with independently traceable narrative segments."""

    model_config = ConfigDict(frozen=True)

    report_id: str
    dataset_snapshot_id: str
    reported_event_type: str | None
    date_received: date | None
    product_codes: tuple[str, ...]
    brand_names: tuple[str, ...]
    manufacturers: tuple[str, ...]
    narratives: tuple[ModelingNarrative, ...]
    modeling_text: str
    cleaning_version: str


class ModelingCorpusManifest(ContractModel):
    """Counts and provenance for one immutable, locally built modeling corpus."""

    corpus_format_version: str
    cleaning_version: str
    snapshot_id: str
    promoted_snapshot_path: str
    output_path: str
    manifest_path: str
    output_sha256: str
    reports_seen: int = Field(ge=0)
    documents_written: int = Field(ge=0)
    narratives_written: int = Field(ge=0)
    blank_narratives_omitted: int = Field(ge=0)
    completed_at: datetime

    @field_validator("completed_at")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("corpus completion timestamp must be timezone-aware")
        return value


class TfidfBaselineManifest(ContractModel):
    """Training-only vocabulary, temporal partitions, and checksums for TF-IDF output."""

    feature_version: str
    snapshot_id: str
    cleaning_version: str
    output_directory: str
    corpus_sha256: str
    uv_lock_sha256: str
    split_version: str | None = None
    training_end: date
    validation_start: date
    validation_end: date
    test_start: date
    fit_sample_limit: int = Field(ge=1)
    fit_sample_size: int = Field(ge=1)
    fit_sample_report_ids_sha256: str
    split_document_counts: Mapping[str, int]
    matrix_shard_counts: Mapping[str, int]
    missing_date_documents: int = Field(ge=0)
    feature_dimensions: Mapping[str, int]
    vectorizer_config: Mapping[str, object]
    artifact_sha256: Mapping[str, str]
    completed_at: datetime

    @field_validator("completed_at")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("TF-IDF completion timestamp must be timezone-aware")
        return value


class DuplicateAuditManifest(ContractModel):
    """Snapshot-bound exact and approximate cross-period narrative audit."""

    audit_version: str
    snapshot_id: str
    cleaning_version: str
    corpus_sha256: str
    output_directory: str
    split_version: str
    training_end: date
    validation_start: date
    validation_end: date
    test_start: date
    narratives_scanned: int = Field(ge=0)
    missing_date_narratives: int = Field(ge=0)
    exact_duplicate_groups: int = Field(ge=0)
    exact_cross_split_groups: int = Field(ge=0)
    report_key_cross_split_groups: int = Field(ge=0)
    near_cross_split_pairs: int = Field(ge=0)
    near_candidate_overflow_documents: int = Field(ge=0)
    near_candidate_strategy: str
    near_similarity_threshold: float = Field(ge=0, le=1)
    near_candidate_search_is_exhaustive: bool
    exact_groups_path: str
    exact_members_path: str
    report_key_overlaps_path: str
    near_pairs_path: str
    artifact_sha256: Mapping[str, str]
    completed_at: datetime

    @field_validator("completed_at")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("duplicate-audit completion timestamp must be timezone-aware")
        return value


class ReportDocument(ContractModel):
    """Immutable report metadata and provenance-preserving narrative evidence."""

    model_config = ConfigDict(frozen=True)

    report_id: str
    reported_event_type: str | None
    date_received: date | None
    product_codes: tuple[str, ...]
    brand_names: tuple[str, ...]
    manufacturers: tuple[str, ...]
    narratives: tuple[NarrativeEvidence, ...]
    dataset_snapshot_id: str
