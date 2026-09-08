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
