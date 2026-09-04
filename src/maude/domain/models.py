from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator

from maude.domain.enums import QualityLevel, RunStatus, TableKind


class ContractModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class SourceIdentity(ContractModel):
    path: str
    filename: str
    sha256: str
    byte_size: int = Field(ge=0)
    archive_member: str | None
    encoding: str


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
