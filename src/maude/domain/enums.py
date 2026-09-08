from enum import StrEnum


class TableKind(StrEnum):
    MASTER = "master"
    DEVICE = "device"
    PATIENT = "patient"
    NARRATIVE = "narrative"


class SourceRole(StrEnum):
    BASE = "base"
    ADD = "add"
    CHANGE = "change"


class RefreshOutcome(StrEnum):
    RUNNING = "running"
    PROMOTED = "promoted"
    UNCHANGED = "unchanged"
    FAILED = "failed"


class RunStatus(StrEnum):
    RUNNING = "running"
    FAILED = "failed"
    VALIDATED = "validated"
    PROMOTED = "promoted"


class QualityLevel(StrEnum):
    BLOCKING = "blocking"
    WARNING = "warning"
