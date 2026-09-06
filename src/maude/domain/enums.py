from enum import StrEnum


class TableKind(StrEnum):
    MASTER = "master"
    DEVICE = "device"
    PATIENT = "patient"
    NARRATIVE = "narrative"


class RunStatus(StrEnum):
    RUNNING = "running"
    FAILED = "failed"
    VALIDATED = "validated"
    PROMOTED = "promoted"


class QualityLevel(StrEnum):
    BLOCKING = "blocking"
    WARNING = "warning"
