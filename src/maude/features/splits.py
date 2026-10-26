"""Fixed MAUDE received-date partitions shared by feature and audit jobs."""

from datetime import date
from typing import Literal

TRAINING_END = date(2024, 12, 31)
VALIDATION_END = date(2025, 12, 31)
TEMPORAL_SPLIT_VERSION = "received-calendar-year-2024-2025-2026plus-v1"

TemporalSplit = Literal["train", "validation", "test"]


def split_for_received_date(received: date | None) -> TemporalSplit | None:
    """Assign a report to the fixed train/validation/test period, or None if undated."""
    if received is None:
        return None
    if received <= TRAINING_END:
        return "train"
    if received <= VALIDATION_END:
        return "validation"
    return "test"
