"""Canonical MAUDE source table schemas and filename/header detection."""

import re
from dataclasses import dataclass
from os import PathLike

from maude.domain.enums import TableKind


class SchemaMismatch(ValueError):
    """Raised when a source cannot be identified as exactly one table schema."""


@dataclass(frozen=True)
class TableSpec:
    kind: TableKind
    filename_tokens: tuple[str, ...]
    required_columns: tuple[str, ...]
    business_key: tuple[str, ...]
    date_columns: tuple[str, ...]


TABLE_SPECS = (
    TableSpec(
        TableKind.MASTER,
        ("mdrfoi",),
        ("MDR_REPORT_KEY", "DATE_RECEIVED", "EVENT_TYPE"),
        ("MDR_REPORT_KEY",),
        ("DATE_RECEIVED", "DATE_REPORT", "DATE_OF_EVENT"),
    ),
    TableSpec(
        TableKind.DEVICE,
        ("device", "foidev"),
        ("MDR_REPORT_KEY", "DEVICE_SEQUENCE_NO", "DEVICE_REPORT_PRODUCT_CODE"),
        ("MDR_REPORT_KEY", "DEVICE_SEQUENCE_NO"),
        ("DATE_RECEIVED", "DATE_RETURNED_TO_MANUFACTURER"),
    ),
    TableSpec(
        TableKind.PATIENT,
        ("patient",),
        ("MDR_REPORT_KEY", "PATIENT_SEQUENCE_NUMBER"),
        ("MDR_REPORT_KEY", "PATIENT_SEQUENCE_NUMBER"),
        ("DATE_RECEIVED",),
    ),
    TableSpec(
        TableKind.NARRATIVE,
        ("foitext",),
        ("MDR_REPORT_KEY", "MDR_TEXT_KEY", "TEXT_TYPE_CODE", "FOI_TEXT"),
        ("MDR_TEXT_KEY",),
        ("DATE_REPORT",),
    ),
)


def normalize_column(name: str) -> str:
    """Normalize a source header into a stable snake-case identifier."""
    value = re.sub(r"[^a-z0-9]+", "_", name.strip().lower()).strip("_")
    return re.sub(r"_+", "_", value)


def spec_for(kind: TableKind) -> TableSpec:
    """Return the sole registered specification for *kind*."""
    matches = tuple(spec for spec in TABLE_SPECS if spec.kind is kind)
    if len(matches) != 1:
        raise SchemaMismatch(f"expected exactly one table spec for {kind!r}, found {len(matches)}")
    return matches[0]


def detect_table(filename: str | PathLike[str], columns: tuple[str, ...]) -> TableSpec:
    """Identify a source by its filename family and complete required header."""
    basename = str(filename).rsplit("/", 1)[-1].lower()
    filename_matches = tuple(
        spec
        for spec in TABLE_SPECS
        if any(token.lower() in basename for token in spec.filename_tokens)
    )
    if not filename_matches:
        raise SchemaMismatch(f"filename {filename!r} does not match a known table family")
    if len(filename_matches) > 1:
        kinds = ", ".join(spec.kind.value for spec in filename_matches)
        raise SchemaMismatch(f"ambiguous filename {filename!r} matches table families: {kinds}")

    spec = filename_matches[0]
    available = {normalize_column(column) for column in columns}
    missing = tuple(
        column for column in spec.required_columns if normalize_column(column) not in available
    )
    if missing:
        raise SchemaMismatch(
            f"filename {filename!r} matches {spec.kind.value} but missing required columns: "
            f"{', '.join(missing)}"
        )
    return spec
