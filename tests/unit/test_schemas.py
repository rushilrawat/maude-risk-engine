import pytest

from maude.domain.enums import TableKind
from maude.ingestion.schemas import SchemaMismatch, detect_table, normalize_column, spec_for


def test_normalize_column_is_stable() -> None:
    assert normalize_column("UDI-DI") == "udi_di"
    assert normalize_column("MANUFACTURER_LINK_FLAG_") == "manufacturer_link_flag"


@pytest.mark.parametrize(
    ("filename", "columns", "expected"),
    [
        ("mdrfoi.txt", ("MDR_REPORT_KEY", "EVENT_TYPE", "DATE_RECEIVED"), TableKind.MASTER),
        (
            "DEVICE.txt",
            ("MDR_REPORT_KEY", "DEVICE_SEQUENCE_NO", "DEVICE_REPORT_PRODUCT_CODE"),
            TableKind.DEVICE,
        ),
        ("patient.txt", ("MDR_REPORT_KEY", "PATIENT_SEQUENCE_NUMBER"), TableKind.PATIENT),
        (
            "foitext.txt",
            ("MDR_REPORT_KEY", "MDR_TEXT_KEY", "TEXT_TYPE_CODE", "FOI_TEXT"),
            TableKind.NARRATIVE,
        ),
    ],
)
def test_detect_table(filename: str, columns: tuple[str, ...], expected: TableKind) -> None:
    assert detect_table(filename, columns).kind is expected


def test_detect_table_rejects_missing_required_column() -> None:
    with pytest.raises(SchemaMismatch, match="FOI_TEXT"):
        detect_table("foitext.txt", ("MDR_REPORT_KEY", "MDR_TEXT_KEY", "TEXT_TYPE_CODE"))


def test_detect_table_rejects_ambiguous_filename() -> None:
    with pytest.raises(SchemaMismatch, match="ambiguous"):
        detect_table(
            "device_patient.txt",
            ("MDR_REPORT_KEY", "DEVICE_SEQUENCE_NO", "PATIENT_SEQUENCE_NUMBER"),
        )


def test_detect_table_requires_filename_family() -> None:
    with pytest.raises(SchemaMismatch, match="filename"):
        detect_table("unknown.txt", ("MDR_REPORT_KEY", "EVENT_TYPE", "DATE_RECEIVED"))


def test_spec_for_returns_unique_spec() -> None:
    assert spec_for(TableKind.MASTER).kind is TableKind.MASTER
