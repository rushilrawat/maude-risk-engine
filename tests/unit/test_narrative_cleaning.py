import hashlib
from datetime import date

from maude.domain.models import (
    ModelingDocument,
    ModelingNarrative,
    NarrativeEvidence,
    ReportDocument,
)
from maude.features.documents import (
    NARRATIVE_CLEANING_VERSION,
    clean_narrative_text,
    prepare_narrative_for_modeling,
    prepare_report_for_modeling,
)


def _evidence(text: str) -> NarrativeEvidence:
    return NarrativeEvidence(
        evidence_id="narrative:123",
        text_type_code="N",
        text=text,
        source_filename="foitext.txt",
        source_line_number=42,
    )


def test_clean_narrative_normalizes_unicode_and_layout_without_losing_content() -> None:
    text = "Cafe\u0301\tNO leakage; 5 mm\r\nModel X-200.\x00"

    assert clean_narrative_text(text) == "Café NO leakage; 5 mm Model X-200."


def test_clean_narrative_for_modeling_keeps_source_evidence_and_provenance() -> None:
    evidence = _evidence("No fluid leak. Device ABC-12, 3 mm.")

    result = prepare_narrative_for_modeling(
        report_id="456",
        dataset_snapshot_id="snapshot-1",
        evidence=evidence,
    )

    assert isinstance(result, ModelingNarrative)
    assert result.report_id == "456"
    assert result.dataset_snapshot_id == "snapshot-1"
    assert result.evidence_id == evidence.evidence_id
    assert result.text_type_code == evidence.text_type_code
    assert result.source_filename == evidence.source_filename
    assert result.source_line_number == evidence.source_line_number
    assert result.source_text_sha256 == hashlib.sha256(evidence.text.encode("utf-8")).hexdigest()
    assert result.modeling_text == evidence.text
    assert result.cleaning_version == NARRATIVE_CLEANING_VERSION
    assert evidence.text == "No fluid leak. Device ABC-12, 3 mm."


def test_clean_narrative_for_modeling_omits_empty_model_text() -> None:
    assert (
        prepare_narrative_for_modeling(
            report_id="456",
            dataset_snapshot_id="snapshot-1",
            evidence=_evidence(" \t\r\n\x00 "),
        )
        is None
    )


def test_prepare_report_for_modeling_joins_cleaned_segments_with_evidence_links() -> None:
    report = ReportDocument(
        report_id="456",
        reported_event_type="D",
        date_received=date(2025, 1, 2),
        product_codes=("ABC",),
        brand_names=("Device A",),
        manufacturers=("Maker A",),
        narratives=(
            _evidence("No leak.\r\nDevice ABC-12."),
            NarrativeEvidence(
                evidence_id="narrative:124",
                text_type_code="F",
                text="5 mm fracture.",
                source_filename="foitext.txt",
                source_line_number=43,
            ),
        ),
        dataset_snapshot_id="snapshot-1",
    )

    result = prepare_report_for_modeling(report)

    assert isinstance(result, ModelingDocument)
    assert result.report_id == report.report_id
    assert result.dataset_snapshot_id == report.dataset_snapshot_id
    assert result.reported_event_type == report.reported_event_type
    assert result.date_received == report.date_received
    assert result.product_codes == report.product_codes
    assert result.brand_names == report.brand_names
    assert result.manufacturers == report.manufacturers
    assert result.modeling_text == "No leak. Device ABC-12.\n\n5 mm fracture."
    assert [item.evidence_id for item in result.narratives] == ["narrative:123", "narrative:124"]
    assert result.cleaning_version == NARRATIVE_CLEANING_VERSION


def test_prepare_report_for_modeling_omits_reports_with_only_blank_narratives() -> None:
    report = ReportDocument(
        report_id="456",
        reported_event_type=None,
        date_received=None,
        product_codes=(),
        brand_names=(),
        manufacturers=(),
        narratives=(_evidence(" \t\r\n"),),
        dataset_snapshot_id="snapshot-1",
    )

    assert prepare_report_for_modeling(report) is None
