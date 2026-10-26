"""Deterministic narrative text preparation for modeling."""

import hashlib
import unicodedata

from maude.domain.models import (
    ModelingDocument,
    ModelingNarrative,
    NarrativeEvidence,
    ReportDocument,
)

NARRATIVE_CLEANING_VERSION = "nfc-whitespace-v1"


def clean_narrative_text(text: str) -> str:
    """Normalize Unicode and layout while preserving narrative content and punctuation."""
    normalized = unicodedata.normalize("NFC", text)
    flattened = "".join(
        " " if character in "\r\n\t" or unicodedata.category(character) == "Cc" else character
        for character in normalized
    )
    return " ".join(flattened.split())


def prepare_narrative_for_modeling(
    report_id: str,
    dataset_snapshot_id: str,
    evidence: NarrativeEvidence,
) -> ModelingNarrative | None:
    """Create a versioned model-text row; return None when cleaning leaves no text.

    The source NarrativeEvidence is immutable and is not modified. Its evidence ID,
    source location, and exact UTF-8 content hash travel with the derived text.
    """
    modeling_text = clean_narrative_text(evidence.text)
    if not modeling_text:
        return None

    return ModelingNarrative(
        report_id=report_id,
        dataset_snapshot_id=dataset_snapshot_id,
        evidence_id=evidence.evidence_id,
        text_type_code=evidence.text_type_code,
        source_filename=evidence.source_filename,
        source_line_number=evidence.source_line_number,
        source_text_sha256=hashlib.sha256(evidence.text.encode("utf-8")).hexdigest(),
        modeling_text=modeling_text,
        cleaning_version=NARRATIVE_CLEANING_VERSION,
    )


def prepare_report_for_modeling(report: ReportDocument) -> ModelingDocument | None:
    """Build one stable model document while retaining each cleaned evidence segment."""
    narratives = tuple(
        prepared
        for evidence in report.narratives
        if (
            prepared := prepare_narrative_for_modeling(
                report_id=report.report_id,
                dataset_snapshot_id=report.dataset_snapshot_id,
                evidence=evidence,
            )
        )
        is not None
    )
    if not narratives:
        return None

    return ModelingDocument(
        report_id=report.report_id,
        dataset_snapshot_id=report.dataset_snapshot_id,
        reported_event_type=report.reported_event_type,
        date_received=report.date_received,
        product_codes=report.product_codes,
        brand_names=report.brand_names,
        manufacturers=report.manufacturers,
        narratives=narratives,
        modeling_text="\n\n".join(item.modeling_text for item in narratives),
        cleaning_version=NARRATIVE_CLEANING_VERSION,
    )
