from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

import pytest

from maude.ingestion.archive import ArchiveValidationError, inspect_source, open_source_text


def test_rejects_archive_path_traversal(tmp_path: Path) -> None:
    archive = tmp_path / "bad.zip"
    with ZipFile(archive, "w", ZIP_DEFLATED) as handle:
        handle.writestr("../patient.txt", "MDR_REPORT_KEY|PATIENT_SEQUENCE_NUMBER\r\n1|1\r\n")
    with pytest.raises(ArchiveValidationError, match="unsafe member"):
        inspect_source(archive)


def test_rejects_absolute_archive_member(tmp_path: Path) -> None:
    archive = tmp_path / "bad.zip"
    with ZipFile(archive, "w", ZIP_DEFLATED) as handle:
        handle.writestr("/patient.txt", "content")
    with pytest.raises(ArchiveValidationError, match="unsafe member"):
        inspect_source(archive)


def test_rejects_archive_with_more_than_one_text_member(tmp_path: Path) -> None:
    archive = tmp_path / "many.zip"
    with ZipFile(archive, "w", ZIP_DEFLATED) as handle:
        handle.writestr("one.txt", "content")
        handle.writestr("two.txt", "content")
    with pytest.raises(ArchiveValidationError, match="exactly one"):
        inspect_source(archive)


def test_inspects_single_text_member(tmp_path: Path) -> None:
    archive = tmp_path / "patient.zip"
    with ZipFile(archive, "w", ZIP_DEFLATED) as handle:
        handle.writestr("patient.txt", "MDR_REPORT_KEY|PATIENT_SEQUENCE_NUMBER\r\n1|1\r\n")
    inspected = inspect_source(archive)
    assert inspected.member == "patient.txt"
    assert len(inspected.sha256) == 64
    assert inspected.byte_size == archive.stat().st_size


def test_inspects_plain_text_source(tmp_path: Path) -> None:
    source = tmp_path / "patient.txt"
    source.write_bytes(b"MDR_REPORT_KEY|PATIENT_SEQUENCE_NUMBER\r\n1|1\r\n")
    inspected = inspect_source(source)
    assert inspected.path == source
    assert inspected.member is None
    assert inspected.sample == source.read_bytes()


def test_streams_plain_text_without_eagerly_reading_full_source(tmp_path: Path) -> None:
    source = tmp_path / "patient.txt"
    source.write_bytes(b"a" * 131072)
    inspected = inspect_source(source)
    assert len(inspected.sample) == 65536
    with open_source_text(source) as stream:
        assert stream.read(1) == "a"
        assert not stream.closed
    assert stream.closed


def test_streams_archive_member_and_closes_zip_handles(tmp_path: Path) -> None:
    archive = tmp_path / "patient.zip"
    with ZipFile(archive, "w", ZIP_DEFLATED) as handle:
        handle.writestr("patient.txt", "café\r\n")
    inspected = inspect_source(archive)
    with open_source_text(archive, inspected.member) as stream:
        assert stream.read() == "café\r\n"
        member_handle = stream.buffer
    assert stream.closed
    assert member_handle.closed


def test_source_encoding_scans_beyond_the_initial_sample_in_bounded_chunks(tmp_path: Path) -> None:
    from maude.ingestion.archive import select_source_encoding

    source = tmp_path / "patient.txt"
    source.write_bytes(b"header\n" + b"a" * 65_536 + b"\xb0\n")

    assert select_source_encoding(source) == "cp1252"
