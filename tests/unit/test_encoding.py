import pytest

from maude.ingestion.encoding import EncodingError, select_encoding


def test_prefers_utf8_when_sample_is_valid() -> None:
    assert select_encoding("café".encode()) == "utf-8"


def test_uses_cp1252_for_legacy_fda_text() -> None:
    assert select_encoding(b"device\x92s label") == "cp1252"


def test_rejects_binary_nulls() -> None:
    with pytest.raises(EncodingError, match="NUL"):
        select_encoding(b"a\x00b")


def test_rejects_samples_invalid_under_supported_encodings() -> None:
    with pytest.raises(EncodingError, match="encoding"):
        select_encoding(b"\x81")
