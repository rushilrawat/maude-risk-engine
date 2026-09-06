"""Bounded, strict encoding detection for FDA source text."""


class EncodingError(ValueError):
    """Raised when a source sample cannot be safely decoded as supported text."""


def select_encoding(sample: bytes) -> str:
    """Select the supported encoding for a bounded source sample.

    UTF-8 is preferred for valid samples; CP1252 is retained for legacy FDA
    exports. NUL bytes are rejected because they indicate a binary source.
    """
    if b"\x00" in sample:
        raise EncodingError("source sample contains NUL bytes")

    try:
        sample.decode("utf-8", errors="strict")
    except UnicodeDecodeError:
        try:
            sample.decode("cp1252", errors="strict")
        except UnicodeDecodeError as exc:
            raise EncodingError(
                "no supported encoding: sample is not valid UTF-8 or CP1252 text"
            ) from exc
        return "cp1252"
    return "utf-8"
