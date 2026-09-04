"""Safe inspection and streaming access for local FDA source files."""

import hashlib
import io
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path, PurePosixPath
from typing import TextIO
from zipfile import BadZipFile, ZipFile, is_zipfile

from maude.domain.models import InspectedSource
from maude.ingestion.encoding import select_encoding

SAMPLE_SIZE = 64 * 1024
HASH_BLOCK_SIZE = 1024 * 1024


class ArchiveValidationError(ValueError):
    """Raised when a source is not a safe, supported FDA text source."""


def sha256_file(path: Path) -> str:
    """Return the SHA-256 digest of *path*, reading it in bounded chunks."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(HASH_BLOCK_SIZE), b""):
            digest.update(block)
    return digest.hexdigest()


def _unsafe_member(name: str) -> bool:
    member_path = PurePosixPath(name)
    return member_path.is_absolute() or ".." in member_path.parts


def inspect_source(path: Path) -> InspectedSource:
    """Validate and inspect a plain text source or single-member ZIP archive."""
    path = Path(path)
    digest = sha256_file(path)
    byte_size = path.stat().st_size

    if is_zipfile(path):
        try:
            with ZipFile(path) as archive:
                bad_member = archive.testzip()
                if bad_member is not None:
                    raise ArchiveValidationError(f"archive member failed CRC check: {bad_member!r}")

                infos = archive.infolist()
                for info in infos:
                    if _unsafe_member(info.filename):
                        raise ArchiveValidationError(f"unsafe member path: {info.filename!r}")

                non_directories = tuple(info for info in infos if not info.is_dir())
                if len(non_directories) != 1 or not non_directories[0].filename.lower().endswith(
                    ".txt"
                ):
                    raise ArchiveValidationError(
                        "archive must contain exactly one non-directory .txt member"
                    )

                selected = non_directories[0]
                with archive.open(selected, "r") as handle:
                    sample = handle.read(SAMPLE_SIZE)
        except ArchiveValidationError:
            raise
        except (BadZipFile, OSError, RuntimeError) as exc:
            raise ArchiveValidationError(f"invalid ZIP archive: {path}") from exc
        return InspectedSource(path, selected.filename, digest, byte_size, sample)

    if path.suffix.lower() != ".txt":
        raise ArchiveValidationError("source must be a .txt file or ZIP archive")
    with path.open("rb") as handle:
        sample = handle.read(SAMPLE_SIZE)
    return InspectedSource(path, None, digest, byte_size, sample)


@contextmanager
def open_source_text(path: Path, member: str | None = None) -> Iterator[TextIO]:
    """Open a source as strict text while streaming its underlying bytes."""
    inspected = inspect_source(path)
    selected = inspected.member
    if member is not None and member != selected:
        raise ArchiveValidationError(
            f"requested member {member!r} does not match inspected member {selected!r}"
        )

    encoding = select_encoding(inspected.sample)
    if selected is None:
        with (
            Path(path).open("rb") as raw_file,
            io.TextIOWrapper(raw_file, encoding=encoding, errors="strict", newline="") as text,
        ):
            yield text
        return

    with (
        ZipFile(path) as archive,
        archive.open(selected, "r") as raw_member,
        io.TextIOWrapper(raw_member, encoding=encoding, errors="strict", newline="") as text,
    ):
        yield text
