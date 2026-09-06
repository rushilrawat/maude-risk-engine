"""Durable, atomically published snapshot manifests."""

import errno
import os
import tempfile
from pathlib import Path

from maude.domain.models import SnapshotResult


def fsync_directory(path: Path) -> None:
    """Sync a directory entry where the host filesystem exposes directory fsync.

    Some filesystems do not support syncing directory descriptors.  Those known
    portability limitations are ignored; other errors still surface so callers
    never claim durability they could have established.
    """
    unsupported = {errno.EINVAL, errno.ENOTSUP, errno.EOPNOTSUPP}
    try:
        descriptor = os.open(path, os.O_RDONLY)
    except OSError as error:
        if error.errno in unsupported:
            return
        raise
    try:
        os.fsync(descriptor)
    except OSError as error:
        if error.errno not in unsupported:
            raise
    finally:
        os.close(descriptor)


def write_manifest(path: Path, snapshot: SnapshotResult) -> None:
    """Atomically persist a validated snapshot manifest at *path*."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    temporary_path = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(snapshot.model_dump_json(indent=2))
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        temporary_path.replace(path)
        fsync_directory(path.parent)
    except BaseException:
        temporary_path.unlink(missing_ok=True)
        raise


def load_manifest(path: Path) -> SnapshotResult:
    """Load and validate one persisted snapshot manifest."""
    return SnapshotResult.model_validate_json(Path(path).read_text(encoding="utf-8"))
