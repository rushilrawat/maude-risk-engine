"""Durable, atomically published snapshot manifests."""

import os
import tempfile
from pathlib import Path

from maude.domain.models import SnapshotResult


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
    except BaseException:
        temporary_path.unlink(missing_ok=True)
        raise


def load_manifest(path: Path) -> SnapshotResult:
    """Load and validate one persisted snapshot manifest."""
    return SnapshotResult.model_validate_json(Path(path).read_text(encoding="utf-8"))
