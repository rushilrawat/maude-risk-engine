from dataclasses import dataclass
from pathlib import Path
from uuid import UUID

_SHA256_LENGTH = 64


@dataclass(frozen=True)
class SnapshotLayout:
    data_root: Path
    snapshot_id: str

    @property
    def staging(self) -> Path:
        return self.data_root / "staging" / self.snapshot_id

    @property
    def promoted(self) -> Path:
        return self.data_root / "silver" / self.snapshot_id

    @property
    def bronze(self) -> Path:
        return self.staging / "bronze"

    @property
    def silver(self) -> Path:
        return self.staging / "silver"

    @property
    def rejects(self) -> Path:
        return self.staging / "rejects"

    @property
    def sources(self) -> Path:
        """Private immutable copies of inputs bound to this staging attempt."""
        return self.staging / "sources"

    @property
    def manifest(self) -> Path:
        return self.data_root / "manifests" / f"{self.snapshot_id}.json"

    @property
    def current_manifest(self) -> Path:
        return self.data_root / "manifests" / "current.json"


@dataclass(frozen=True)
class RefreshLayout:
    data_root: Path
    run_id: UUID

    @property
    def catalogs(self) -> Path:
        return self.data_root / "raw" / "catalogs"

    @property
    def raw_root(self) -> Path:
        return self.data_root / "raw" / "sha256"

    @property
    def run_manifest(self) -> Path:
        return self.data_root / "refresh-runs" / f"{self.run_id}.json"

    def raw_object(self, sha256: str) -> Path:
        if len(sha256) != _SHA256_LENGTH or any(
            character not in "0123456789abcdef" for character in sha256
        ):
            raise ValueError("SHA-256 must be 64 lowercase hexadecimal characters")
        return self.raw_root / sha256[:2] / f"{sha256}.zip"
