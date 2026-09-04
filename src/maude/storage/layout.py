from dataclasses import dataclass
from pathlib import Path


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
    def manifest(self) -> Path:
        return self.data_root / "manifests" / f"{self.snapshot_id}.json"

    @property
    def current_manifest(self) -> Path:
        return self.data_root / "manifests" / "current.json"
