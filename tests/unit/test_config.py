from pathlib import Path

import pytest
from pydantic import ValidationError

from maude.config import Settings


def test_settings_reject_batch_size_below_one(tmp_path: Path) -> None:
    with pytest.raises(ValidationError):
        Settings(data_root=tmp_path, batch_rows=0)


def test_settings_accepts_environment_backed_data_root(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MAUDE_DATA_ROOT", "/tmp/maude-data")
    settings = Settings()
    assert settings.data_root == Path("/tmp/maude-data")
    assert settings.batch_rows == 50_000
    assert settings.parser_version == "1.0.0"


def test_settings_rejects_batch_size_above_limit(tmp_path: Path) -> None:
    with pytest.raises(ValidationError):
        Settings(data_root=tmp_path, batch_rows=500_001)
