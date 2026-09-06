from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="MAUDE_", env_file=".env")

    data_root: Path
    batch_rows: int = Field(default=50_000, ge=1, le=500_000)
    parser_version: str = "1.0.0"
