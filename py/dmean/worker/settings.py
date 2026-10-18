"""Worker configuration, from environment variables."""

from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class WorkerSettings(BaseSettings):
    model_config = SettingsConfigDict(extra="ignore")

    rabbit_url: str                               # RABBIT_URL
    storage_root: str = "/data"                   # STORAGE_ROOT
    fetch_threads: int = Field(5, ge=1)           # concurrent operand reads
    heartbeat_s: int = Field(60, ge=10)
    log_level: str = "INFO"
