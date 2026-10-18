"""Supervisor configuration. Workers inherit the environment (RABBIT_URL, STORAGE_ROOT)."""

from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class SupervisorSettings(BaseSettings):
    model_config = SettingsConfigDict(extra="ignore")

    database_url: str                                  # DATABASE_URL
    reconcile_interval_s: float = Field(2.0, gt=0)
    drain_grace_s: float = Field(600.0, gt=0)          # longer than any normal task
    startup_failure_window_s: float = Field(10.0, gt=0)
    log_level: str = "INFO"
