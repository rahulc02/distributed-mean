"""Coordinator configuration, from environment variables, validated at startup."""

from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class CoordinatorSettings(BaseSettings):
    model_config = SettingsConfigDict(extra="ignore")

    database_url: str                                  # DATABASE_URL
    rabbit_url: str                                    # RABBIT_URL
    k_leaf: int = Field(5, ge=2)                       # K_LEAF: files per leaf task
    k_merge: int = Field(5, ge=2)                      # K_MERGE: partials per merge task
    poll_new_jobs_s: float = Field(1.0, gt=0)
    scan_interval_s: float = Field(10.0, gt=0)
    publish_grace_s: float = Field(5.0, ge=0)
    completion_prefetch: int = Field(20, ge=1)
    leader_lock_key: int = 424242
    log_level: str = "INFO"
