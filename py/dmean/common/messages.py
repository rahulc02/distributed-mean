"""Queue message contracts, version 1.

Spec: contracts/messages.schema.json. Every service validates messages where
they enter, so a malformed message fails loudly instead of corrupting state.
"""

from __future__ import annotations

from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class _Message(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class OperandRef(_Message):
    id: str            # f{i} for input files, the producing task id for partials
    key: str           # object storage key
    count: int = Field(gt=0)


class TaskMessage(_Message):
    """Coordinator to workers, on the 'tasks' queue."""

    v: Literal[1] = 1
    job_id: UUID
    task_id: str
    level: int = Field(ge=1)
    final: bool        # input counts sum to f: write the result CSV, not a partial
    f: int = Field(ge=2)
    c: int = Field(ge=2)
    inputs: list[OperandRef] = Field(min_length=1)
    output_key: str    # chosen by the coordinator; workers never build keys


class CompletionMessage(_Message):
    """Workers to coordinator, on the 'completions' queue."""

    v: Literal[1] = 1
    job_id: UUID
    task_id: str
    output_key: str
    count: int = Field(gt=0)
    duration_ms: int = Field(ge=0)
    worker_id: str
