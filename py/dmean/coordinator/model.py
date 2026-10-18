"""Plain data types for the coordinator's core. No I/O, no libraries."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Job:
    id: str
    f: int
    c: int
    k_leaf: int
    k_merge: int
    status: str = "RUNNING"


@dataclass(frozen=True)
class Input:
    """One operand a task will read: an input file or a partial."""

    id: str
    key: str
    count: int


@dataclass(frozen=True)
class Task:
    job_id: str
    task_id: str
    level: int
    final: bool
    f: int
    c: int
    inputs: tuple[Input, ...]
    output_key: str

    @property
    def input_count(self) -> int:
        """Files covered by this task's output."""
        return sum(i.count for i in self.inputs)


@dataclass(frozen=True)
class Ready:
    """A partial waiting in a job's ready pool."""

    id: str
    key: str
    count: int
    level: int


@dataclass(frozen=True)
class Completion:
    job_id: str
    task_id: str
    output_key: str
    count: int
    duration_ms: int
    worker_id: str


@dataclass(frozen=True)
class MergeDone:
    """What the store returns when a merge task is marked done for the first time."""

    input_count: int  # files covered, which the completion's count must match
    n_inputs: int     # operands read, for exact progress


class InvariantViolation(RuntimeError):
    """State that the design says cannot happen. The job is failed, never guessed at."""
