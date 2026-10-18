"""Run one task: read its operands, sum them, write the output. No queue, no DB."""

from __future__ import annotations

import time
from concurrent.futures import Executor
from dataclasses import dataclass
from typing import Protocol, Sequence

from dmean.common.operand import decode, encode, to_csv
from dmean.common.storage import ObjectStore

from .adders import adder_for


class InputRef(Protocol):
    key: str
    count: int


class TaskLike(Protocol):
    task_id: str
    final: bool
    f: int
    c: int
    output_key: str

    @property
    def inputs(self) -> Sequence[InputRef]: ...


@dataclass(frozen=True)
class TaskResult:
    output_key: str
    count: int
    duration_ms: int


class CountMismatch(ValueError):
    """An input's stored count disagrees with the task message: corrupt data, never retry."""


def run_task(task: TaskLike, store: ObjectStore, pool: Executor) -> TaskResult:
    started = time.monotonic()

    def load(ref: InputRef):
        values, count = decode(store.get(ref.key), task.c)
        if count != ref.count:
            raise CountMismatch(f"{ref.key}: stored count {count}, task says {ref.count}")
        return values, count

    total, count = adder_for(len(task.inputs), pool).sum(task.inputs, load)
    if task.final:
        if count != task.f:
            raise CountMismatch(f"final task covers {count} files, job has {task.f}")
        store.put(task.output_key, to_csv(total / task.f))   # the job's answer
    else:
        store.put(task.output_key, encode(total, count))      # a partial: sum, never mean
    return TaskResult(task.output_key, count, int((time.monotonic() - started) * 1000))
