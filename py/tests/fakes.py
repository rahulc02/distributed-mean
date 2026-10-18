"""In-memory implementations of the coordinator's ports.

FakeStore mirrors pg.py statement by statement: the same guards, the same
"changed a row or not" results, and real rollback on exceptions. Tests drive
the real Coordinator service through it.
"""

from __future__ import annotations

import asyncio
import copy
import random
from collections.abc import Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass, field, replace

from dmean.coordinator.model import Completion, Job, MergeDone, Ready, Task


@dataclass
class TaskRow:
    id: str
    job_id: str
    level: int
    status: str                      # PENDING | DONE | FAILED
    task: Task | None = None         # merges only: inputs, final, output key
    published: bool = False


@dataclass
class OperandRow:
    job_id: str
    ready: Ready
    consumed_by: str | None = None


@dataclass
class JobRow:
    job: Job
    leaves_published: bool = False
    total_tasks: int = 0
    total_operands: int = 0
    tasks_done: int = 0
    operands_done: int = 0
    result_key: str | None = None
    error: str | None = None


@dataclass
class State:
    jobs: dict[str, JobRow] = field(default_factory=dict)
    new_jobs: list[Job] = field(default_factory=list)
    tasks: dict[str, TaskRow] = field(default_factory=dict)
    operands: dict[str, OperandRow] = field(default_factory=dict)
    events: list[tuple[str | None, str]] = field(default_factory=list)


class FakeTx:
    def __init__(self, s: State) -> None:
        self.s = s

    async def claim_new_job(self, k_leaf, k_merge, totals):
        if not self.s.new_jobs:
            return None
        pending = self.s.new_jobs.pop(0)
        job = replace(pending, k_leaf=k_leaf, k_merge=k_merge, status="RUNNING")
        t, o = totals(job.f)
        self.s.jobs[job.id] = JobRow(job, total_tasks=t, total_operands=o)
        return job

    async def lock_job(self, job_id):
        row = self.s.jobs.get(job_id)
        return row.job if row else None

    async def add_progress(self, job_id, operands_read):
        row = self.s.jobs[job_id]
        row.tasks_done += 1
        row.operands_done += operands_read

    async def mark_job_done(self, job_id, result_key):
        row = self.s.jobs[job_id]
        if row.job.status != "RUNNING":
            return False
        row.job = replace(row.job, status="DONE")
        row.result_key = result_key
        return True

    async def fail_job(self, job_id, reason):
        row = self.s.jobs.get(job_id)
        if row is None or row.job.status not in ("GENERATING", "NEW", "RUNNING"):
            return False
        row.job = replace(row.job, status="FAILED")
        row.error = reason
        return True

    async def mark_leaves_published(self, job_id):
        self.s.jobs[job_id].leaves_published = True

    async def jobs_needing_leaves(self):
        return [r.job for r in self.s.jobs.values() if r.job.status == "RUNNING" and not r.leaves_published]

    async def mark_leaf_done(self, c: Completion, files: int):
        if c.task_id in self.s.tasks:                 # ON CONFLICT DO NOTHING
            return False
        self.s.tasks[c.task_id] = TaskRow(c.task_id, c.job_id, 1, "DONE", published=True)
        return True

    async def mark_merge_done(self, c: Completion):
        row = self.s.tasks.get(c.task_id)
        if row is None or row.status != "PENDING":    # WHERE status = 'PENDING'
            return None
        row.status = "DONE"
        assert row.task is not None
        return MergeDone(input_count=row.task.input_count, n_inputs=len(row.task.inputs))

    async def create_task(self, task: Task):
        assert task.task_id not in self.s.tasks, "task ids are unique"
        self.s.tasks[task.task_id] = TaskRow(task.task_id, task.job_id, task.level, "PENDING", task)
        for i in task.inputs:                         # UPDATE ... WHERE status = 'READY'
            op = self.s.operands[i.id]
            if op.consumed_by is not None:
                raise AssertionError(f"operand {i.id} consumed twice")
            op.consumed_by = task.task_id

    async def fail_task(self, task_id, job_id, level):
        row = self.s.tasks.get(task_id)
        if row is None:
            self.s.tasks[task_id] = TaskRow(task_id, job_id, level, "FAILED")
        else:
            row.status = "FAILED"

    async def done_leaf_ids(self, job_id):
        return {t.id for t in self.s.tasks.values() if t.job_id == job_id and t.level == 1}

    async def unpublished_tasks(self, older_than_s):
        return [t.task for t in self.s.tasks.values() if t.status == "PENDING" and not t.published and t.task]

    async def mark_published(self, task_ids: Sequence[str]):
        for task_id in task_ids:
            self.s.tasks[task_id].published = True

    async def insert_ready(self, job_id, operand: Ready):
        assert operand.id not in self.s.operands, "an operand is inserted once (primary key)"
        self.s.operands[operand.id] = OperandRow(job_id, operand)

    async def ready_operands(self, job_id):
        return [o.ready for o in self.s.operands.values() if o.job_id == job_id and o.consumed_by is None]

    async def log_event(self, job_id, message):
        self.s.events.append((job_id, message))


class FakeStore:
    def __init__(self) -> None:
        self.state = State()

    @asynccontextmanager
    async def transaction(self):
        snapshot = copy.deepcopy(self.state)
        try:
            yield FakeTx(self.state)
        except BaseException:
            self.state = snapshot                    # rollback
            raise
        await asyncio.sleep(0)

    def add_new_job(self, job_id: str, f: int, c: int) -> None:
        self.state.new_jobs.append(Job(job_id, f, c, k_leaf=0, k_merge=0, status="NEW"))


class PublishFailed(RuntimeError):
    """Simulates the coordinator dying between commit and publish."""


class FakePublisher:
    """Collects published tasks; optionally fails at random to simulate crashes."""

    def __init__(
        self, rng: random.Random | None = None, fail_rate: float = 0.0, max_failures: int | None = None
    ) -> None:
        self.queue: list[Task] = []
        self.rng = rng or random.Random(0)
        self.fail_rate = fail_rate
        self.failures_left = max_failures  # crashes are rare events, not a constant rate

    async def publish(self, task: Task) -> None:
        await asyncio.sleep(0)
        if self.fail_rate and self.failures_left != 0 and self.rng.random() < self.fail_rate:
            if self.failures_left is not None:
                self.failures_left -= 1
            raise PublishFailed(task.task_id)
        self.queue.append(task)
