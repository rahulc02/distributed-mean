"""The coordinator's logic. Talks only to the Store and Publisher ports.

Every method is safe to rerun: commits happen before publishes, every write is
guarded, and duplicates of any message change nothing.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Iterable, Iterator, Sequence
from typing import TypeVar

from . import keys
from .grouping import plan_groups
from .model import Completion, InvariantViolation, Job, Ready, Task
from .planning import leaf_size, leaf_tasks, merge_task, total_operands, total_tasks
from .ports import Publisher, Store

log = logging.getLogger("dmean.coordinator")
T = TypeVar("T")


def chunked(items: Sequence[T], size: int) -> Iterator[Sequence[T]]:
    for start in range(0, len(items), size):
        yield items[start : start + size]


class Coordinator:
    def __init__(
        self,
        store: Store,
        publisher: Publisher,
        k_leaf: int = 5,
        k_merge: int = 5,
        publish_grace_s: float = 5.0,
        publish_batch: int = 1000,
    ) -> None:
        self.store = store
        self.publisher = publisher
        self.k_leaf = k_leaf
        self.k_merge = k_merge
        self.publish_grace_s = publish_grace_s
        self.publish_batch = publish_batch

    # ------------------------------------------------------------ job start

    def _totals(self, f: int) -> tuple[int, int]:
        return total_tasks(f, self.k_leaf, self.k_merge), total_operands(f, self.k_leaf, self.k_merge)

    async def start_new_jobs(self) -> int:
        """NEW -> RUNNING for every waiting job, then publish its leaves."""
        started = 0
        while True:
            async with self.store.transaction() as tx:
                job = await tx.claim_new_job(self.k_leaf, self.k_merge, self._totals)
                if job is not None:
                    await tx.log_event(job.id, f"job started: F={job.f}, K_leaf={job.k_leaf}, K_merge={job.k_merge}")
            if job is None:
                return started
            await self._publish_leaves(job)
            started += 1

    async def _publish_leaves(self, job: Job, skip: Iterable[str] = ()) -> None:
        skipped = set(skip)
        tasks = [t for t in leaf_tasks(job) if t.task_id not in skipped]
        for batch in chunked(tasks, self.publish_batch):
            await asyncio.gather(*(self.publisher.publish(t) for t in batch))  # confirms overlap
        async with self.store.transaction() as tx:
            await tx.mark_leaves_published(job.id)

    # ------------------------------------------------------------ completions

    async def on_completion(self, c: Completion) -> None:
        """Record a task's first completion, grow the merge tree, finish the job."""
        new_tasks: list[Task] = []
        async with self.store.transaction() as tx:
            job = await tx.lock_job(c.job_id)                # serializes this job's commits
            if job is None or job.status != "RUNNING":
                return                                        # unknown, finished or failed job

            level = keys.task_level(c.task_id)
            if level == 1:
                files = leaf_size(job.f, job.k_leaf, keys.leaf_index(c.task_id))
                if not await tx.mark_leaf_done(c, files):
                    return                                    # duplicate completion
                expected, operands_read = files, files
            else:
                done = await tx.mark_merge_done(c)
                if done is None:
                    return                                    # duplicate completion
                expected, operands_read = done.input_count, done.n_inputs

            if c.count != expected:
                raise InvariantViolation(
                    f"task {c.task_id} reported count {c.count}, expected {expected}"
                )
            await tx.add_progress(job.id, operands_read)

            if c.count == job.f:                               # the final task finished
                if await tx.mark_job_done(job.id, c.output_key):
                    await tx.log_event(job.id, "job done")
                return

            await tx.insert_ready(job.id, Ready(c.task_id, c.output_key, c.count, level))
            for group in plan_groups(await tx.ready_operands(job.id), job.f, job.k_merge):
                task = merge_task(job, group)
                await tx.create_task(task)
                new_tasks.append(task)

        # Committed. Publishing after the commit means a crash here leaves the
        # tasks with published_at NULL, which recovery republishes.
        await self._publish(new_tasks)

    async def _publish(self, tasks: Sequence[Task]) -> None:
        if not tasks:
            return
        for task in tasks:
            await self.publisher.publish(task)
        async with self.store.transaction() as tx:
            await tx.mark_published([t.task_id for t in tasks])

    # ------------------------------------------------------------ failures

    async def fail_job(self, job_id: str, reason: str) -> None:
        async with self.store.transaction() as tx:
            if await tx.fail_job(job_id, reason):
                await tx.log_event(job_id, f"job failed: {reason}")
        log.error("job %s failed: %s", job_id, reason)

    async def on_dead_letter(self, task: Task) -> None:
        """A task exhausted its retries or carried corrupt data: fail it and its job."""
        async with self.store.transaction() as tx:
            await tx.fail_task(task.task_id, task.job_id, task.level)
            if await tx.fail_job(task.job_id, f"task {task.task_id} dead-lettered"):
                await tx.log_event(task.job_id, f"task {task.task_id} dead-lettered; job failed")

    # ------------------------------------------------------------ recovery

    async def recover(self) -> None:
        """Startup or leader takeover: nothing is in flight, so no grace period."""
        await self._republish_unpublished(older_than_s=0.0)
        await self._republish_missing_leaves()

    async def scan(self) -> None:
        """Periodic: only tasks stranded longer than a normal commit-to-publish gap."""
        await self._republish_unpublished(older_than_s=self.publish_grace_s)
        await self._republish_missing_leaves()

    async def _republish_unpublished(self, older_than_s: float) -> None:
        async with self.store.transaction() as tx:
            tasks = await tx.unpublished_tasks(older_than_s)
        if tasks:
            log.warning("republishing %d committed but unpublished tasks", len(tasks))
        await self._publish(tasks)

    async def _republish_missing_leaves(self) -> None:
        async with self.store.transaction() as tx:
            jobs = await tx.jobs_needing_leaves()
        for job in jobs:
            async with self.store.transaction() as tx:
                done = await tx.done_leaf_ids(job.id)
                await tx.log_event(job.id, "republishing leaves after an interrupted publish")
            await self._publish_leaves(job, skip=done)
