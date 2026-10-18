"""Drive the real Coordinator through a hostile simulated system.

The simulated broker delivers tasks and completions in random order, delivers
some twice, and redelivers tasks whose worker "crashed". The simulated
coordinator crashes between commit and publish and then recovers. Whatever
happens, the job must finish with the exact mean and exactly L + M tasks.

Input file i holds [i, 10i], so every sum is an exact integer in float64 and
the expected mean [(F+1)/2, 5(F+1)] can be compared with ==.
"""

from __future__ import annotations

import asyncio
import random

import numpy as np
import pytest

from dmean.coordinator import keys
from dmean.coordinator.model import Completion, Task
from dmean.coordinator.planning import total_tasks
from dmean.coordinator.service import Coordinator
from tests.fakes import FakePublisher, FakeStore, PublishFailed

JOB = "6f1c2a8e-3d4b-4c5a-9e7f-1a2b3c4d5e6f"
C = 2


def seed_inputs(f: int) -> dict[str, tuple[np.ndarray, int]]:
    return {keys.file_key(JOB, i): (np.array([i, 10.0 * i]), 1) for i in range(1, f + 1)}


def execute(task: Task, objects: dict[str, tuple[np.ndarray, int]]) -> Completion:
    """A simulated worker: sum the inputs, write the output, report."""
    total, count = np.zeros(C), 0
    for i in task.inputs:
        values, n = objects[i.key]
        total = total + values
        count += n
    objects[task.output_key] = (total / task.f, count) if task.final else (total, count)
    return Completion(task.job_id, task.task_id, task.output_key, count, 1, "sim")


async def simulate(seed: int) -> None:
    rng = random.Random(seed)
    f = rng.randint(2, 160)
    k_leaf, k_merge = rng.randint(2, 7), rng.randint(2, 7)
    dup_rate, crash_rate = rng.choice([0, 0.1, 0.3]), rng.choice([0, 0.1, 0.3])

    store = FakeStore()
    publisher = FakePublisher(rng, fail_rate=rng.choice([0, 0.02, 0.1]), max_failures=rng.randint(0, 6))
    coord = Coordinator(store, publisher, k_leaf=k_leaf, k_merge=k_merge)
    objects = seed_inputs(f)
    completions: list[Completion] = []
    store.add_new_job(JOB, f=f, c=C)

    async def guarded(step) -> None:
        """Run a coordinator step; on a simulated crash, restart and recover."""
        try:
            await step()
        except PublishFailed:
            while True:
                try:
                    await coord.recover()
                    return
                except PublishFailed:
                    continue

    await guarded(coord.start_new_jobs)

    idle_scans = 0
    for _ in range(200_000):
        job = store.state.jobs[JOB]
        if job.job.status == "DONE" and not publisher.queue and not completions:
            break
        # Recovery happens on its own schedule too (the periodic scan).
        if rng.random() < 0.01:
            await guarded(coord.scan)

        if publisher.queue and (not completions or rng.random() < 0.5):
            task = publisher.queue.pop(rng.randrange(len(publisher.queue)))
            if rng.random() < crash_rate:
                publisher.queue.append(task)           # worker died: redelivered later
                continue
            if rng.random() < dup_rate:
                publisher.queue.append(task)           # delivered twice
            completion = execute(task, objects)
            completions.append(completion)
            if rng.random() < dup_rate:
                completions.append(completion)         # completion published twice
        elif completions:
            completion = completions.pop(rng.randrange(len(completions)))
            try:
                await coord.on_completion(completion)
            except PublishFailed:
                # Committed, then crashed before publishing. The completion is
                # already acknowledged in effect: recovery must repair the gap.
                await guarded(coord.recover)
        else:
            # Nothing in flight and the job is not done: a scan must unstick it,
            # or the pipeline is stuck (for example, a broken tail rule).
            await guarded(coord.scan)
            idle_scans += 1
            if idle_scans > 3 and not publisher.queue:
                break
            continue
        idle_scans = 0

    row = store.state.jobs[JOB]
    assert row.job.status == "DONE", f"seed {seed}: job stuck in {row.job.status}"
    result, count = objects[row.result_key]
    assert count == f
    assert result.tolist() == [(f + 1) / 2, 5.0 * (f + 1)], f"seed {seed}: wrong mean"

    done_tasks = [t for t in store.state.tasks.values() if t.status == "DONE"]
    assert len(done_tasks) == total_tasks(f, k_leaf, k_merge) == row.tasks_done
    assert row.operands_done == row.total_operands
    consumed = [o for o in store.state.operands.values() if o.consumed_by]
    assert len(consumed) == len(store.state.operands)  # every partial was merged exactly once


@pytest.mark.parametrize("seed", list(range(300)))
def test_pipeline_is_exact_under_reordering_duplicates_and_crashes(seed):
    asyncio.run(simulate(seed))
