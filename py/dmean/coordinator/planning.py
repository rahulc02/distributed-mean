"""Leaf and merge task construction and exact job totals. Pure."""

from __future__ import annotations

from . import keys
from .grouping import Group
from .model import Input, Job, Task


def leaf_count(f: int, k_leaf: int) -> int:
    return -(-f // k_leaf)


def leaf_size(f: int, k_leaf: int, g: int) -> int:
    """Number of files leaf g covers (the last leaf may be smaller)."""
    return min(k_leaf, f - g * k_leaf)


def merge_count(leaves: int, k_merge: int) -> int:
    """Every non-final merge removes exactly k_merge - 1 operands; L must reach 1."""
    return 0 if leaves <= 1 else -(-(leaves - 1) // (k_merge - 1))


def total_tasks(f: int, k_leaf: int, k_merge: int) -> int:
    leaves = leaf_count(f, k_leaf)
    return leaves + merge_count(leaves, k_merge)


def total_operands(f: int, k_leaf: int, k_merge: int) -> int:
    """Operand reads in a whole job: every file once, every partial but the result once."""
    leaves = leaf_count(f, k_leaf)
    return f + leaves + merge_count(leaves, k_merge) - 1


def leaf_tasks(job: Job) -> list[Task]:
    n = leaf_count(job.f, job.k_leaf)
    tasks = []
    for g in range(n):
        first = g * job.k_leaf + 1
        last = min((g + 1) * job.k_leaf, job.f)
        task_id = keys.leaf_task_id(job.id, g)
        tasks.append(
            Task(
                job_id=job.id,
                task_id=task_id,
                level=1,
                final=n == 1,
                f=job.f,
                c=job.c,
                inputs=tuple(Input(f"f{i}", keys.file_key(job.id, i), 1) for i in range(first, last + 1)),
                output_key=keys.result_key(job.id) if n == 1 else keys.partial_key(job.id, task_id),
            )
        )
    return tasks


def merge_task(job: Job, group: Group, task_id: str | None = None) -> Task:
    level = max(o.level for o in group.inputs) + 1
    task_id = task_id or keys.merge_task_id(job.id, level)
    return Task(
        job_id=job.id,
        task_id=task_id,
        level=level,
        final=group.final,
        f=job.f,
        c=job.c,
        inputs=tuple(Input(o.id, o.key, o.count) for o in group.inputs),
        output_key=keys.result_key(job.id) if group.final else keys.partial_key(job.id, task_id),
    )
