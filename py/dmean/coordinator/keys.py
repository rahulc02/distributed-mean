"""Every naming convention in one place. Workers never build keys; they get them."""

from __future__ import annotations

import uuid


def file_key(job_id: str, i: int) -> str:
    return f"files/{job_id}/f{i}"


def partial_key(job_id: str, task_id: str) -> str:
    return f"partials/{job_id}/{task_id}"


def result_key(job_id: str) -> str:
    return f"results/{job_id}.csv"


def leaf_task_id(job_id: str, g: int) -> str:
    """Leaf g sums files k*g+1 .. k*(g+1). Deterministic, so duplicates collide."""
    return f"{job_id}:1:{g}"


def merge_task_id(job_id: str, level: int) -> str:
    return f"{job_id}:{level}:{uuid.uuid4().hex[:12]}"


def task_level(task_id: str) -> int:
    """Task ids are '{job}:{level}:{suffix}'; job ids are UUIDs without colons."""
    return int(task_id.split(":")[1])


def leaf_index(task_id: str) -> int:
    _job, level, g = task_id.split(":")
    if level != "1":
        raise ValueError(f"not a leaf task id: {task_id}")
    return int(g)
