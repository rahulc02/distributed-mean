"""Which READY operands become new tasks. Pure: no I/O, trivially testable."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from .model import Ready


@dataclass(frozen=True)
class Group:
    inputs: tuple[Ready, ...]
    final: bool  # the inputs cover every file: this task writes the result


def plan_groups(ready: Sequence[Ready], f: int, k: int) -> list[Group]:
    """Group a job's ready pool.

    - Every non-final group has exactly k inputs.
    - The tail rule: when fewer than k remain but their counts sum to f, nothing
      else can still be in flight (count conservation), so group them all.
    - Higher levels first keeps the pool small; ties break by id for determinism.
    """
    if k < 2:
        raise ValueError(f"k must be at least 2, got {k}")
    pool = sorted(ready, key=lambda o: (-o.level, o.id))
    groups: list[Group] = []
    while len(pool) >= k:
        take, pool = tuple(pool[:k]), pool[k:]
        groups.append(Group(take, final=sum(o.count for o in take) == f))
    if len(pool) >= 2 and sum(o.count for o in pool) == f:
        groups.append(Group(tuple(pool), final=True))
    return groups
