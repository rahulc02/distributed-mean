"""The supervisor's decisions. Pure: no processes, no database."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Literal

Status = Literal["STARTING", "RUNNING", "DRAINING"]


@dataclass(frozen=True)
class WorkerView:
    id: str
    status: Status
    busy: bool
    started_at: float


@dataclass(frozen=True)
class Plan:
    spawn: int
    drain: tuple[str, ...]


def reconcile(desired: int, workers: Sequence[WorkerView]) -> Plan:
    """Spawn up to `desired`, or drain the extras: idle first, then the youngest."""
    active = [w for w in workers if w.status != "DRAINING"]
    if len(active) < desired:
        return Plan(spawn=desired - len(active), drain=())
    extra = len(active) - desired
    victims = sorted(active, key=lambda w: (w.busy, -w.started_at))[:extra]
    return Plan(spawn=0, drain=tuple(w.id for w in victims))


def spawn_delay(recent_failures: int, base_s: float = 1.0, cap_s: float = 60.0) -> float:
    """Exponential backoff after workers die on startup (e.g. the broker is unreachable)."""
    if recent_failures <= 0:
        return 0.0
    return min(cap_s, base_s * 2 ** (recent_failures - 1))
