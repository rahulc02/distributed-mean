"""How a worker tells its supervisor what it is doing.

The supervisor hands each worker one end of a pipe. Standalone workers (run
with `python -m dmean.worker`) get a reporter that does nothing.
"""

from __future__ import annotations

from typing import Protocol


class StatusReporter(Protocol):
    def report(self, status: str) -> None: ...     # "ready" | "busy" | "idle"


class NullReporter:
    def report(self, status: str) -> None:
        pass


class PipeReporter:
    def __init__(self, conn) -> None:              # multiprocessing.connection.Connection
        self.conn = conn

    def report(self, status: str) -> None:
        try:
            self.conn.send(status)
        except (BrokenPipeError, OSError):
            pass                                    # supervisor gone: PDEATHSIG will stop us
