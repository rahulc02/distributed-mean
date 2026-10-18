"""Owns the worker processes: spawn, read their status, drain, kill, reap."""

from __future__ import annotations

import multiprocessing as mp
import socket
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from multiprocessing.connection import Connection
from multiprocessing.process import BaseProcess

from .reconcile import Status, WorkerView

Target = Callable[[str, Connection], None]


@dataclass
class Managed:
    id: str
    process: BaseProcess
    conn: Connection
    started_at: float
    status: Status = "STARTING"
    busy: bool = False
    drain_started: float | None = None


@dataclass(frozen=True)
class Exited:
    id: str
    exit_code: int | None
    expected: bool              # we asked it to stop
    lifetime_s: float
    reached_running: bool


@dataclass
class ProcessTable:
    target: Target
    ctx: mp.context.BaseContext = field(default_factory=lambda: mp.get_context("spawn"))
    clock: Callable[[], float] = time.monotonic
    host: str = field(default_factory=socket.gethostname)
    workers: dict[str, Managed] = field(default_factory=dict)
    _ever_running: set[str] = field(default_factory=set)

    def spawn(self) -> str:
        worker_id = f"{self.host}-{uuid.uuid4().hex[:6]}"
        recv, send = self.ctx.Pipe(duplex=False)
        process = self.ctx.Process(target=self.target, args=(worker_id, send), name=worker_id)
        process.start()
        send.close()                                   # the child owns the sending end
        self.workers[worker_id] = Managed(worker_id, process, recv, self.clock())
        return worker_id

    def read_status(self) -> list[str]:
        """Drain every pipe. Returns ids that just reached RUNNING."""
        became_running = []
        for w in self.workers.values():
            try:
                while w.conn.poll():
                    message = w.conn.recv()
                    if message == "ready" and w.status == "STARTING":
                        w.status = "RUNNING"
                        became_running.append(w.id)
                        self._ever_running.add(w.id)
                    elif message == "busy":
                        w.busy = True
                    elif message == "idle":
                        w.busy = False
            except (EOFError, OSError):
                pass                                    # the process is exiting; reap() will see it
        return became_running

    def drain(self, worker_id: str) -> None:
        w = self.workers[worker_id]
        if w.status != "DRAINING":
            w.process.terminate()                       # SIGTERM: finish the current task, then exit
            w.status = "DRAINING"
            w.drain_started = self.clock()

    def kill_overdue(self, grace_s: float) -> list[str]:
        killed = []
        for w in self.workers.values():
            if w.drain_started is not None and self.clock() - w.drain_started > grace_s and w.process.is_alive():
                w.process.kill()                        # its unacked task is redelivered by RabbitMQ
                killed.append(w.id)
        return killed

    def reap(self) -> list[Exited]:
        exited = []
        for worker_id, w in list(self.workers.items()):
            if not w.process.is_alive():
                w.process.join(timeout=0)
                w.conn.close()
                del self.workers[worker_id]
                exited.append(Exited(
                    worker_id, w.process.exitcode, expected=w.status == "DRAINING",
                    lifetime_s=self.clock() - w.started_at, reached_running=worker_id in self._ever_running,
                ))
                self._ever_running.discard(worker_id)
        return exited

    def views(self) -> list[WorkerView]:
        return [WorkerView(w.id, w.status, w.busy, w.started_at) for w in self.workers.values()]

    def pid(self, worker_id: str) -> int | None:
        return self.workers[worker_id].process.pid

    def drain_all(self, grace_s: float, poll_s: float = 0.2) -> list[Exited]:
        for worker_id in list(self.workers):
            self.drain(worker_id)
        exited: list[Exited] = []
        deadline = self.clock() + grace_s
        while self.workers and self.clock() < deadline:
            self.read_status()
            exited += self.reap()
            time.sleep(poll_s)
        for w in self.workers.values():
            w.process.kill()
        time.sleep(poll_s)
        return exited + self.reap()
