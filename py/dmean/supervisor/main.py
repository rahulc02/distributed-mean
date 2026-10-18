"""Supervisor process: a reconcile loop that keeps W worker processes running.

Observe the processes it owns, ask the pure `reconcile` what should change,
apply it, record it. `PUT /workers` wakes it immediately via NOTIFY.
"""

from __future__ import annotations

import logging
import signal
import socket
import threading
import time

import psycopg

from dmean.common.logs import setup_logging
from dmean.worker.main import run_worker

from .processes import ProcessTable
from .reconcile import reconcile, spawn_delay
from .repo import SupervisorRepo
from .settings import SupervisorSettings

log = logging.getLogger("dmean.supervisor")


def main() -> None:
    settings = SupervisorSettings()
    setup_logging("supervisor", settings.log_level)
    host = socket.gethostname()
    repo = SupervisorRepo(psycopg.connect(settings.database_url, autocommit=True), host)
    table = ProcessTable(target=run_worker, host=host)

    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())

    repo.forget_previous_run()
    repo.listen()
    failures, next_spawn_at = 0, 0.0
    log.info("supervising workers on %s", host)

    while not stop.is_set():
        repo.wait_for_change(settings.reconcile_interval_s)
        now = time.monotonic()

        for e in table.reap():
            repo.mark_exited(e)
            if e.expected:
                repo.log_event(f"worker {e.id} exited after draining")
            else:
                repo.log_event(f"worker {e.id} died unexpectedly (exit code {e.exit_code})")
                if not e.reached_running and e.lifetime_s < settings.startup_failure_window_s:
                    failures += 1
                    next_spawn_at = now + spawn_delay(failures)

        if table.read_status():
            failures = 0                                # a worker came up: the environment is healthy

        plan = reconcile(repo.desired_workers(), table.views())
        if plan.spawn and now >= next_spawn_at:
            for _ in range(plan.spawn):
                worker_id = table.spawn()
                repo.log_event(f"worker {worker_id} started")
        for worker_id in plan.drain:
            table.drain(worker_id)
            repo.log_event(f"worker {worker_id} draining")
        for worker_id in table.kill_overdue(settings.drain_grace_s):
            repo.log_event(f"worker {worker_id} force-killed after the drain grace period")

        repo.sync(table.views(), {v.id: table.pid(v.id) for v in table.views()})

    log.info("stopping: draining all workers")
    for e in table.drain_all(settings.drain_grace_s):
        repo.mark_exited(e)
    repo.log_event("supervisor stopped")
