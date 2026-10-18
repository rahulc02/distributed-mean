"""The supervisor's Postgres access: desired W in, worker status out.

The supervisor is the only writer of the workers table; workers themselves
never touch Postgres.
"""

from __future__ import annotations

import psycopg

from .processes import Exited
from .reconcile import WorkerView


class SupervisorRepo:
    def __init__(self, conn: psycopg.Connection, host: str) -> None:
        self.conn = conn                               # autocommit=True
        self.host = host

    def listen(self) -> None:
        self.conn.execute("LISTEN desired_workers")

    def wait_for_change(self, timeout_s: float) -> None:
        """Return when PUT /workers notifies us, or after timeout_s.

        notifies() also yields notifications that arrived during other queries.
        """
        for _ in self.conn.notifies(timeout=timeout_s, stop_after=1):
            pass

    def desired_workers(self) -> int:
        row = self.conn.execute("SELECT desired_workers FROM config").fetchone()
        return row[0] if row else 0

    def forget_previous_run(self) -> None:
        """Rows left by a previous supervisor on this host describe processes that no longer exist."""
        self.conn.execute(
            "UPDATE workers SET status = 'EXITED', exited_at = now() WHERE host = %s AND status <> 'EXITED'",
            (self.host,),
        )

    def sync(self, views: list[WorkerView], pids: dict[str, int | None]) -> None:
        with self.conn.transaction():
            for v in views:
                self.conn.execute(
                    """INSERT INTO workers (id, pid, host, status, busy)
                       VALUES (%s, %s, %s, %s, %s)
                       ON CONFLICT (id) DO UPDATE
                          SET status = EXCLUDED.status, busy = EXCLUDED.busy,
                              pid = EXCLUDED.pid, heartbeat_at = now()""",
                    (v.id, pids.get(v.id), self.host, v.status, v.busy),
                )

    def mark_exited(self, e: Exited) -> None:
        self.conn.execute(
            """UPDATE workers SET status = 'EXITED', busy = false, exited_at = now(), exit_code = %s
                WHERE id = %s""",
            (e.exit_code, e.id),
        )

    def log_event(self, message: str) -> None:
        self.conn.execute("INSERT INTO events (source, message) VALUES ('supervisor', %s)", (message,))
