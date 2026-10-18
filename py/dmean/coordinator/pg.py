"""Postgres implementation of the Store port.

Each method is one guarded statement. The service decides what to do; this
module only says how, so the logic tested against the in-memory fake is the
logic that runs here.

Expects a psycopg AsyncConnection opened with autocommit=True and
row_factory=dict_row; every unit of work runs inside `transaction()`.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager

from psycopg import AsyncConnection
from psycopg.types.json import Jsonb

from .model import Completion, Input, InvariantViolation, Job, MergeDone, Ready, Task
from .ports import TotalsFn


def _job(row: dict) -> Job:
    return Job(row["id"], row["f"], row["c"], row["k_leaf"] or 0, row["k_merge"] or 0, row["status"])


class PgTx:
    def __init__(self, conn: AsyncConnection) -> None:
        self.conn = conn

    # ---------------------------------------------------------------- jobs

    async def claim_new_job(self, k_leaf: int, k_merge: int, totals: TotalsFn) -> Job | None:
        cur = await self.conn.execute(
            """SELECT id::text AS id, f, c FROM jobs
                WHERE status = 'NEW'
                ORDER BY created_at
                LIMIT 1
                FOR UPDATE SKIP LOCKED"""
        )
        row = await cur.fetchone()
        if row is None:
            return None
        total_tasks, total_operands = totals(row["f"])
        await self.conn.execute(
            """UPDATE jobs
                  SET status = 'RUNNING', k_leaf = %s, k_merge = %s,
                      total_tasks = %s, total_operands = %s,
                      leaves_published = false, started_at = now()
                WHERE id = %s AND status = 'NEW'""",
            (k_leaf, k_merge, total_tasks, total_operands, row["id"]),
        )
        return Job(row["id"], row["f"], row["c"], k_leaf, k_merge, "RUNNING")

    async def lock_job(self, job_id: str) -> Job | None:
        cur = await self.conn.execute(
            """SELECT id::text AS id, f, c, k_leaf, k_merge, status
                 FROM jobs WHERE id = %s FOR UPDATE""",
            (job_id,),
        )
        row = await cur.fetchone()
        return _job(row) if row else None

    async def add_progress(self, job_id: str, operands_read: int) -> None:
        await self.conn.execute(
            """UPDATE jobs SET tasks_done = tasks_done + 1,
                              operands_done = operands_done + %s
                WHERE id = %s""",
            (operands_read, job_id),
        )

    async def mark_job_done(self, job_id: str, result_key: str) -> bool:
        cur = await self.conn.execute(
            """UPDATE jobs SET status = 'DONE', result_key = %s, finished_at = now()
                WHERE id = %s AND status = 'RUNNING'""",
            (result_key, job_id),
        )
        return cur.rowcount == 1

    async def fail_job(self, job_id: str, reason: str) -> bool:
        cur = await self.conn.execute(
            """UPDATE jobs SET status = 'FAILED', error = %s, finished_at = now()
                WHERE id = %s AND status IN ('GENERATING', 'NEW', 'RUNNING')""",
            (reason, job_id),
        )
        return cur.rowcount == 1

    async def mark_leaves_published(self, job_id: str) -> None:
        await self.conn.execute("UPDATE jobs SET leaves_published = true WHERE id = %s", (job_id,))

    async def jobs_needing_leaves(self) -> list[Job]:
        cur = await self.conn.execute(
            """SELECT id::text AS id, f, c, k_leaf, k_merge, status FROM jobs
                WHERE status = 'RUNNING' AND NOT leaves_published"""
        )
        return [_job(r) for r in await cur.fetchall()]

    # ---------------------------------------------------------------- tasks

    async def mark_leaf_done(self, c: Completion, files: int) -> bool:
        """Leaf rows exist only once done; the deterministic id makes duplicates collide."""
        cur = await self.conn.execute(
            """INSERT INTO tasks (id, job_id, level, input_count, output_key, status,
                                  published_at, worker_id, duration_ms, completed_at)
               VALUES (%s, %s, 1, %s, %s, 'DONE', now(), %s, %s, now())
               ON CONFLICT (id) DO NOTHING""",
            (c.task_id, c.job_id, files, c.output_key, c.worker_id, c.duration_ms),
        )
        return cur.rowcount == 1

    async def mark_merge_done(self, c: Completion) -> MergeDone | None:
        cur = await self.conn.execute(
            """UPDATE tasks SET status = 'DONE', worker_id = %s, duration_ms = %s, completed_at = now()
                WHERE id = %s AND status = 'PENDING'
            RETURNING input_count, jsonb_array_length(inputs) AS n_inputs""",
            (c.worker_id, c.duration_ms, c.task_id),
        )
        row = await cur.fetchone()
        return MergeDone(row["input_count"], row["n_inputs"]) if row else None

    async def create_task(self, task: Task) -> None:
        await self.conn.execute(
            """INSERT INTO tasks (id, job_id, level, inputs, input_count, final, output_key, status)
               VALUES (%s, %s, %s, %s, %s, %s, %s, 'PENDING')""",
            (
                task.task_id, task.job_id, task.level,
                Jsonb([{"id": i.id, "key": i.key, "count": i.count} for i in task.inputs]),
                task.input_count, task.final, task.output_key,
            ),
        )
        ids = [i.id for i in task.inputs]
        cur = await self.conn.execute(
            """UPDATE operands SET status = 'CONSUMED', consumed_by = %s
                WHERE id = ANY(%s) AND status = 'READY'""",
            (task.task_id, ids),
        )
        if cur.rowcount != len(ids):
            raise InvariantViolation(f"task {task.task_id}: only {cur.rowcount} of {len(ids)} inputs were READY")

    async def fail_task(self, task_id: str, job_id: str, level: int) -> None:
        await self.conn.execute(
            """INSERT INTO tasks (id, job_id, level, status) VALUES (%s, %s, %s, 'FAILED')
               ON CONFLICT (id) DO UPDATE SET status = 'FAILED' WHERE tasks.status <> 'DONE'""",
            (task_id, job_id, level),
        )

    async def done_leaf_ids(self, job_id: str) -> set[str]:
        cur = await self.conn.execute("SELECT id FROM tasks WHERE job_id = %s AND level = 1", (job_id,))
        return {r["id"] for r in await cur.fetchall()}

    async def unpublished_tasks(self, older_than_s: float) -> list[Task]:
        cur = await self.conn.execute(
            """SELECT t.id, t.job_id::text AS job_id, t.level, t.inputs, t.final, t.output_key, j.f, j.c
                 FROM tasks t JOIN jobs j ON j.id = t.job_id
                WHERE t.status = 'PENDING' AND t.published_at IS NULL
                  AND j.status = 'RUNNING'
                  AND t.created_at <= now() - make_interval(secs => %s)
                ORDER BY t.created_at""",
            (older_than_s,),
        )
        return [
            Task(
                job_id=r["job_id"], task_id=r["id"], level=r["level"], final=r["final"],
                f=r["f"], c=r["c"], output_key=r["output_key"],
                inputs=tuple(Input(i["id"], i["key"], i["count"]) for i in r["inputs"]),
            )
            for r in await cur.fetchall()
        ]

    async def mark_published(self, task_ids: Sequence[str]) -> None:
        await self.conn.execute(
            "UPDATE tasks SET published_at = now() WHERE id = ANY(%s) AND published_at IS NULL",
            (list(task_ids),),
        )

    # ---------------------------------------------------------------- operands

    async def insert_ready(self, job_id: str, operand: Ready) -> None:
        await self.conn.execute(
            """INSERT INTO operands (id, job_id, key, count, level, status)
               VALUES (%s, %s, %s, %s, %s, 'READY')""",
            (operand.id, job_id, operand.key, operand.count, operand.level),
        )

    async def ready_operands(self, job_id: str) -> list[Ready]:
        cur = await self.conn.execute(
            """SELECT id, key, count, level FROM operands
                WHERE job_id = %s AND status = 'READY'
                ORDER BY level DESC, id""",
            (job_id,),
        )
        return [Ready(r["id"], r["key"], r["count"], r["level"]) for r in await cur.fetchall()]

    # ---------------------------------------------------------------- events

    async def log_event(self, job_id: str | None, message: str) -> None:
        await self.conn.execute(
            "INSERT INTO events (source, job_id, message) VALUES ('coordinator', %s, %s)",
            (job_id, message),
        )


class PgStore:
    """One connection, used by one unit of work at a time (the coordinator's turn lock)."""

    def __init__(self, conn: AsyncConnection) -> None:
        self.conn = conn

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[PgTx]:
        async with self.conn.transaction():
            yield PgTx(self.conn)
