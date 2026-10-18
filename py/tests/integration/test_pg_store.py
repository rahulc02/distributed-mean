"""The coordinator service against real Postgres.

Runs only when DATABASE_URL points at a migrated database, for example:

    docker compose up -d postgres && docker compose run --rm migrate
    DATABASE_URL=postgresql://dmean:dmean@localhost:5432/dmean pytest tests/integration

Each test truncates the tables it uses.
"""

from __future__ import annotations

import asyncio
import os

import pytest

psycopg = pytest.importorskip("psycopg")
if not os.environ.get("DATABASE_URL"):
    pytest.skip("DATABASE_URL not set", allow_module_level=True)

from psycopg.rows import dict_row  # noqa: E402

from dmean.coordinator.model import Completion  # noqa: E402
from dmean.coordinator.pg import PgStore  # noqa: E402
from dmean.coordinator.service import Coordinator  # noqa: E402
from tests.fakes import FakePublisher  # noqa: E402

URL = os.environ["DATABASE_URL"]


async def connect():
    return await psycopg.AsyncConnection.connect(URL, autocommit=True, row_factory=dict_row)


async def fresh_job(conn, f=12, c=2) -> str:
    await conn.execute("TRUNCATE jobs, tasks, operands, events CASCADE")
    cur = await conn.execute(
        "INSERT INTO jobs (f, c, seed, status) VALUES (%s, %s, 1, 'NEW') RETURNING id::text AS id", (f, c)
    )
    return (await cur.fetchone())["id"]


def done(task, count=None):
    return Completion(task.job_id, task.task_id, task.output_key, count or task.input_count, 1, "w1")


def test_f12_end_to_end_through_the_real_schema():
    async def go():
        conn = await connect()
        job_id = await fresh_job(conn)
        publisher = FakePublisher()
        coord = Coordinator(PgStore(conn), publisher)
        assert await coord.start_new_jobs() == 1
        for leaf in list(publisher.queue):
            await coord.on_completion(done(leaf))
            await coord.on_completion(done(leaf))             # duplicate: no effect
        final = publisher.queue[-1]
        assert final.final and final.input_count == 12
        await coord.on_completion(done(final))
        cur = await conn.execute(
            "SELECT status, tasks_done, total_tasks, operands_done, total_operands FROM jobs WHERE id = %s",
            (job_id,),
        )
        job = await cur.fetchone()
        assert job["status"] == "DONE"
        assert job["tasks_done"] == job["total_tasks"] == 4
        assert job["operands_done"] == job["total_operands"] == 15
        await conn.close()

    asyncio.run(go())


def test_two_coordinators_racing_start_a_job_once():
    async def go():
        setup = await connect()
        await fresh_job(setup)
        a, b = await connect(), await connect()
        pa, pb = FakePublisher(), FakePublisher()
        started = await asyncio.gather(
            Coordinator(PgStore(a), pa).start_new_jobs(),
            Coordinator(PgStore(b), pb).start_new_jobs(),
        )
        assert sorted(started) == [0, 1]
        assert len(pa.queue) + len(pb.queue) == 3
        for conn in (setup, a, b):
            await conn.close()

    asyncio.run(go())
