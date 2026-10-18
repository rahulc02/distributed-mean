"""Coordinator process shell: leadership, consumers and periodic loops.

One Postgres connection holds the leader lock and performs every write, so a
coordinator that loses its session can never write again. One asyncio.Lock (the
"turn") makes every unit of work run strictly one at a time.
"""

from __future__ import annotations

import asyncio
import logging
import signal
from collections.abc import Awaitable, Callable

import aio_pika
import psycopg
from aio_pika.abc import AbstractIncomingMessage
from psycopg.rows import dict_row
from pydantic import ValidationError

from dmean.common.logs import setup_logging
from dmean.common.messages import CompletionMessage, TaskMessage
from dmean.common.topology import COMPLETIONS, TASKS_DLQ

from .model import InvariantViolation
from .pg import PgStore
from .rabbit import RabbitPublisher, completion_from_message, declare_topology, task_from_message
from .service import Coordinator
from .settings import CoordinatorSettings

log = logging.getLogger("dmean.coordinator")


async def become_leader(conn: psycopg.AsyncConnection, key: int) -> None:
    """Block until this process holds the advisory lock; the lock dies with the session."""
    while True:
        cur = await conn.execute("SELECT pg_try_advisory_lock(%s) AS ok", (key,))
        row = await cur.fetchone()
        if row and row["ok"]:
            log.info("became leader")
            return
        log.info("standby: another coordinator is leader")
        await asyncio.sleep(2.0)


async def every(interval_s: float, turn: asyncio.Lock, step: Callable[[], Awaitable[object]]) -> None:
    while True:
        await asyncio.sleep(interval_s)
        try:
            async with turn:
                await step()
        except psycopg.OperationalError:
            raise                                    # lost the database: exit and let the restart policy act
        except Exception:
            log.exception("periodic step failed; retrying next interval")


async def run(settings: CoordinatorSettings) -> None:
    conn = await psycopg.AsyncConnection.connect(settings.database_url, autocommit=True, row_factory=dict_row)
    await become_leader(conn, settings.leader_lock_key)

    broker = await aio_pika.connect_robust(settings.rabbit_url)
    channel = await broker.channel(publisher_confirms=True)
    await declare_topology(channel)
    await channel.set_qos(prefetch_count=settings.completion_prefetch)

    coord = Coordinator(
        PgStore(conn), RabbitPublisher(channel),
        k_leaf=settings.k_leaf, k_merge=settings.k_merge, publish_grace_s=settings.publish_grace_s,
    )
    turn = asyncio.Lock()
    fatal = asyncio.Event()

    async with turn:
        await coord.recover()                        # startup scan: nothing is in flight yet

    async def on_completion(message: AbstractIncomingMessage) -> None:
        try:
            completion = completion_from_message(CompletionMessage.model_validate_json(message.body))
        except ValidationError:
            log.error("dropping malformed completion: %r", message.body[:200])
            await message.reject(requeue=False)
            return
        try:
            async with turn:
                await coord.on_completion(completion)
        except InvariantViolation as exc:
            async with turn:
                await coord.fail_job(completion.job_id, str(exc))
        except psycopg.OperationalError:
            log.exception("database lost while handling a completion")
            fatal.set()
            return                                    # unacked: redelivered to the next leader
        except Exception:
            log.exception("completion handling failed; requeueing", extra={"task_id": completion.task_id})
            await asyncio.sleep(1.0)
            await message.nack(requeue=True)
            return
        await message.ack()                           # only after commit and publish

    async def on_dead_letter(message: AbstractIncomingMessage) -> None:
        try:
            task = task_from_message(TaskMessage.model_validate_json(message.body))
        except ValidationError:
            log.error("dropping malformed dead letter: %r", message.body[:200])
            await message.ack()
            return
        async with turn:
            await coord.on_dead_letter(task)
        await message.ack()

    await (await channel.get_queue(COMPLETIONS)).consume(on_completion)
    await (await channel.get_queue(TASKS_DLQ)).consume(on_dead_letter)

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)

    loops = [
        asyncio.create_task(every(settings.poll_new_jobs_s, turn, coord.start_new_jobs)),
        asyncio.create_task(every(settings.scan_interval_s, turn, coord.scan)),
        asyncio.create_task(fatal.wait()),
        asyncio.create_task(stop.wait()),
    ]
    done, pending = await asyncio.wait(loops, return_when=asyncio.FIRST_COMPLETED)
    for task in pending:
        task.cancel()
    async with turn:                                  # let the current unit of work finish
        await broker.close()
        await conn.close()
    for task in done:
        if task.exception():
            raise task.exception()                    # type: ignore[misc]
    if fatal.is_set():
        raise SystemExit(1)


def main() -> None:
    settings = CoordinatorSettings()
    setup_logging("coordinator", settings.log_level)
    asyncio.run(run(settings))
