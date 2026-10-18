"""Worker process: consume one task at a time, sum it, report the completion.

Threading follows pika's documented pattern for long work: the AMQP connection
lives on the main thread, the task runs on a separate thread, and the result is
handed back with add_callback_threadsafe. The connection keeps servicing
heartbeats however long a task takes.
"""

from __future__ import annotations

import functools
import logging
import os
import signal
import socket
import threading
from concurrent.futures import ThreadPoolExecutor

import pika
from pydantic import ValidationError

from dmean.common.logs import setup_logging
from dmean.common.messages import CompletionMessage, TaskMessage
from dmean.common.operand import CorruptOperand
from dmean.common.storage import LocalStore
from dmean.common.topology import COMPLETIONS, TASKS

from .executor import CountMismatch, run_task
from .settings import WorkerSettings
from .status import NullReporter, PipeReporter, StatusReporter

log = logging.getLogger("dmean.worker")

# Bad data never gets better on retry: dead-letter at once.
PERMANENT = (CorruptOperand, CountMismatch, ValidationError, FileNotFoundError)


def set_parent_death_signal() -> None:
    """Linux: receive SIGTERM if the supervisor dies, so we drain instead of being orphaned."""
    try:
        import ctypes

        libc = ctypes.CDLL("libc.so.6", use_errno=True)
        PR_SET_PDEATHSIG = 1
        libc.prctl(PR_SET_PDEATHSIG, signal.SIGTERM)
    except OSError:
        pass                                          # not Linux: best effort


class Worker:
    def __init__(self, settings: WorkerSettings, worker_id: str, status: StatusReporter) -> None:
        self.settings = settings
        self.worker_id = worker_id
        self.status = status
        self.store = LocalStore(settings.storage_root)
        self.fetchers = ThreadPoolExecutor(max_workers=settings.fetch_threads, thread_name_prefix="fetch")
        self.inflight = threading.Event()

    def run(self) -> None:
        params = pika.URLParameters(self.settings.rabbit_url)
        params.heartbeat = self.settings.heartbeat_s
        self.conn = pika.BlockingConnection(params)
        self.ch = self.conn.channel()
        self.ch.confirm_delivery()                    # basic_publish waits for the broker
        self.ch.basic_qos(prefetch_count=1)           # one task at a time
        self.tag = self.ch.basic_consume(TASKS, self._on_message, consumer_tag=self.worker_id)

        signal.signal(signal.SIGTERM, self._drain)
        signal.signal(signal.SIGINT, self._drain)
        self.status.report("ready")
        log.info("consuming", extra={"worker_id": self.worker_id})

        self.ch.start_consuming()                     # returns once our consumer is cancelled
        while self.inflight.is_set():                 # finish the task we already hold
            self.conn.process_data_events(time_limit=1)
        self.conn.close()
        self.fetchers.shutdown()
        log.info("drained and exiting", extra={"worker_id": self.worker_id})

    def _drain(self, *_: object) -> None:
        log.info("drain requested", extra={"worker_id": self.worker_id})
        self.conn.add_callback_threadsafe(lambda: self.ch.basic_cancel(self.tag))

    # Runs on the connection thread.
    def _on_message(self, ch, method, _props, body: bytes) -> None:
        self.inflight.set()
        self.status.report("busy")
        threading.Thread(target=self._work, args=(method.delivery_tag, body), daemon=True).start()

    # Runs on the task thread.
    def _work(self, tag: int, body: bytes) -> None:
        try:
            task = TaskMessage.model_validate_json(body)
            result = run_task(task, self.store, self.fetchers)
            done = CompletionMessage(
                job_id=task.job_id, task_id=task.task_id, output_key=result.output_key,
                count=result.count, duration_ms=result.duration_ms, worker_id=self.worker_id,
            )
            outcome = functools.partial(self._complete, tag, done.model_dump_json().encode())
        except PERMANENT as exc:
            log.error("permanent failure, dead-lettering: %s", exc, extra={"worker_id": self.worker_id})
            outcome = functools.partial(self._settle, lambda: self.ch.basic_reject(tag, requeue=False))
        except Exception:
            log.exception("transient failure, requeueing", extra={"worker_id": self.worker_id})
            outcome = functools.partial(self._settle, lambda: self.ch.basic_nack(tag, requeue=True))
        self.conn.add_callback_threadsafe(outcome)

    # Back on the connection thread.
    def _complete(self, tag: int, completion: bytes) -> None:
        try:
            self.ch.basic_publish(                    # confirmed by the broker before we ack
                "", COMPLETIONS, completion,
                pika.BasicProperties(content_type="application/json", delivery_mode=2),
            )
        except Exception:
            log.exception("could not publish completion; requeueing the task")
            self._settle(lambda: self.ch.basic_nack(tag, requeue=True))
            return
        self._settle(lambda: self.ch.basic_ack(tag))

    def _settle(self, action) -> None:
        try:
            action()
        finally:
            self.inflight.clear()
            self.status.report("idle")


def run_worker(worker_id: str, status_conn=None) -> None:
    """Entry point used by the supervisor (one process per worker)."""
    set_parent_death_signal()
    settings = WorkerSettings()
    setup_logging("worker", settings.log_level)
    reporter = PipeReporter(status_conn) if status_conn is not None else NullReporter()
    Worker(settings, worker_id, reporter).run()


def main() -> None:
    """Standalone: `python -m dmean.worker`, outside the supervisor."""
    worker_id = os.environ.get("WORKER_ID") or f"{socket.gethostname()}-{os.getpid()}"
    run_worker(worker_id)
