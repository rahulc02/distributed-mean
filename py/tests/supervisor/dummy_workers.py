"""Stand-in worker processes for testing the process table without RabbitMQ."""

import signal
import sys
import time


def well_behaved(worker_id, conn):
    """Reports ready, toggles busy/idle, exits cleanly on SIGTERM."""
    stopping = False

    def on_term(*_):
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, on_term)
    conn.send("ready")
    while not stopping:
        conn.send("busy")
        time.sleep(0.05)
        conn.send("idle")
        time.sleep(0.05)
    sys.exit(0)


def stubborn(worker_id, conn):
    """Ignores SIGTERM, like a worker stuck in a task."""
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    conn.send("ready")
    while True:
        time.sleep(0.1)


def crashes_on_start(worker_id, conn):
    sys.exit(3)
