"""End-to-end tests against the running docker-compose stack.

    docker compose up -d --build
    cd py && E2E=1 pytest tests/e2e -v

They talk to the API on localhost:3000, read Postgres on localhost:5432, and
read/write the ./data bind mount that the API and workers share.
"""

from __future__ import annotations

import base64
import json
import os
import subprocess
import time
import urllib.parse
import urllib.request
from pathlib import Path

import pytest

if not os.environ.get("E2E"):
    pytest.skip("set E2E=1 to run end-to-end tests against docker compose", allow_module_level=True)

psycopg = pytest.importorskip("psycopg")

REPO = Path(__file__).resolve().parents[3]
DATA = Path(os.environ.get("E2E_DATA_DIR", REPO / "data"))
API = os.environ.get("E2E_API", "http://localhost:3000")
DB = os.environ.get("E2E_DATABASE_URL", "postgresql://dmean:dmean@localhost:5432/dmean")
RABBIT_ADMIN = os.environ.get("E2E_RABBIT_ADMIN", "http://dmean:dmean@localhost:15672")


def http(method: str, path: str, body: dict | None = None) -> tuple[int, bytes]:
    req = urllib.request.Request(
        API + path, method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as res:
            return res.status, res.read()
    except urllib.error.HTTPError as err:
        return err.code, err.read()


def compose(*args: str) -> str:
    return subprocess.run(["docker", "compose", *args], cwd=REPO, check=True, capture_output=True, text=True).stdout


@pytest.fixture(scope="session")
def db():
    with psycopg.connect(DB, autocommit=True) as conn:
        yield conn


def set_workers(count: int, timeout_s: float = 60) -> None:
    """Set W and wait until exactly `count` workers run and none are still starting or draining."""
    status, _ = http("PUT", "/workers", {"count": count})
    assert status == 202, "API not reachable; is the stack up?"
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        _, body = http("GET", "/workers")
        w = json.loads(body)
        if w["running"] == count and w["starting"] == 0 and w["draining"] == 0:
            return
        time.sleep(0.5)
    pytest.fail(f"workers did not settle at {count} within {timeout_s} s")


def queue_depth(name: str) -> int:
    """Messages waiting in a RabbitMQ queue, from the management API."""
    url = urllib.parse.urlsplit(RABBIT_ADMIN)
    req = urllib.request.Request(f"{url.scheme}://{url.hostname}:{url.port}/api/queues/%2F/{name}")
    token = base64.b64encode(f"{url.username}:{url.password}".encode()).decode()
    req.add_header("Authorization", f"Basic {token}")
    with urllib.request.urlopen(req, timeout=10) as res:
        return json.loads(res.read()).get("messages_ready", 0)


@pytest.fixture(scope="session", autouse=True)
def workers_running():
    set_workers(4)


def wait_for_status(db, job_id: str, status: str = "DONE", timeout_s: float = 300) -> dict:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        row = db.execute("SELECT status, error, result_key, seed FROM jobs WHERE id = %s", (job_id,)).fetchone()
        if row and row[0] == status:
            return {"status": row[0], "error": row[1], "result_key": row[2], "seed": row[3]}
        if row and row[0] == "FAILED":
            pytest.fail(f"job {job_id} failed: {row[1]}")
        time.sleep(0.5)
    pytest.fail(f"job {job_id} did not reach {status} within {timeout_s} s")


def wait_until_running(db, job_id: str, min_tasks_done: int = 1, timeout_s: float = 120) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        row = db.execute("SELECT status, tasks_done FROM jobs WHERE id = %s", (job_id,)).fetchone()
        if row and row[0] == "RUNNING" and row[1] >= min_tasks_done:
            return
        time.sleep(0.2)
    pytest.fail(f"job {job_id} never got going")
