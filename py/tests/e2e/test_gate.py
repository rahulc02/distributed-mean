"""The end-to-end gate: correct answers through the real stack, and under faults."""

from __future__ import annotations

import json
import time
import uuid

import numpy as np

from dmean.common.operand import encode
from dmean.common.rng import expected_mean

from .conftest import DATA, compose, http, wait_for_status, wait_until_running


def read_result(job_id: str) -> np.ndarray:
    status, body = http("GET", f"/jobs/{job_id}/result")
    assert status == 200, body
    return np.array([float(v) for v in body.decode().strip().split(",")])


def test_f12_example_returns_6_5_and_65(db):
    """Known files written straight to storage: aggregation depends only on the contract."""
    job_id = str(uuid.uuid4())
    for i in range(1, 13):
        path = DATA / "files" / job_id / f"f{i}"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(encode(np.array([i, 10.0 * i]), 1))
    db.execute("INSERT INTO jobs (id, f, c, seed, status) VALUES (%s, 12, 2, 0, 'NEW')", (job_id,))

    wait_for_status(db, job_id, timeout_s=60)
    assert read_result(job_id).tolist() == [6.5, 65.0]


def submit(f: int, c: int) -> str:
    status, body = http("POST", "/jobs", {"f": f, "c": c})
    assert status == 202, body
    return json.loads(body)["id"]


def check_generated(db, job_id: str, f: int, c: int, timeout_s: float = 300) -> None:
    job = wait_for_status(db, job_id, timeout_s=timeout_s)
    np.testing.assert_allclose(read_result(job_id), expected_mean(job["seed"], f, c), rtol=1e-12, atol=0)


def test_generated_job_matches_an_independent_computation(db):
    f, c = 60, 300
    check_generated(db, submit(f, c), f, c)


def test_several_jobs_at_once_all_finish_correctly(db):
    sizes = [(37, 50), (120, 40), (8, 400)]
    ids = [submit(f, c) for f, c in sizes]
    for job_id, (f, c) in zip(ids, sizes):
        check_generated(db, job_id, f, c)


def test_bad_input_is_rejected_with_400():
    status, body = http("POST", "/jobs", {"f": 1, "c": 10})
    assert status == 400 and b"invalid request" in body


def test_killing_a_worker_mid_job_does_not_change_the_answer(db):
    f, c = 1500, 2000
    job_id = submit(f, c)
    wait_until_running(db, job_id, min_tasks_done=5)
    busy = db.execute("SELECT pid FROM workers WHERE status = 'RUNNING' AND busy LIMIT 1").fetchone()
    assert busy, "no busy worker to kill"
    compose("exec", "-T", "supervisor", "sh", "-c", f"kill -9 {busy[0]}")   # its task is redelivered
    check_generated(db, job_id, f, c)


def test_restarting_the_coordinator_mid_job_does_not_change_the_answer(db):
    f, c = 1500, 2000
    job_id = submit(f, c)
    wait_until_running(db, job_id, min_tasks_done=20)
    compose("restart", "coordinator")                                       # recovery scan on startup
    check_generated(db, job_id, f, c)


def test_changing_w_mid_job_does_not_change_the_answer(db):
    f, c = 1500, 2000
    job_id = submit(f, c)
    wait_until_running(db, job_id, min_tasks_done=5)
    http("PUT", "/workers", {"count": 1})
    time.sleep(3)
    http("PUT", "/workers", {"count": 6})
    check_generated(db, job_id, f, c)
    http("PUT", "/workers", {"count": 4})
