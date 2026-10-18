"""The end-to-end gate: correct answers through the real stack, and under faults."""

from __future__ import annotations

import json
import time
import uuid

import numpy as np

from dmean.common.operand import encode
from dmean.common.rng import expected_mean

from .conftest import DATA, compose, http, queue_depth, set_workers, wait_for_status, wait_until_running


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


# About 1,500 tasks per job.
FAULT_JOB = (6000, 500)


def assert_still_running(db, job_id: str) -> None:
    """The fault must land mid-job, or the test proves nothing."""
    status = db.execute("SELECT status FROM jobs WHERE id = %s", (job_id,)).fetchone()[0]
    assert status == "RUNNING", f"job was {status} when the fault landed; the test proved nothing"


def test_killing_a_worker_mid_job_does_not_change_the_answer(db):
    f, c = FAULT_JOB
    # One worker makes the job long enough to catch mid-way, and with a backlog and
    # prefetch 1 that worker always holds an unacked task when it is killed.
    set_workers(1)
    try:
        job_id = submit(f, c)
        wait_until_running(db, job_id, min_tasks_done=5)
        pid = db.execute("SELECT pid FROM workers WHERE status = 'RUNNING'").fetchone()[0]
        compose("exec", "-T", "supervisor", "sh", "-c", f"kill -9 {pid}")   # its task is redelivered
        assert_still_running(db, job_id)
        check_generated(db, job_id, f, c)
    finally:
        set_workers(4)


def test_a_coordinator_outage_mid_job_does_not_change_the_answer(db):
    f, c = FAULT_JOB
    set_workers(0)                                         # nothing progresses until the crash
    try:
        job_id = submit(f, c)
        wait_until_running(db, job_id, min_tasks_done=0)
        compose("kill", "coordinator")                     # SIGKILL: no graceful shutdown
        try:
            # Workers keep summing leaves while no coordinator runs; their
            # completions must wait in the queue, not be lost.
            set_workers(4)
            deadline = time.monotonic() + 60
            while queue_depth("completions") < 100:
                assert time.monotonic() < deadline, "completions did not pile up while the coordinator was down"
                time.sleep(0.5)
            assert_still_running(db, job_id)               # only the coordinator finishes jobs
        finally:
            compose("start", "coordinator")                # recovery scan, then the backlog
        check_generated(db, job_id, f, c)
    finally:
        set_workers(4)


def test_changing_w_mid_job_does_not_change_the_answer(db):
    f, c = FAULT_JOB
    try:
        job_id = submit(f, c)
        wait_until_running(db, job_id, min_tasks_done=5)
        http("PUT", "/workers", {"count": 1})
        time.sleep(3)
        assert_still_running(db, job_id)
        http("PUT", "/workers", {"count": 6})
        check_generated(db, job_id, f, c)
    finally:
        set_workers(4)
