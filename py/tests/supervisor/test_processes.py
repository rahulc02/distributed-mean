"""Real processes, spawned with the same multiprocessing context the supervisor uses."""

import time

from dmean.supervisor.processes import ProcessTable
from tests.supervisor import dummy_workers


def wait_until(predicate, timeout=10.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.05)
    return False


def test_spawn_report_ready_drain_and_reap():
    table = ProcessTable(target=dummy_workers.well_behaved)
    a, b = table.spawn(), table.spawn()
    running = set()
    assert wait_until(lambda: running.update(table.read_status()) or running == {a, b})
    assert {v.status for v in table.views()} == {"RUNNING"}

    table.drain(a)
    exited = []
    assert wait_until(lambda: exited.extend(table.reap()) or exited)
    assert exited[0].id == a and exited[0].expected and exited[0].exit_code == 0
    assert [v.id for v in table.views()] == [b]
    table.drain_all(grace_s=5)
    assert not table.workers


def test_a_stubborn_worker_is_killed_after_the_grace_period():
    table = ProcessTable(target=dummy_workers.stubborn)
    worker = table.spawn()
    assert wait_until(lambda: table.read_status() == [worker])
    table.drain(worker)
    time.sleep(0.3)
    assert table.kill_overdue(grace_s=0.2) == [worker]
    exited = []
    assert wait_until(lambda: exited.extend(table.reap()) or exited)
    assert exited[0].exit_code != 0


def test_a_crash_on_start_is_reported_as_unexpected():
    table = ProcessTable(target=dummy_workers.crashes_on_start)
    table.spawn()
    exited = []
    assert wait_until(lambda: exited.extend(table.reap()) or exited)
    e = exited[0]
    assert not e.expected and not e.reached_running and e.exit_code == 3
