import asyncio

import pytest

from dmean.coordinator.model import Completion, InvariantViolation
from dmean.coordinator.service import Coordinator
from tests.fakes import FakePublisher, FakeStore, PublishFailed

JOB = "6f1c2a8e-3d4b-4c5a-9e7f-1a2b3c4d5e6f"


def setup(f=12):
    store, publisher = FakeStore(), FakePublisher()
    store.add_new_job(JOB, f=f, c=2)
    return store, publisher, Coordinator(store, publisher)


def done(task, count=None):
    return Completion(task.job_id, task.task_id, task.output_key, count or task.input_count, 1, "w1")


def run(coro):
    return asyncio.run(coro)


def test_starting_a_job_publishes_its_leaves_then_flags_them():
    store, publisher, coord = setup()
    assert run(coord.start_new_jobs()) == 1
    assert [t.task_id for t in publisher.queue] == [f"{JOB}:1:{g}" for g in range(3)]
    row = store.state.jobs[JOB]
    assert row.leaves_published and row.total_tasks == 4 and row.total_operands == 12 + 3 + 1 - 1


def test_duplicate_completion_changes_nothing():
    store, publisher, coord = setup()
    run(coord.start_new_jobs())
    leaf = publisher.queue[0]
    run(coord.on_completion(done(leaf)))
    before = store.state.jobs[JOB].tasks_done
    run(coord.on_completion(done(leaf)))
    assert store.state.jobs[JOB].tasks_done == before == 1


def test_third_leaf_triggers_the_final_task_and_its_completion_finishes_the_job():
    store, publisher, coord = setup()
    run(coord.start_new_jobs())
    for leaf in list(publisher.queue):
        run(coord.on_completion(done(leaf)))
    final = publisher.queue[-1]
    assert final.final and final.level == 2 and final.input_count == 12
    run(coord.on_completion(done(final)))
    row = store.state.jobs[JOB]
    assert row.job.status == "DONE" and row.result_key == final.output_key
    assert row.tasks_done == row.total_tasks and row.operands_done == row.total_operands


def test_a_wrong_count_is_an_invariant_violation_and_rolls_back():
    store, publisher, coord = setup()
    run(coord.start_new_jobs())
    with pytest.raises(InvariantViolation):
        run(coord.on_completion(done(publisher.queue[0], count=4)))
    assert store.state.jobs[JOB].tasks_done == 0
    assert not store.state.tasks


def test_a_crash_between_commit_and_publish_is_repaired_by_recover():
    store, publisher, coord = setup()
    run(coord.start_new_jobs())
    leaves = list(publisher.queue)
    run(coord.on_completion(done(leaves[0])))
    run(coord.on_completion(done(leaves[1])))
    publisher.fail_rate = 1.0                          # the next publish "crashes"
    with pytest.raises(PublishFailed):
        run(coord.on_completion(done(leaves[2])))
    assert store.state.jobs[JOB].tasks_done == 3       # the commit happened
    publisher.fail_rate = 0.0
    run(coord.recover())
    assert publisher.queue[-1].final                   # the merge was republished


def test_completions_for_a_finished_job_are_ignored():
    store, publisher, coord = setup(f=3)
    run(coord.start_new_jobs())
    (only,) = publisher.queue
    run(coord.on_completion(done(only)))
    run(coord.on_completion(done(only)))
    assert store.state.jobs[JOB].tasks_done == 1
