import pytest

from dmean.coordinator import keys
from dmean.coordinator.model import Job
from dmean.coordinator.planning import (
    leaf_count, leaf_size, leaf_tasks, merge_count, total_operands, total_tasks,
)

JOB = "6f1c2a8e-3d4b-4c5a-9e7f-1a2b3c4d5e6f"


@pytest.mark.parametrize(
    "case",
    [(1, 5, 0), (3, 5, 1), (5, 5, 1), (6, 5, 2), (20_000, 5, 5_000), (100, 10, 11)],
)
def test_merge_count_matches_the_design_table(case):
    leaves, k, merges = case
    assert merge_count(leaves, k) == merges


def test_totals_at_full_scale():
    assert leaf_count(100_000, 5) == 20_000
    assert total_tasks(100_000, 5, 5) == 25_000
    assert total_operands(100_000, 5, 5) == 100_000 + 20_000 + 5_000 - 1


def test_leaf_tasks_cover_every_file_once_with_a_short_last_leaf():
    tasks = leaf_tasks(Job(JOB, f=12, c=2, k_leaf=5, k_merge=5))
    assert [len(t.inputs) for t in tasks] == [5, 5, 2]
    assert [i.id for t in tasks for i in t.inputs] == [f"f{i}" for i in range(1, 13)]
    assert [leaf_size(12, 5, g) for g in range(3)] == [5, 5, 2]
    assert not any(t.final for t in tasks)
    assert tasks[2].output_key == keys.partial_key(JOB, f"{JOB}:1:2")


def test_a_job_that_fits_in_one_leaf_writes_the_result_directly():
    (task,) = leaf_tasks(Job(JOB, f=4, c=2, k_leaf=5, k_merge=5))
    assert task.final and task.output_key == keys.result_key(JOB)


def test_task_ids_encode_their_level():
    assert keys.task_level(keys.leaf_task_id(JOB, 7)) == 1
    assert keys.leaf_index(keys.leaf_task_id(JOB, 7)) == 7
    assert keys.task_level(keys.merge_task_id(JOB, 3)) == 3
