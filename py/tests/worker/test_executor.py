from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

import numpy as np
import pytest

from dmean.common.operand import CorruptOperand, decode, encode
from dmean.common.storage import LocalStore
from dmean.worker.executor import CountMismatch, run_task

POOL = ThreadPoolExecutor(max_workers=5)


@dataclass
class Ref:
    key: str
    count: int


@dataclass
class FakeTask:
    task_id: str
    final: bool
    f: int
    c: int
    output_key: str
    inputs: list


def write_files(store, n):
    for i in range(1, n + 1):
        store.put(f"files/J/f{i}", encode(np.array([i, 10.0 * i]), 1))


def test_a_leaf_writes_a_partial_sum(tmp_path):
    store = LocalStore(tmp_path)
    write_files(store, 5)
    task = FakeTask("J:1:0", False, 12, 2, "partials/J/J:1:0", [Ref(f"files/J/f{i}", 1) for i in range(1, 6)])
    result = run_task(task, store, POOL)
    values, count = decode(store.get(result.output_key), 2)
    assert values.tolist() == [15.0, 150.0] and count == 5 == result.count


def test_the_final_task_writes_the_mean_as_csv(tmp_path):
    store = LocalStore(tmp_path)
    store.put("partials/J/a", encode(np.array([15.0, 150.0]), 5))
    store.put("partials/J/b", encode(np.array([40.0, 400.0]), 5))
    store.put("partials/J/c", encode(np.array([23.0, 230.0]), 2))
    task = FakeTask("J:2:x", True, 12, 2, "results/J.csv",
                    [Ref("partials/J/a", 5), Ref("partials/J/b", 5), Ref("partials/J/c", 2)])
    run_task(task, store, POOL)
    assert store.get("results/J.csv").decode() == "6.5,65\n"


def test_a_count_that_disagrees_with_the_message_is_permanent(tmp_path):
    store = LocalStore(tmp_path)
    store.put("partials/J/a", encode(np.array([1.0, 1.0]), 3))
    task = FakeTask("J:2:x", False, 12, 2, "partials/J/out", [Ref("partials/J/a", 4)])
    with pytest.raises(CountMismatch):
        run_task(task, store, POOL)


def test_corrupt_input_is_permanent(tmp_path):
    store = LocalStore(tmp_path)
    store.put("files/J/f1", b"not an operand at all, definitely")
    task = FakeTask("J:1:0", False, 12, 2, "partials/J/out", [Ref("files/J/f1", 1)])
    with pytest.raises(CorruptOperand):
        run_task(task, store, POOL)


def test_rerunning_a_task_writes_identical_bytes(tmp_path):
    store = LocalStore(tmp_path)
    write_files(store, 5)
    task = FakeTask("J:1:0", False, 12, 2, "partials/J/J:1:0", [Ref(f"files/J/f{i}", 1) for i in range(1, 6)])
    run_task(task, store, POOL)
    first = bytes(store.get(task.output_key))
    run_task(task, store, POOL)
    assert bytes(store.get(task.output_key)) == first


def test_store_rejects_keys_that_escape_the_root(tmp_path):
    with pytest.raises(ValueError):
        LocalStore(tmp_path).put("../outside", b"x")
