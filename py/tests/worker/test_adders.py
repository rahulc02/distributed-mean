from concurrent.futures import ThreadPoolExecutor

import numpy as np
import pytest

from dmean.worker.adders import PairwiseAdder, StreamingAdder, adder_for

POOL = ThreadPoolExecutor(max_workers=5)


def loader(values_by_ref):
    def load(ref):
        return values_by_ref[ref].copy(), 1
    return load


def test_pairwise_sums_and_counts():
    data = {i: np.array([i, 10.0 * i]) for i in range(1, 6)}
    total, count = PairwiseAdder(POOL).sum(list(data), loader(data))
    assert total.tolist() == [15.0, 150.0] and count == 5


def test_pairwise_is_bit_identical_across_runs():
    rng = np.random.default_rng(7)
    data = {i: rng.random(1000) for i in range(5)}
    first, _ = PairwiseAdder(POOL).sum(list(data), loader(data))
    for _ in range(20):
        again, _ = PairwiseAdder(POOL).sum(list(data), loader(data))
        assert again.tobytes() == first.tobytes()


def test_pairwise_rejects_more_than_five_inputs():
    with pytest.raises(ValueError):
        PairwiseAdder(POOL).sum(list(range(6)), loader({}))


class CountingExecutor:
    """Tracks how many fetched buffers exist that the adder has not folded in yet."""

    def __init__(self):
        self.outstanding = 0
        self.peak = 0

    def submit(self, fn, ref):
        self.outstanding += 1
        self.peak = max(self.peak, self.outstanding)
        value = fn(ref)
        executor = self

        class Done:
            def result(self):
                executor.outstanding -= 1
                return value

        return Done()


def test_streaming_matches_a_plain_sum_and_never_holds_more_than_five_buffers():
    data = {i: np.array([float(i)]) for i in range(1, 101)}
    pool = CountingExecutor()
    total, count = StreamingAdder(pool).sum(list(data), loader(data))
    assert total.tolist() == [5050.0] and count == 100
    assert pool.peak <= 4          # four fetches in flight + the accumulator = 5 buffers


def test_factory_picks_by_size():
    assert isinstance(adder_for(5, POOL), PairwiseAdder)
    assert isinstance(adder_for(6, POOL), StreamingAdder)
