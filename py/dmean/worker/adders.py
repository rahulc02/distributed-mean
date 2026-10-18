"""Summing strategies. Both are deterministic: retries produce identical bytes.

PairwiseAdder: up to 5 inputs fetched concurrently, combined in a fixed pairing
in place, so memory never exceeds the 5 fetched buffers.

StreamingAdder: any number of inputs, at most `window` fetches in flight, added
in task order into one accumulator (accumulator + 4 in flight = 5 buffers).
"""

from __future__ import annotations

from collections import deque
from collections.abc import Callable, Sequence
from concurrent.futures import Executor
from itertools import islice
from typing import Protocol, TypeVar

import numpy as np

R = TypeVar("R")
Loader = Callable[[R], tuple[np.ndarray, int]]

MAX_IN_MEMORY = 5


class Adder(Protocol):
    def sum(self, refs: Sequence[R], load: Loader) -> tuple[np.ndarray, int]: ...


class PairwiseAdder:
    def __init__(self, pool: Executor) -> None:
        self.pool = pool

    def sum(self, refs, load):
        if not 1 <= len(refs) <= MAX_IN_MEMORY:
            raise ValueError(f"pairwise adder takes 1..{MAX_IN_MEMORY} inputs, got {len(refs)}")
        loaded = list(self.pool.map(load, refs))         # concurrent fetch, input order kept
        bufs = [values for values, _ in loaded]
        while len(bufs) > 1:                              # (1+2), (3+4), 5 -> (12+34), 5 -> (1234+5)
            pairs = [(bufs[i], bufs[i + 1]) for i in range(0, len(bufs) - 1, 2)]
            for a, b in pairs:
                np.add(a, b, out=a)                       # in place; b is released
            bufs = [a for a, _ in pairs] + (bufs[-1:] if len(bufs) % 2 else [])
        return bufs[0], sum(count for _, count in loaded)


class StreamingAdder:
    def __init__(self, pool: Executor, window: int = MAX_IN_MEMORY - 1) -> None:
        self.pool, self.window = pool, window

    def sum(self, refs, load):
        it = iter(refs)
        pending = deque(self.pool.submit(load, r) for r in islice(it, self.window))
        if not pending:
            raise ValueError("a task needs at least one input")
        acc, n = None, 0
        while pending:
            values, count = pending.popleft().result()
            acc = values if acc is None else np.add(acc, values, out=acc)
            n += count
            nxt = next(it, None)
            if nxt is not None:
                pending.append(self.pool.submit(load, nxt))
        return acc, n


def adder_for(n_inputs: int, pool: Executor) -> Adder:
    return PairwiseAdder(pool) if n_inputs <= MAX_IN_MEMORY else StreamingAdder(pool)
