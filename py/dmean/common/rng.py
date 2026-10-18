"""Python port of api/src/generation/rng.ts, bit for bit.

Used by verification tooling and the end-to-end tests to regenerate a job's
inputs from its seed and check the result independently. Emulates JavaScript's
32-bit integer semantics (Math.imul, >>>, ^ on int32).
"""

from __future__ import annotations

from collections.abc import Callable

import numpy as np

_M32 = 0xFFFFFFFF


def _i32(x: int) -> int:
    x &= _M32
    return x - (1 << 32) if x & 0x80000000 else x


def _u32(x: int) -> int:
    return x & _M32


def _imul(a: int, b: int) -> int:
    return _i32(_u32(a) * _u32(b))


def mulberry32(seed: int) -> Callable[[], float]:
    a = _u32(seed)

    def nxt() -> float:
        nonlocal a
        a = _u32(a + 0x6D2B79F5)
        t = a
        t = _imul(_i32(t ^ (t >> 15)), _i32(t | 1))
        t = _i32(_i32(t) ^ _i32(t + _imul(_i32(t ^ (_u32(t) >> 7)), _i32(t | 61))))
        return _u32(t ^ (_u32(t) >> 14)) / 4294967296

    return nxt


def file_seed(job_seed: int, file_index: int) -> int:
    return _u32(_i32(job_seed) ^ _imul(file_index, 0x9E3779B1))


def generate_values(job_seed: int, file_index: int, c: int) -> np.ndarray:
    nxt = mulberry32(file_seed(job_seed, file_index))
    return np.array([nxt() for _ in range(c)], dtype=np.float64)


def expected_mean(job_seed: int, f: int, c: int) -> np.ndarray:
    """The answer a job must produce, computed independently of the pipeline."""
    total = np.zeros(c)
    for i in range(1, f + 1):
        total += generate_values(job_seed, i, c)
    return total / f
