"""Operand format v1, the one contract shared across languages.

Spec: contracts/operand-format.md. TypeScript twin: api/src/operand/codec.ts.

An operand is a sum vector plus the number of input files it covers. Input
files have count 1. Partials store sums, never means, so combining operands is
plain addition and the counts carry the weighting.
"""

from __future__ import annotations

import io
import struct

import numpy as np

MAGIC = b"DMOP"
VERSION = 1
HEADER = struct.Struct("<4sHHQQ")  # magic, version, flags, count, c -> 24 bytes
DTYPE = np.dtype("<f8")


class CorruptOperand(ValueError):
    """The bytes are not a valid operand for this job; never retry, dead-letter."""


def decode(buf: bytes | bytearray | memoryview, expected_c: int) -> tuple[np.ndarray, int]:
    """Validate and decode an operand.

    Returns (values, count). Over a ``bytearray`` the array is a zero-copy,
    writable view, so callers can add into it in place.
    """
    if len(buf) < HEADER.size:
        raise CorruptOperand(f"operand too short: {len(buf)} bytes")
    magic, version, _flags, count, c = HEADER.unpack_from(buf)
    if magic != MAGIC:
        raise CorruptOperand(f"bad magic {magic!r}")
    if version != VERSION:
        raise CorruptOperand(f"unsupported version {version}")
    if c != expected_c:
        raise CorruptOperand(f"c is {c}, expected {expected_c}")
    if count <= 0:
        raise CorruptOperand(f"count must be positive, got {count}")
    if len(buf) != HEADER.size + DTYPE.itemsize * c:
        raise CorruptOperand(f"length {len(buf)}, expected {HEADER.size + DTYPE.itemsize * c}")
    values = np.frombuffer(buf, dtype=DTYPE, count=c, offset=HEADER.size)
    return values, count


def encode(values: np.ndarray, count: int) -> bytes:
    """Encode a sum vector and the number of input files it covers."""
    if count <= 0:
        raise ValueError(f"count must be positive, got {count}")
    if values.ndim != 1:
        raise ValueError(f"values must be one-dimensional, got shape {values.shape}")
    header = HEADER.pack(MAGIC, VERSION, 0, count, values.size)
    return header + np.ascontiguousarray(values, dtype=DTYPE).tobytes()


def to_csv(means: np.ndarray) -> bytes:
    """The job's answer: one line of C comma-separated means, full precision."""
    out = io.StringIO()
    np.savetxt(out, means.reshape(1, -1), delimiter=",", fmt="%.17g")
    return out.getvalue().encode("ascii")
