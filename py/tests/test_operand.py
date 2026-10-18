import math
from pathlib import Path

import numpy as np
import pytest

from dmean.common.operand import HEADER, CorruptOperand, decode, encode, to_csv

FIXTURE = Path(__file__).resolve().parents[2] / "contracts" / "fixtures" / "operand_v1.bin"
VALUES = [1.5, -2.25, 0.0, -0.0, 5e-324, 1.7976931348623157e308, 0.1]


def test_header_is_24_bytes():
    assert HEADER.size == 24


def test_round_trip_preserves_every_bit():
    values, count = decode(bytearray(encode(np.array(VALUES), 3)), len(VALUES))
    assert count == 3
    assert values.tobytes() == np.array(VALUES, dtype="<f8").tobytes()
    assert math.copysign(1.0, values[3]) == -1.0  # negative zero survives


def test_decodes_the_typescript_fixture():
    """Contract test: bytes written by api/src/operand/codec.ts."""
    values, count = decode(bytearray(FIXTURE.read_bytes()), len(VALUES))
    assert count == 3
    assert values.tobytes() == np.array(VALUES, dtype="<f8").tobytes()


def test_encodes_byte_identical_to_the_typescript_fixture():
    assert encode(np.array(VALUES), 3) == FIXTURE.read_bytes()


def test_decode_over_bytearray_is_writable_and_zero_copy():
    buf = bytearray(encode(np.array([1.0, 2.0]), 1))
    values, _ = decode(buf, 2)
    np.add(values, values, out=values)
    assert decode(buf, 2)[0].tolist() == [2.0, 4.0]  # the add landed in buf itself


@pytest.mark.parametrize(
    "mutate",
    [
        lambda b: b"XXXX" + b[4:],                          # magic
        lambda b: b[:4] + (2).to_bytes(2, "little") + b[6:],  # version
        lambda b: b[:-8],                                   # truncated
        lambda b: b[:8] + (0).to_bytes(8, "little") + b[16:],  # count 0
        lambda b: b[:10],                                   # shorter than a header
    ],
)
def test_rejects_corrupt_operands(mutate):
    good = encode(np.array([1.0, 2.0, 3.0]), 1)
    with pytest.raises(CorruptOperand):
        decode(bytearray(mutate(good)), 3)


def test_rejects_wrong_c():
    with pytest.raises(CorruptOperand):
        decode(bytearray(encode(np.array([1.0, 2.0, 3.0]), 1)), 4)


def test_csv_is_one_full_precision_line():
    assert to_csv(np.array([2.5, 3.5, 0.1])) == b"2.5,3.5,0.10000000000000001\n"
