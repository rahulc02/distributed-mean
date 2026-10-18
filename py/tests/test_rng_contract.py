import json
from pathlib import Path

from dmean.common.rng import generate_values

FIXTURE = Path(__file__).resolve().parents[2] / "contracts" / "fixtures" / "rng_v1.json"


def test_python_generator_matches_typescript_bit_for_bit():
    for case in json.loads(FIXTURE.read_text()):
        values = generate_values(case["seed"], case["file"], case["c"])
        assert values.tolist() == case["values"], case
