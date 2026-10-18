import pytest

pydantic = pytest.importorskip("pydantic")

from dmean.common.messages import CompletionMessage, OperandRef, TaskMessage  # noqa: E402

JOB = "6f1c2a8e-3d4b-4c5a-9e7f-1a2b3c4d5e6f"


def task(**overrides):
    fields = dict(
        job_id=JOB, task_id=f"{JOB}:1:0", level=1, final=False, f=12, c=3,
        inputs=[OperandRef(id="f1", key=f"files/{JOB}/f1", count=1)],
        output_key=f"partials/{JOB}/{JOB}:1:0",
    )
    return TaskMessage(**(fields | overrides))


def test_task_round_trips_through_json():
    original = task()
    assert TaskMessage.model_validate_json(original.model_dump_json()) == original


def test_unknown_fields_are_rejected():
    with pytest.raises(pydantic.ValidationError):
        TaskMessage.model_validate({**task().model_dump(mode="json"), "surprise": 1})


def test_a_task_needs_at_least_one_input():
    with pytest.raises(pydantic.ValidationError):
        task(inputs=[])


def test_completion_count_must_be_positive():
    with pytest.raises(pydantic.ValidationError):
        CompletionMessage(job_id=JOB, task_id="t", output_key="k", count=0, duration_ms=1, worker_id="w")
