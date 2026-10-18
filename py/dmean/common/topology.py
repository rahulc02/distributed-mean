"""RabbitMQ topology, declared idempotently by the coordinator at startup.

Kept library-agnostic (plain names and arguments) so the coordinator (aio-pika)
and tests can share one definition.
"""

from __future__ import annotations

from dataclasses import dataclass, field

TASKS = "tasks"
COMPLETIONS = "completions"
TASKS_DLQ = "tasks.dlq"

# Corrupt inputs are rejected straight to the DLQ; transient failures are
# requeued until the quorum queue's delivery limit dead-letters them.
DELIVERY_LIMIT = 5


@dataclass(frozen=True)
class QueueSpec:
    name: str
    arguments: dict[str, object] = field(default_factory=dict)


QUEUES: tuple[QueueSpec, ...] = (
    QueueSpec(TASKS_DLQ, {"x-queue-type": "quorum"}),
    QueueSpec(
        TASKS,
        {
            "x-queue-type": "quorum",
            "x-delivery-limit": DELIVERY_LIMIT,
            "x-dead-letter-exchange": "",
            "x-dead-letter-routing-key": TASKS_DLQ,
        },
    ),
    QueueSpec(COMPLETIONS, {"x-queue-type": "quorum"}),
)
