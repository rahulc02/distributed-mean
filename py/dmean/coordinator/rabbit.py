"""RabbitMQ adapters: message conversion, the confirming publisher, topology."""

from __future__ import annotations

import aio_pika
from aio_pika.abc import AbstractChannel

from dmean.common.messages import CompletionMessage, OperandRef, TaskMessage
from dmean.common.topology import QUEUES, TASKS

from .model import Completion, Input, Task


def to_message(task: Task) -> TaskMessage:
    return TaskMessage(
        job_id=task.job_id, task_id=task.task_id, level=task.level, final=task.final,
        f=task.f, c=task.c, output_key=task.output_key,
        inputs=[OperandRef(id=i.id, key=i.key, count=i.count) for i in task.inputs],
    )


def task_from_message(m: TaskMessage) -> Task:
    return Task(
        job_id=str(m.job_id), task_id=m.task_id, level=m.level, final=m.final, f=m.f, c=m.c,
        inputs=tuple(Input(i.id, i.key, i.count) for i in m.inputs), output_key=m.output_key,
    )


def completion_from_message(m: CompletionMessage) -> Completion:
    return Completion(str(m.job_id), m.task_id, m.output_key, m.count, m.duration_ms, m.worker_id)


async def declare_topology(channel: AbstractChannel) -> None:
    """Idempotent: declaring an existing queue with the same arguments is a no-op."""
    for spec in QUEUES:
        await channel.declare_queue(spec.name, durable=True, arguments=dict(spec.arguments))


class RabbitPublisher:
    """Publishes tasks persistently; returns only once the broker confirms."""

    def __init__(self, channel: AbstractChannel) -> None:
        self.channel = channel  # opened with publisher_confirms=True

    async def publish(self, task: Task) -> None:
        message = aio_pika.Message(
            to_message(task).model_dump_json().encode(),
            content_type="application/json",
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            message_id=task.task_id,
        )
        await self.channel.default_exchange.publish(message, routing_key=TASKS)
