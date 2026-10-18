/** Reads queue statistics from RabbitMQ's management HTTP API. */
export interface QueueStats {
  name: string;
  waiting: number;
  inProgress: number;
  consumers: number;
  ackRate: number;
}

export class RabbitAdmin {
  private readonly auth: string;

  constructor(
    private readonly baseUrl: string,
    user: string,
    pass: string,
  ) {
    this.auth = 'Basic ' + Buffer.from(`${user}:${pass}`).toString('base64');
  }

  async queue(name: string): Promise<QueueStats | null> {
    const res = await fetch(`${this.baseUrl}/api/queues/%2F/${encodeURIComponent(name)}`, {
      headers: { authorization: this.auth },
      signal: AbortSignal.timeout(3000),
    });
    if (res.status === 404) return null; // not declared yet: the coordinator has not started
    if (!res.ok) throw new Error(`RabbitMQ management API: ${res.status}`);
    const q = (await res.json()) as Record<string, any>;
    return {
      name,
      waiting: q.messages_ready ?? 0,
      inProgress: q.messages_unacknowledged ?? 0,
      consumers: q.consumers ?? 0,
      ackRate: q.message_stats?.ack_details?.rate ?? 0,
    };
  }
}
