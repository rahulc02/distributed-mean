/**
 * Fan-out to every open dashboard. Snapshot topics keep only their latest
 * value and publish only on change; the log topic keeps a short history so a
 * new viewer sees recent events immediately.
 */
export type Send = (topic: string, data: unknown) => void;

export class Hub {
  private readonly subscribers = new Set<Send>();
  private readonly latest = new Map<string, string>();
  private readonly history: unknown[] = [];

  constructor(private readonly historySize = 50) {}

  /** Replays current state, then streams changes. Returns an unsubscribe function. */
  subscribe(send: Send): () => void {
    for (const [topic, json] of this.latest) send(topic, JSON.parse(json));
    for (const entry of this.history) send('log', entry);
    this.subscribers.add(send);
    return () => this.subscribers.delete(send);
  }

  /** Publishes a snapshot if it differs from the last one. Returns whether it did. */
  publish(topic: string, data: unknown): boolean {
    const json = JSON.stringify(data);
    if (this.latest.get(topic) === json) return false;
    this.latest.set(topic, json);
    this.broadcast(topic, data);
    return true;
  }

  /** Appends to the log, keeping the most recent `historySize` entries. */
  append(entry: unknown): void {
    this.history.push(entry);
    if (this.history.length > this.historySize) this.history.shift();
    this.broadcast('log', entry);
  }

  get size(): number {
    return this.subscribers.size;
  }

  private broadcast(topic: string, data: unknown): void {
    for (const send of this.subscribers) {
      try {
        send(topic, data);
      } catch {
        this.subscribers.delete(send); // a broken connection never blocks the others
      }
    }
  }
}
