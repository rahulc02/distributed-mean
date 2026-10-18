import type pg from 'pg';

export type EventSource = 'api' | 'generator' | 'coordinator' | 'supervisor';

export interface EventView {
  id: number;
  at: string;
  source: EventSource;
  jobId: string | null;
  message: string;
}

export class EventRepo {
  constructor(private readonly db: pg.Pool) {}

  async log(source: EventSource, jobId: string | null, message: string): Promise<void> {
    await this.db.query('INSERT INTO events (source, job_id, message) VALUES ($1, $2, $3)', [source, jobId, message]);
  }

  /** Events after `afterId`, oldest first. */
  async since(afterId: number, limit: number): Promise<EventView[]> {
    const { rows } = await this.db.query(
      `SELECT id, at, source, job_id::text AS job_id, message FROM events
        WHERE id > $1 ORDER BY id LIMIT $2`,
      [afterId, limit],
    );
    return rows.map((r) => ({ id: Number(r.id), at: r.at.toISOString(), source: r.source, jobId: r.job_id, message: r.message }));
  }

  async lastId(): Promise<number> {
    const { rows } = await this.db.query<{ id: string | null }>('SELECT max(id) AS id FROM events');
    return Number(rows[0]?.id ?? 0);
  }
}
