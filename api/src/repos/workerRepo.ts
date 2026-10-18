import type pg from 'pg';

export interface WorkerView {
  id: string;
  pid: number | null;
  status: 'STARTING' | 'RUNNING' | 'DRAINING' | 'EXITED';
  busy: boolean;
  startedAt: string;
  heartbeatAt: string;
}

export class WorkerRepo {
  constructor(private readonly db: pg.Pool) {}

  async desired(): Promise<number> {
    const { rows } = await this.db.query<{ desired_workers: number }>('SELECT desired_workers FROM config');
    return rows[0]!.desired_workers;
  }

  /** Writes the target and wakes the supervisor (LISTEN desired_workers) at once. */
  async setDesired(count: number): Promise<void> {
    const client = await this.db.connect();
    try {
      await client.query('BEGIN');
      await client.query('UPDATE config SET desired_workers = $1', [count]);
      await client.query("SELECT pg_notify('desired_workers', $1::text)", [String(count)]);
      await client.query('COMMIT'); // the notification is delivered on commit
    } catch (err) {
      await client.query('ROLLBACK');
      throw err;
    } finally {
      client.release();
    }
  }

  async live(): Promise<WorkerView[]> {
    const { rows } = await this.db.query(
      `SELECT id, pid, status, busy, started_at, heartbeat_at FROM workers
        WHERE status <> 'EXITED' ORDER BY started_at`,
    );
    return rows.map((r) => ({
      id: r.id,
      pid: r.pid,
      status: r.status,
      busy: r.busy,
      startedAt: r.started_at.toISOString(),
      heartbeatAt: r.heartbeat_at.toISOString(),
    }));
  }
}
