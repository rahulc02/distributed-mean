import type pg from 'pg';

export type JobStatus = 'GENERATING' | 'NEW' | 'RUNNING' | 'DONE' | 'FAILED';

export interface JobView {
  id: string;
  f: number;
  c: number;
  status: JobStatus;
  kLeaf: number | null;
  kMerge: number | null;
  tasksDone: number;
  totalTasks: number | null;
  operandsDone: number;
  totalOperands: number | null;
  resultKey: string | null;
  error: string | null;
  createdAt: string;
  genStartedAt: string | null;
  genFinishedAt: string | null;
  startedAt: string | null;
  finishedAt: string | null;
}

export interface GeneratingJob {
  id: string;
  f: number;
  c: number;
  seed: number;
}

const COLUMNS = `id::text, f, c, status, k_leaf, k_merge, tasks_done, total_tasks,
  operands_done, total_operands, result_key, error, created_at, gen_started_at,
  gen_finished_at, started_at, finished_at`;

const iso = (d: Date | null) => (d ? d.toISOString() : null);
const num = (v: string | number | null) => (v === null ? null : Number(v)); // int8 arrives as a string

function toView(r: Record<string, any>): JobView {
  return {
    id: r.id,
    f: r.f,
    c: r.c,
    status: r.status,
    kLeaf: r.k_leaf,
    kMerge: r.k_merge,
    tasksDone: r.tasks_done,
    totalTasks: r.total_tasks,
    operandsDone: Number(r.operands_done),
    totalOperands: num(r.total_operands),
    resultKey: r.result_key,
    error: r.error,
    createdAt: r.created_at.toISOString(),
    genStartedAt: iso(r.gen_started_at),
    genFinishedAt: iso(r.gen_finished_at),
    startedAt: iso(r.started_at),
    finishedAt: iso(r.finished_at),
  };
}

export class JobRepo {
  constructor(private readonly db: pg.Pool) {}

  async create(f: number, c: number, seed: number): Promise<string> {
    const { rows } = await this.db.query<{ id: string }>(
      `INSERT INTO jobs (f, c, seed, status, gen_started_at)
       VALUES ($1, $2, $3, 'GENERATING', now()) RETURNING id::text`,
      [f, c, seed],
    );
    return rows[0]!.id;
  }

  async get(id: string): Promise<JobView | null> {
    const { rows } = await this.db.query(`SELECT ${COLUMNS} FROM jobs WHERE id = $1`, [id]);
    return rows[0] ? toView(rows[0]) : null;
  }

  async recent(limit: number): Promise<JobView[]> {
    const { rows } = await this.db.query(`SELECT ${COLUMNS} FROM jobs ORDER BY created_at DESC LIMIT $1`, [limit]);
    return rows.map(toView);
  }

  async countsByStatus(): Promise<Record<JobStatus, number>> {
    const counts: Record<JobStatus, number> = { GENERATING: 0, NEW: 0, RUNNING: 0, DONE: 0, FAILED: 0 };
    const { rows } = await this.db.query<{ status: JobStatus; n: string }>(
      'SELECT status, count(*) AS n FROM jobs GROUP BY status',
    );
    for (const r of rows) counts[r.status] = Number(r.n);
    return counts;
  }

  async generating(): Promise<GeneratingJob[]> {
    const { rows } = await this.db.query(
      `SELECT id::text, f, c, seed FROM jobs WHERE status = 'GENERATING' ORDER BY created_at`,
    );
    return rows.map((r) => ({ id: r.id, f: r.f, c: r.c, seed: Number(r.seed) }));
  }

  /** GENERATING -> NEW: the hand-off to aggregation. Guarded, so it happens once. */
  async markGenerated(id: string): Promise<boolean> {
    const { rowCount } = await this.db.query(
      `UPDATE jobs SET status = 'NEW', gen_finished_at = now()
        WHERE id = $1 AND status = 'GENERATING'`,
      [id],
    );
    return rowCount === 1;
  }

  async markFailed(id: string, reason: string): Promise<void> {
    await this.db.query(
      `UPDATE jobs SET status = 'FAILED', error = $2, finished_at = now()
        WHERE id = $1 AND status IN ('GENERATING', 'NEW')`,
      [id, reason],
    );
  }
}
