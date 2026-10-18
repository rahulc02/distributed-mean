import type { EventRepo } from '../repos/eventRepo.js';
import type { JobRepo, JobView } from '../repos/jobRepo.js';
import type { WorkerService } from '../services/workerService.js';
import type { Hub } from './hub.js';
import type { RabbitAdmin } from './rabbitAdmin.js';

export interface JobRow extends JobView {
  progress: number; // operands read / total, 0..1
  etaSeconds: number | null;
}

/** Progress and ETA from exact totals: elapsed time scaled by work remaining. */
export function withProgress(job: JobView, now = Date.now()): JobRow {
  const total = job.totalOperands ?? 0;
  const done = job.operandsDone;
  const progress = job.status === 'DONE' ? 1 : total > 0 ? done / total : 0;
  let etaSeconds: number | null = null;
  if (job.status === 'RUNNING' && job.startedAt && done > 0 && total > done) {
    const elapsed = (now - Date.parse(job.startedAt)) / 1000;
    etaSeconds = Math.round((elapsed * (total - done)) / done);
  }
  return { ...job, progress, etaSeconds };
}

/**
 * One poll loop for every viewer: Postgres every `dbMs`, RabbitMQ every
 * `rabbitMs`. A hundred open dashboards cost the same as one.
 */
export class Poller {
  private timers: NodeJS.Timeout[] = [];
  private lastEventId = 0;
  private queueStats: unknown = null;

  constructor(
    private readonly jobs: JobRepo,
    private readonly events: EventRepo,
    private readonly workers: WorkerService,
    private readonly rabbit: RabbitAdmin,
    private readonly hub: Hub,
    private readonly dbMs = 2000,
    private readonly rabbitMs = 5000,
  ) {}

  async start(): Promise<void> {
    this.lastEventId = Math.max(0, (await this.events.lastId()) - 50); // seed the log with recent history
    await Promise.allSettled([this.pollDb(), this.pollRabbit()]);
    this.timers.push(setInterval(() => void this.pollDb(), this.dbMs));
    this.timers.push(setInterval(() => void this.pollRabbit(), this.rabbitMs));
  }

  stop(): void {
    for (const t of this.timers) clearInterval(t);
    this.timers = [];
  }

  private async pollDb(): Promise<void> {
    try {
      const [counts, recent, workers, newEvents] = await Promise.all([
        this.jobs.countsByStatus(),
        this.jobs.recent(20),
        this.workers.view(),
        this.events.since(this.lastEventId, 200),
      ]);
      this.hub.publish('jobs', recent.map((j) => withProgress(j)));
      this.hub.publish('system', { jobs: counts, workers, queue: this.queueStats });
      for (const e of newEvents) {
        this.hub.append(e);
        this.lastEventId = e.id;
      }
    } catch (err) {
      console.error(JSON.stringify({ level: 'ERROR', service: 'api', msg: 'dashboard poll failed', err: String(err) }));
    }
  }

  private async pollRabbit(): Promise<void> {
    try {
      const [tasks, completions, dlq] = await Promise.all([
        this.rabbit.queue('tasks'),
        this.rabbit.queue('completions'),
        this.rabbit.queue('tasks.dlq'),
      ]);
      this.queueStats = { tasks, completions, deadLettered: dlq?.waiting ?? 0 };
    } catch {
      this.queueStats = null; // shown as unavailable; never blocks the database view
    }
  }
}
