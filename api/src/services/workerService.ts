import { BadRequestError } from '../http/errors.js';
import type { EventRepo } from '../repos/eventRepo.js';
import type { WorkerRepo, WorkerView } from '../repos/workerRepo.js';

export interface WorkersView {
  desired: number;
  starting: number;
  running: number;
  draining: number;
  busy: number;
  free: number;
  workers: WorkerView[];
}

/** W is desired state: the API records it, the supervisor makes it true. */
export class WorkerService {
  constructor(
    private readonly workers: WorkerRepo,
    private readonly events: EventRepo,
    private readonly maxWorkers: number,
  ) {}

  async setDesired(count: number): Promise<void> {
    if (count > this.maxWorkers) throw new BadRequestError(`count must be at most ${this.maxWorkers}`);
    await this.workers.setDesired(count);
    await this.events.log('api', null, `desired workers set to ${count}`);
  }

  async view(): Promise<WorkersView> {
    const [desired, workers] = await Promise.all([this.workers.desired(), this.workers.live()]);
    const running = workers.filter((w) => w.status === 'RUNNING');
    return {
      desired,
      starting: workers.filter((w) => w.status === 'STARTING').length,
      running: running.length,
      draining: workers.filter((w) => w.status === 'DRAINING').length,
      busy: running.filter((w) => w.busy).length,
      free: running.filter((w) => !w.busy).length,
      workers,
    };
  }
}
