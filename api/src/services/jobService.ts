import { randomInt } from 'node:crypto';
import type { Readable } from 'node:stream';
import { ConflictError, NotFoundError } from '../http/errors.js';
import type { Generator, JobSpec } from '../generation/generator.js';
import type { EventRepo } from '../repos/eventRepo.js';
import type { JobRepo, JobView } from '../repos/jobRepo.js';
import type { ObjectStore } from '../storage/objectStore.js';

export interface CreateJobInput {
  f: number;
  c: number;
}

/**
 * Job lifecycle on the API side: create, generate, hand off (GENERATING -> NEW).
 * Everything after NEW belongs to the coordinator.
 */
export class JobService {
  private readonly running = new Set<string>();

  constructor(
    private readonly jobs: JobRepo,
    private readonly events: EventRepo,
    private readonly generator: Generator,
    private readonly store: ObjectStore,
  ) {}

  async create(input: CreateJobInput): Promise<string> {
    const seed = randomInt(0, 2 ** 32);
    const id = await this.jobs.create(input.f, input.c, seed);
    await this.events.log('api', id, `job created: F=${input.f}, C=${input.c}`);
    void this.runGeneration({ id, f: input.f, c: input.c, seed }); // respond now; generate in the background
    return id;
  }

  async get(id: string): Promise<JobView> {
    const job = await this.jobs.get(id);
    if (!job) throw new NotFoundError(`job ${id} not found`);
    return job;
  }

  async result(id: string): Promise<Readable> {
    const job = await this.get(id);
    if (job.status !== 'DONE' || !job.resultKey) throw new ConflictError(`job ${id} is ${job.status}`);
    return this.store.stream(job.resultKey);
  }

  /** Startup: finish generation for jobs left mid-way. Seeds make regeneration identical. */
  async resumeGenerating(): Promise<void> {
    for (const job of await this.jobs.generating()) void this.runGeneration(job);
  }

  private async runGeneration(job: JobSpec): Promise<void> {
    if (this.running.has(job.id)) return;
    this.running.add(job.id);
    try {
      await this.events.log('generator', job.id, `generating ${job.f} files of ${job.c} values`);
      await this.generator.generate(job);
      if (await this.jobs.markGenerated(job.id)) {
        await this.events.log('generator', job.id, 'generation finished; job queued');
      }
    } catch (err) {
      const reason = `generation failed: ${err instanceof Error ? err.message : String(err)}`;
      await this.jobs.markFailed(job.id, reason);
      await this.events.log('generator', job.id, reason);
    } finally {
      this.running.delete(job.id);
    }
  }
}
