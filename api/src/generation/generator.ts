import { Piscina } from 'piscina';
import type { RangeTask } from './generateRange.js';

export interface JobSpec {
  id: string;
  f: number;
  c: number;
  seed: number;
}

/** Generates a job's F files on a pool of worker threads (generation is CPU-bound). */
export class Generator {
  private readonly pool: Piscina;

  constructor(
    threads: number,
    private readonly storeRoot: string,
    private readonly rangeSize: number,
  ) {
    // Under tsx (development) this module is .ts; compiled, it is .js.
    const ext = import.meta.url.endsWith('.ts') ? '.ts' : '.js';
    this.pool = new Piscina({
      filename: new URL(`./generateRange${ext}`, import.meta.url).href,
      maxThreads: threads,
    });
  }

  async generate(job: JobSpec): Promise<void> {
    const runs: Promise<number>[] = [];
    for (let start = 1; start <= job.f; start += this.rangeSize) {
      const task: RangeTask = {
        jobId: job.id,
        start,
        end: Math.min(start + this.rangeSize - 1, job.f),
        c: job.c,
        seed: job.seed,
        storeRoot: this.storeRoot,
      };
      runs.push(this.pool.run(task));
    }
    await Promise.all(runs); // Piscina queues ranges beyond maxThreads
  }

  close(): Promise<void> {
    return this.pool.destroy();
  }
}
