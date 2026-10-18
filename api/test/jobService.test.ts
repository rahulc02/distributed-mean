import { test } from 'node:test';
import assert from 'node:assert/strict';
import type { Generator } from '../src/generation/generator.js';
import type { EventRepo } from '../src/repos/eventRepo.js';
import type { JobRepo } from '../src/repos/jobRepo.js';
import { JobService } from '../src/services/jobService.js';
import type { ObjectStore } from '../src/storage/objectStore.js';

/** Like Piscina: generation in progress rejects when the pool is destroyed. */
class FakeGenerator {
  private abort: ((err: Error) => void) | null = null;
  generate(): Promise<void> {
    return new Promise((_resolve, reject) => {
      this.abort = reject;
    });
  }
  async close(): Promise<void> {
    this.abort?.(new Error('Terminating worker thread'));
  }
  fail(message: string): void {
    this.abort?.(new Error(message));
  }
}

function setup() {
  const failed: string[] = [];
  const jobs = {
    create: async () => 'J',
    markGenerated: async () => true,
    markFailed: async (id: string) => void failed.push(id),
  } as unknown as JobRepo;
  const events = { log: async () => {} } as unknown as EventRepo;
  const generator = new FakeGenerator();
  const service = new JobService(jobs, events, generator as unknown as Generator, {} as ObjectStore);
  return { service, generator, failed };
}

const settle = () => new Promise((r) => setImmediate(r));

test('shutting down mid-generation leaves the job GENERATING so restart resumes it', async () => {
  const { service, failed } = setup();
  await service.create({ f: 10, c: 2 });
  await settle();
  await service.close();
  await settle();
  assert.deepEqual(failed, []);
});

test('a real generation error still fails the job', async () => {
  const { service, generator, failed } = setup();
  await service.create({ f: 10, c: 2 });
  await settle();
  generator.fail('disk full');
  await settle();
  assert.deepEqual(failed, ['J']);
});
