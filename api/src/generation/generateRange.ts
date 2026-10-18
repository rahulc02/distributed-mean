/**
 * Generates input files start..end of one job. Runs inside a worker thread
 * (see generator.ts) and is the single function generation option 2 would call
 * from a queue consumer instead. One file is in memory at a time.
 */
import { encodeOperand } from '../operand/codec.js';
import { LocalStore } from '../storage/localStore.js';
import { fileSeed, mulberry32 } from './rng.js';

export interface RangeTask {
  jobId: string;
  start: number; // first file index, 1-based, inclusive
  end: number; // last file index, inclusive
  c: number;
  seed: number;
  storeRoot: string;
}

export const fileKey = (jobId: string, i: number) => `files/${jobId}/f${i}`;

export function generateValues(seed: number, i: number, c: number): Float64Array {
  const next = mulberry32(fileSeed(seed, i));
  const values = new Float64Array(c);
  for (let j = 0; j < c; j++) values[j] = next();
  return values;
}

export default async function generateRange(task: RangeTask): Promise<number> {
  const store = new LocalStore(task.storeRoot);
  for (let i = task.start; i <= task.end; i++) {
    await store.put(fileKey(task.jobId, i), encodeOperand(1n, generateValues(task.seed, i, task.c)));
  }
  return task.end - task.start + 1;
}
