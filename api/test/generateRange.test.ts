import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, readFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import generateRange, { fileKey, generateValues } from '../src/generation/generateRange.js';
import { decodeOperand } from '../src/operand/codec.js';

test('writes one operand per file with count 1 and the seeded values', async () => {
  const root = await mkdtemp(join(tmpdir(), 'gen-'));
  const written = await generateRange({ jobId: 'J', start: 3, end: 7, c: 10, seed: 5, storeRoot: root });
  assert.equal(written, 5);
  for (let i = 3; i <= 7; i++) {
    const { count, values } = decodeOperand(await readFile(join(root, fileKey('J', i))), 10);
    assert.equal(count, 1n);
    assert.deepEqual(Array.from(values), Array.from(generateValues(5, i, 10)));
  }
});

test('regenerating a range writes identical bytes', async () => {
  const root = await mkdtemp(join(tmpdir(), 'gen-'));
  const task = { jobId: 'J', start: 1, end: 2, c: 1000, seed: 9, storeRoot: root };
  await generateRange(task);
  const first = await readFile(join(root, fileKey('J', 1)));
  await generateRange(task);
  assert.deepEqual(await readFile(join(root, fileKey('J', 1))), first);
});
