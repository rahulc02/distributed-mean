import { test } from 'node:test';
import assert from 'node:assert/strict';
import { fileSeed, mulberry32 } from '../src/generation/rng.js';

test('same seed, same sequence', () => {
  const a = mulberry32(42);
  const b = mulberry32(42);
  for (let i = 0; i < 1000; i++) assert.equal(a(), b());
});

test('values stay in [0, 1)', () => {
  const next = mulberry32(7);
  for (let i = 0; i < 100_000; i++) {
    const v = next();
    assert.ok(v >= 0 && v < 1);
  }
});

test('files of one job get different streams', () => {
  const firsts = new Set(Array.from({ length: 1000 }, (_, i) => mulberry32(fileSeed(123, i + 1))()));
  assert.equal(firsts.size, 1000);
});

test('the mean of many draws is close to 0.5', () => {
  const next = mulberry32(99);
  let sum = 0;
  const n = 200_000;
  for (let i = 0; i < n; i++) sum += next();
  assert.ok(Math.abs(sum / n - 0.5) < 0.005);
});
