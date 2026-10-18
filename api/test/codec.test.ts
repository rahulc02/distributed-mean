import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { CorruptOperand, decodeOperand, encodeOperand, HEADER_BYTES } from '../src/operand/codec.js';

const FIXTURE = fileURLToPath(new URL('../../contracts/fixtures/operand_v1.bin', import.meta.url));
const VALUES = [1.5, -2.25, 0, -0, 5e-324, 1.7976931348623157e308, 0.1];

test('header layout matches the spec', () => {
  const buf = encodeOperand(7n, Float64Array.from([1, 2]));
  assert.equal(buf.length, HEADER_BYTES + 16);
  assert.equal(buf.toString('ascii', 0, 4), 'DMOP');
  assert.equal(buf.readUInt16LE(4), 1);
  assert.equal(buf.readUInt16LE(6), 0);
  assert.equal(buf.readBigUInt64LE(8), 7n);
  assert.equal(buf.readBigUInt64LE(16), 2n);
  assert.equal(buf.readDoubleLE(24), 1);
  assert.equal(buf.readDoubleLE(32), 2);
});

test('round trip preserves every bit, including -0 and subnormals', () => {
  const { count, values } = decodeOperand(encodeOperand(3n, Float64Array.from(VALUES)), VALUES.length);
  assert.equal(count, 3n);
  assert.deepEqual(Array.from(values), VALUES);
  assert.ok(Object.is(values[3], -0));
});

test('encoding matches the committed cross-language fixture byte for byte', () => {
  const expected = readFileSync(FIXTURE);
  assert.deepEqual(encodeOperand(3n, Float64Array.from(VALUES)), expected);
});

test('rejects wrong magic, version, c and length', () => {
  const good = encodeOperand(1n, Float64Array.from([1, 2, 3]));
  const corrupt = (mutate: (b: Buffer) => Buffer) => () => decodeOperand(mutate(Buffer.from(good)), 3);

  assert.throws(corrupt((b) => { b.write('XXXX', 0, 'ascii'); return b; }), CorruptOperand);
  assert.throws(corrupt((b) => { b.writeUInt16LE(2, 4); return b; }), CorruptOperand);
  assert.throws(() => decodeOperand(good, 4), CorruptOperand);
  assert.throws(corrupt((b) => b.subarray(0, b.length - 8)), CorruptOperand);
  assert.throws(corrupt((b) => { b.writeBigUInt64LE(0n, 8); return b; }), CorruptOperand);
});

test('refuses to encode a non-positive count', () => {
  assert.throws(() => encodeOperand(0n, new Float64Array(1)), RangeError);
});
