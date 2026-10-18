/**
 * Operand format v1: the one contract shared across languages.
 * Spec: contracts/operand-format.md. Python twin: py/dmean/common/operand.py.
 *
 *   offset 0   4 bytes  magic   "DMOP"
 *   offset 4   2 bytes  version 1
 *   offset 6   2 bytes  flags   0
 *   offset 8   8 bytes  count   input files covered
 *   offset 16  8 bytes  c       number of values
 *   offset 24  8*c      values  float64 sums, little-endian
 */

export const MAGIC = 'DMOP';
export const VERSION = 1;
export const HEADER_BYTES = 24;

export class CorruptOperand extends Error {
  override readonly name = 'CorruptOperand';
}

export interface Operand {
  count: bigint;
  values: Float64Array;
}

/** Encodes a sum vector and the number of input files it covers. */
export function encodeOperand(count: bigint, values: Float64Array): Buffer {
  if (count <= 0n) throw new RangeError(`count must be positive, got ${count}`);
  const buf = Buffer.allocUnsafe(HEADER_BYTES + values.length * 8);
  buf.write(MAGIC, 0, 'ascii');
  buf.writeUInt16LE(VERSION, 4);
  buf.writeUInt16LE(0, 6);
  buf.writeBigUInt64LE(count, 8);
  buf.writeBigUInt64LE(BigInt(values.length), 16);
  // Write each value explicitly little-endian, so the bytes are correct on any CPU.
  for (let i = 0; i < values.length; i++) {
    buf.writeDoubleLE(values[i]!, HEADER_BYTES + i * 8);
  }
  return buf;
}

/** Decodes and validates an operand; `expectedC` is the job's C. */
export function decodeOperand(buf: Buffer, expectedC: number): Operand {
  if (buf.length < HEADER_BYTES) throw new CorruptOperand(`operand too short: ${buf.length} bytes`);
  const magic = buf.toString('ascii', 0, 4);
  const version = buf.readUInt16LE(4);
  const count = buf.readBigUInt64LE(8);
  const c = buf.readBigUInt64LE(16);
  if (magic !== MAGIC) throw new CorruptOperand(`bad magic ${JSON.stringify(magic)}`);
  if (version !== VERSION) throw new CorruptOperand(`unsupported version ${version}`);
  if (c !== BigInt(expectedC)) throw new CorruptOperand(`c is ${c}, expected ${expectedC}`);
  if (count <= 0n) throw new CorruptOperand(`count must be positive, got ${count}`);
  if (buf.length !== HEADER_BYTES + expectedC * 8) {
    throw new CorruptOperand(`length ${buf.length}, expected ${HEADER_BYTES + expectedC * 8}`);
  }
  const values = new Float64Array(expectedC);
  for (let i = 0; i < expectedC; i++) values[i] = buf.readDoubleLE(HEADER_BYTES + i * 8);
  return { count, values };
}
