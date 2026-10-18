/**
 * Writes the cross-language fixtures:
 *   contracts/fixtures/operand_v1.bin  byte-exact operand both codecs must match
 *   contracts/fixtures/rng_v1.json     reference draws for the seeded generator,
 *                                      so Python tests can regenerate inputs exactly
 */
import { writeFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { generateValues } from '../src/generation/generateRange.js';
import { encodeOperand } from '../src/operand/codec.js';

export const FIXTURE_COUNT = 3n;
export const FIXTURE_VALUES = [1.5, -2.25, 0, -0, 5e-324, 1.7976931348623157e308, 0.1];

const dir = (name: string) => fileURLToPath(new URL(`../../contracts/fixtures/${name}`, import.meta.url));

writeFileSync(dir('operand_v1.bin'), encodeOperand(FIXTURE_COUNT, Float64Array.from(FIXTURE_VALUES)));

const cases = [
  { seed: 0, file: 1, c: 5 },
  { seed: 4294967295, file: 99999, c: 5 },
  { seed: 123456789, file: 42, c: 5 },
];
const rng = cases.map((k) => ({ ...k, values: Array.from(generateValues(k.seed, k.file, k.c)) }));
writeFileSync(dir('rng_v1.json'), JSON.stringify(rng, null, 2) + '\n');
console.log('wrote contracts/fixtures/operand_v1.bin and rng_v1.json');
