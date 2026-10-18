/**
 * Deterministic random numbers. Math.random cannot be seeded, and generation
 * must be repeatable: a crash mid-generation is recovered by regenerating, and
 * identical seeds must give identical files.
 */

/** mulberry32: a small, fast 32-bit generator returning values in [0, 1). */
export function mulberry32(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

/** One independent stream per file, derived from the job's seed. */
export function fileSeed(jobSeed: number, fileIndex: number): number {
  return (jobSeed ^ Math.imul(fileIndex, 0x9e3779b1)) >>> 0;
}
