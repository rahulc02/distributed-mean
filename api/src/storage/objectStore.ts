import type { Readable } from 'node:stream';

/** Object storage port. Keys look like 'files/{job}/f17'. */
export interface ObjectStore {
  put(key: string, data: Buffer): Promise<void>;
  stream(key: string): Readable;
  exists(key: string): Promise<boolean>;
  deletePrefix(prefix: string): Promise<void>;
}
