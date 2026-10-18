import { randomUUID } from 'node:crypto';
import { createReadStream } from 'node:fs';
import { access, mkdir, rename, rm, writeFile } from 'node:fs/promises';
import { dirname, join, resolve, sep } from 'node:path';
import type { Readable } from 'node:stream';
import type { ObjectStore } from './objectStore.js';

/** A directory as an object store. Writes are atomic: temp file, then rename. */
export class LocalStore implements ObjectStore {
  private readonly root: string;

  constructor(root: string) {
    this.root = resolve(root);
  }

  private path(key: string): string {
    const path = resolve(join(this.root, key));
    if (!path.startsWith(this.root + sep)) throw new Error(`key escapes the store root: ${key}`);
    return path;
  }

  async put(key: string, data: Buffer): Promise<void> {
    const path = this.path(key);
    await mkdir(dirname(path), { recursive: true });
    const tmp = join(dirname(path), `.${randomUUID()}.tmp`);
    await writeFile(tmp, data);
    await rename(tmp, path); // readers see the old object or the new one, never half
  }

  stream(key: string): Readable {
    return createReadStream(this.path(key));
  }

  async exists(key: string): Promise<boolean> {
    try {
      await access(this.path(key));
      return true;
    } catch {
      return false;
    }
  }

  async deletePrefix(prefix: string): Promise<void> {
    await rm(this.path(prefix), { recursive: true, force: true });
  }
}
