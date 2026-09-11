import { mkdir, open, readdir, readFile, rename, stat, unlink } from 'node:fs/promises';
import { join } from 'node:path';
import { randomUUID } from 'node:crypto';
import { MAX_GZIP_BYTES } from './protocol.js';

// Single executor owns a directory. Never mount the same spool into two workers.
export class RemoteResultSpool {
  constructor({ directory, maxBytes = 16 * MAX_GZIP_BYTES }) {
    if (!Number.isSafeInteger(maxBytes) || maxBytes < MAX_GZIP_BYTES * 2) throw new TypeError('spool limit too small');
    Object.assign(this, { directory, maxBytes });
    this.pending = Promise.resolve();
  }

  // Network renewal and result upload share this one executor's directory.
  // Serialize mutations with space checks; separate owners remain forbidden.
  exclusive(action) {
    const result = this.pending.then(action);
    this.pending = result.catch(() => {});
    return result;
  }

  async init() { await mkdir(this.directory, { recursive: true, mode: 0o700 }); }

  async syncDirectory() {
    const directory = await open(this.directory, 'r');
    try { await directory.sync(); } finally { await directory.close(); }
  }

  async usageUnlocked() {
    const names = await readdir(this.directory);
    const sizes = await Promise.all(names.map(async (name) => (await stat(join(this.directory, name))).size));
    return sizes.reduce((total, size) => total + size, 0);
  }

  usage() { return this.exclusive(() => this.usageUnlocked()); }

  async writable() {
    return this.exclusive(async () => !(await readdir(this.directory)).some((name) => name.endsWith('.blocked'))
      && (await this.usageUnlocked()) + Math.ceil(MAX_GZIP_BYTES * 4 / 3) + 65536 <= this.maxBytes);
  }

  async save(name, bytes) {
    if (!['claim.json', 'pending.json', 'network.json', 'youtube-session.json'].includes(name)) throw new TypeError('invalid spool file');
    return this.exclusive(async () => {
      if ((await this.usageUnlocked()) + bytes.length > this.maxBytes) throw new Error('SPOOL_FULL');
      const temporary = join(this.directory, `${randomUUID()}.tmp`);
      const file = await open(temporary, 'wx', 0o600);
      try { await file.writeFile(bytes); await file.sync(); } finally { await file.close(); }
      await rename(temporary, join(this.directory, name));
      await this.syncDirectory();
    });
  }

  async read(name) {
    try { return JSON.parse(await readFile(join(this.directory, name), 'utf8')); }
    catch (error) { if (error.code === 'ENOENT') return null; throw error; }
  }

  async remove(name) {
    return this.exclusive(async () => {
      await unlink(join(this.directory, name)).catch((error) => { if (error.code !== 'ENOENT') throw error; });
      await this.syncDirectory();
    });
  }

  async block() {
    return this.exclusive(async () => {
      await rename(join(this.directory, 'pending.json'), join(this.directory, `${randomUUID()}.blocked`));
      await this.syncDirectory();
    });
  }

  async archiveStaleResult() {
    return this.exclusive(async () => {
      // Keep the exact bytes for inspection. They still count toward the disk
      // cap, but a conclusively expired execution must not disable this Worker.
      await rename(join(this.directory, 'pending.json'), join(this.directory, `${randomUUID()}.stale`));
      await this.syncDirectory();
    });
  }
}
