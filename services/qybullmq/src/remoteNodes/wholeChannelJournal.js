import { mkdir, open, readFile, truncate } from 'node:fs/promises';
import { join } from 'node:path';
import { createHash } from 'node:crypto';

export const digest = bytes => createHash('sha256').update(bytes).digest('hex');

// One executor owns each directory, as for RemoteResultSpool. Worker containers
// must never share a spool volume; RemoteChannelPlanExecutor serializes runs.
// Append + fsync avoids rewriting all earlier video results for every checkpoint.
export class Journal {
  constructor(directory, maxBytes = 64 * 1024 * 1024) {
    this.directory = directory;
    this.path = join(directory, 'journal.ndjson');
    this.maxBytes = maxBytes;
    this.records = new Map();
    this.tail = Promise.resolve();
  }

  async init() {
    await mkdir(this.directory, { recursive: true, mode: 0o700 });
    const fd = await open(this.path, 'a', 0o600);
    await fd.sync();
    await fd.close();
    const directory = await open(this.directory, 'r');
    try { await directory.sync(); } finally { await directory.close(); }
    let bytes = await readFile(this.path);
    if (bytes.length > this.maxBytes) throw new Error('JOURNAL_FULL');
    const end = bytes.lastIndexOf(10) + 1;
    if (end !== bytes.length) {
      // An interrupted append cannot have been acknowledged to the caller.
      await truncate(this.path, end);
      const repaired = await open(this.path, 'r+');
      try { await repaired.sync(); } finally { await repaired.close(); }
      bytes = bytes.subarray(0, end);
    }
    this.records.clear();
    for (const line of bytes.toString('utf8').split('\n').filter(Boolean)) {
      const row = JSON.parse(line);
      if (digest(JSON.stringify(row.value)) !== row.sha256 || this.records.has(row.key)) {
        throw new Error('JOURNAL_CORRUPT');
      }
      this.records.set(row.key, row);
    }
    this.bytes = bytes.length;
    return this;
  }

  get(key) { return this.records.get(key)?.value; }

  put(key, value) {
    const action = this.tail.then(async () => {
      const serialized = JSON.stringify(value);
      const sha256 = digest(serialized);
      const prior = this.records.get(key);
      if (prior) {
        if (prior.sha256 !== sha256) throw new Error('JOURNAL_IDENTITY_CONFLICT');
        return prior.value;
      }
      const row = { key, sha256, value: JSON.parse(serialized) };
      const bytes = Buffer.from(`${JSON.stringify(row)}\n`);
      if (this.bytes + bytes.length > this.maxBytes) throw new Error('JOURNAL_FULL');
      const fd = await open(this.path, 'a', 0o600);
      try { await fd.writeFile(bytes); await fd.sync(); } finally { await fd.close(); }
      this.bytes += bytes.length;
      this.records.set(key, row);
      return row.value;
    });
    // Fail closed after an I/O failure: a fresh init must reconcile a possibly
    // committed append before any further write can occur.
    this.tail = action;
    return action;
  }
}
