import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { performance } from 'node:perf_hooks';
import { collectChannel, resultChunks, ChannelInbox } from './channel.mjs';
import { Journal } from './journal.mjs';
import { inputFixture, youtubeFixture } from './fixture.mjs';

const directory = await mkdtemp(join(tmpdir(), 'channel-autonomy-measure-'));
try {
  const input = inputFixture();
  const events = [];
  const node = await new Journal(join(directory, 'node')).init();
  const center = new ChannelInbox({ journal: await new Journal(join(directory, 'center')).init(),
    assertOwner: manifest => { if (manifest.generation !== input.generation) throw new Error('STALE_GENERATION'); } });
  const entries = Array.from({ length: 30 }, (_, n) => ({ id: `new-${n}`, position: n + 1 }));
  entries.push({ id: 'stored', position: 31 });
  const start = performance.now();
  const result = await collectChannel({ input, journal: node, youtube: youtubeFixture(events, { entries }) });
  const collected = performance.now();
  const { manifest, chunks } = resultChunks(result);
  let receipt;
  for (const chunk of chunks) receipt = await center.receive(manifest, chunk);
  const received = performance.now();
  console.log(JSON.stringify({ kind: 'isolated_fixture_measurement', real_youtube_requests: 0,
    detail_fixture_calls: events.filter(event => event[0] === 'detail').length,
    targets: result.items.length, center_calls_during_collection: 0,
    result_chunk_submissions: chunks.length, payload_bytes: manifest.bytes,
    local_journal_records: node.records.size, local_journal_bytes: node.bytes,
    collect_and_fsync_ms: +(collected - start).toFixed(2),
    chunk_and_receive_fsync_ms: +(received - collected).toFixed(2),
    receipt_state: receipt.state, plan_finalized: false,
    limitation: 'Fixture calls return immediately; not production throughput or a network benchmark.' }, null, 2));
} finally { await rm(directory, { recursive: true, force: true }); }
