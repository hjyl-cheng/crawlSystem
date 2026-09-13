import { join } from 'node:path';
import { rm, readdir } from 'node:fs/promises';
import { Journal } from './wholeChannelJournal.js';
import { collectWholeChannel, interruptWholeChannel } from './wholeChannelCollector.js';
import { wholeChannelParts, decodeWholeChannelParts, validateWholeChannelPart } from './wholeChannelProtocol.js';
import { encodeResult, uuid, RemoteProtocolError } from './protocol.js';

const directory = (spool, commandId) => join(spool.directory, 'whole', uuid(commandId));

export async function flushWholeChannel({ client, spool }) {
  const pending = await spool.read('whole-pending.json');
  if (!pending) return false;
  if (client.transport !== 'nats' || typeof client.uploadWholeChannel !== 'function') throw new Error('WHOLE_CHANNEL_NATS_REQUIRED');
  const journal = await new Journal(directory(spool, pending.command_id), spool.maxBytes).init();
  const result = journal.get('result');
  if (!result) throw new Error('WHOLE_CHANNEL_JOURNAL_INCOMPLETE');
  const { manifest, parts } = wholeChannelParts(result);
  let receipt;
  for (const chunk of parts) {
    const bytes = await encodeResult({ version: 1, generation: pending.generation,
      batch_id: pending.command_id, command_id: pending.command_id, outcome: 'success',
      data: { input_sha256: result.input_sha256, manifest, chunk } });
    try { receipt = await client.uploadWholeChannel(pending, bytes); }
    catch (error) {
      if (error.code !== 'STALE_LEASE' || error.status !== 409) throw error;
      await journal.put('stale', { code: error.code });
      await spool.archiveStaleResult('whole-pending.json');
      return true;
    }
    if (receipt?.durable !== true || receipt.command_id !== pending.command_id || receipt.part !== chunk.part
        || receipt.sha256 !== chunk.sha256 || receipt.result_sha256 !== manifest.sha256) throw new RemoteProtocolError('INVALID_WHOLE_CHANNEL_RECEIPT', 502);
  }
  if (receipt.complete !== true) throw new RemoteProtocolError('WHOLE_CHANNEL_NOT_RECEIVED', 503);
  await journal.put('receipt', receipt);
  await spool.remove('whole-pending.json');
  await rm(directory(spool, pending.command_id), { recursive: true, force: true });
  return true;
}

// Discover the fsynced delivery identity even if the process died before saving
// its upload pointer. Replay before network cleanup can retire the original task.
export async function recoverWholeChannel({ client, spool }) {
  const root = join(spool.directory, 'whole');
  const entries = await readdir(root, { withFileTypes: true }).catch(error => {
    if (error.code === 'ENOENT') return [];
    throw error;
  });
  for (const entry of entries) {
    if (!entry.isDirectory()) throw new Error('WHOLE_CHANNEL_INVALID_DIRECTORY');
    const journal = await new Journal(directory(spool, entry.name), spool.maxBytes).init();
    if (journal.get('stale')) {
      if ((await spool.read('whole-pending.json'))?.command_id === entry.name) await spool.archiveStaleResult('whole-pending.json');
      continue;
    }
    if (journal.get('receipt')) {
      if ((await spool.read('whole-pending.json'))?.command_id === entry.name) await spool.remove('whole-pending.json');
      await rm(directory(spool, entry.name), { recursive: true, force: true });
      continue;
    }
    const pending = journal.get('delivery');
    if (!pending) continue; // No collection starts before delivery is fsynced.
    if (pending.command_id !== entry.name) throw new Error('WHOLE_CHANNEL_DELIVERY_CONFLICT');
    await interruptWholeChannel(journal);
    const existing = await spool.read('whole-pending.json');
    if (existing && JSON.stringify(existing) !== JSON.stringify(pending)) throw new Error('WHOLE_CHANNEL_DELIVERY_CONFLICT');
    await spool.save('whole-pending.json', Buffer.from(JSON.stringify(pending)));
    await flushWholeChannel({ client, spool });
  }
  // Never discard unacknowledged data if its journal is missing or damaged.
  const pending = await spool.read('whole-pending.json');
  if (pending && !entries.some(entry => entry.name === pending.command_id)) throw new Error('WHOLE_CHANNEL_JOURNAL_MISSING');
}

export async function executeWholeChannelCommand({ client, spool, lease, command, youtube, signal }) {
  if (client.transport !== 'nats' || typeof client.wholeChannelInput !== 'function') throw new Error('WHOLE_CHANNEL_NATS_REQUIRED');
  const journal = await new Journal(directory(spool, command.command_id), spool.maxBytes).init();
  let input = journal.get('input');
  if (!input) {
    const parts = [];
    let manifest;
    for (let part = 0; !manifest || part < manifest.parts; part++) {
      signal.throwIfAborted();
      const reply = await client.wholeChannelInput(lease, command.command_id, part);
      if (reply.command_id !== command.command_id || reply.generation !== lease.generation
          || (manifest && JSON.stringify(manifest) !== JSON.stringify(reply.manifest))) throw new Error('WHOLE_CHANNEL_INPUT_CONFLICT');
      manifest = reply.manifest;
      if (manifest.sha256 !== command.input.input_sha256) throw new Error('WHOLE_CHANNEL_INPUT_CONFLICT');
      parts.push(reply.chunk);
      // Validate each part and its count before allocating the next response.
      validateWholeChannelPart(manifest, reply.chunk);
    }
    input = decodeWholeChannelParts(manifest, parts);
  }
  if (input.plan.channel_id !== lease.input.plan.channel_id || input.plan.plan_id !== lease.input.plan.plan_id
      || input.generation !== lease.generation) throw new Error('WHOLE_CHANNEL_INPUT_CONFLICT');
  await journal.put('input', input);
  await journal.put('delivery', { task_id: lease.task_id, generation: lease.generation, command_id: command.command_id });
  await collectWholeChannel({ input, journal, youtube, signal });
  await spool.save('whole-pending.json', Buffer.from(JSON.stringify({ task_id: lease.task_id,
    generation: lease.generation, command_id: command.command_id })));
  await flushWholeChannel({ client, spool });
}
