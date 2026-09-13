import { randomUUID } from 'node:crypto';
import { canonicalIncrementalJson } from '../incrementalPlan.js';
import { decodeResult, encodeResult, hash, uuid, RemoteProtocolError } from './protocol.js';
import { planFromTask } from './channelPlanContract.js';
import { wholeChannelBytes, wholeChannelParts, validateWholeChannelPart, decodeWholeChannelParts } from './wholeChannelProtocol.js';

const fail = code => { throw new RemoteProtocolError(code); };

// All receipt checks and writes occur while holding the existing task row lock.
// A historical owner can replay bytes already accepted, never introduce bytes.
export class WholeChannelStore {
  constructor({ channelPlans, assertBusinessFence }) {
    if (typeof assertBusinessFence !== 'function') throw new TypeError('transactional business fence required');
    Object.assign(this, { channelPlans, store: channelPlans.store, assertBusinessFence });
  }

  async prepare(client, task, input) {
    const plan = planFromTask(task);
    if (input.plan?.plan_id !== plan.plan_id || input.plan?.channel_id !== plan.channel_id
        || canonicalIncrementalJson(input.plan) !== canonicalIncrementalJson(plan)
        || input.generation !== task.generation) fail('WHOLE_CHANNEL_INPUT_IDENTITY');
    const inputSha256 = hash(wholeChannelBytes(input));
    const prior = (await client.query('SELECT command_id,input_sha256 FROM remote_ingestion.whole_channel_inputs WHERE task_id=$1 AND generation=$2', [task.task_id, task.generation])).rows[0];
    if (prior) {
      if (prior.input_sha256 !== inputSha256) fail('WHOLE_CHANNEL_INPUT_CONFLICT');
      return prior.command_id;
    }
    const commandId = randomUUID();
    await client.query(`INSERT INTO remote_ingestion.channel_commands(command_id,task_id,generation,command_key,operation,input)
      VALUES($1,$2,$3,$4,'collect_channel',$5)`, [commandId, task.task_id, task.generation,
      hash(`collect_channel:${inputSha256}`), { version: 1, channel_id: plan.channel_id, input_sha256: inputSha256 }]);
    await client.query(`INSERT INTO remote_ingestion.whole_channel_inputs(command_id,task_id,generation,input_sha256,input_json)
      VALUES($1,$2,$3,$4,$5)`, [commandId, task.task_id, task.generation, inputSha256, input]);
    return commandId;
  }

  async input(nodeId, request) {
    return this.store.transaction(async client => {
      await this.channelPlans.lock(client, { ...request, node_id: nodeId });
      const row = (await client.query('SELECT input_json FROM remote_ingestion.whole_channel_inputs WHERE command_id=$1 AND task_id=$2 AND generation=$3',
        [uuid(request.command_id), uuid(request.task_id), request.generation])).rows[0];
      if (!row) fail('WHOLE_CHANNEL_INPUT_MISSING');
      const { manifest, parts } = wholeChannelParts(row.input_json);
      if (!Number.isSafeInteger(request.part) || !parts[request.part]) fail('INVALID_WHOLE_CHANNEL_PART');
      return { command_id: request.command_id, generation: request.generation, manifest, chunk: parts[request.part] };
    });
  }

  async receive(nodeId, lease, compressed) {
    const { value: envelope } = await decodeResult(compressed);
    if (envelope.outcome !== 'success' || envelope.batch_id !== envelope.command_id) fail('INVALID_WHOLE_CHANNEL_FRAME');
    const value = { ...envelope.data, command_id: envelope.command_id, generation: envelope.generation };
    if (lease.generation != null && lease.generation !== value.generation) fail('STALE_LEASE');
    const commandId = uuid(value.command_id);
    const generation = value.generation;
    const payload = validateWholeChannelPart(value.manifest, value.chunk);
    return this.store.transaction(async client => {
      const task = await this.channelPlans.lock(client, { ...lease, generation, node_id: nodeId },
        { requireLive: false, allowHistoricalReceipt: true });
      const historical = task.generation !== generation || task.node_id !== nodeId;
      if (historical) {
        const owner = (await client.query('SELECT 1 FROM remote_ingestion.claims WHERE task_id=$1 AND generation=$2 AND node_id=$3 LIMIT 1',
          [task.task_id, generation, nodeId])).rows[0];
        if (!owner) fail('STALE_LEASE');
      }
      const input = (await client.query('SELECT * FROM remote_ingestion.whole_channel_inputs WHERE command_id=$1 AND task_id=$2 AND generation=$3 FOR UPDATE',
        [commandId, task.task_id, generation])).rows[0];
      if (!input || input.input_sha256 !== value.input_sha256) fail('WHOLE_CHANNEL_INPUT_CONFLICT');
      if (input.result_manifest && canonicalIncrementalJson(input.result_manifest) !== canonicalIncrementalJson(value.manifest)) fail('WHOLE_CHANNEL_RESULT_CONFLICT');
      const prior = (await client.query('SELECT sha256 FROM remote_ingestion.whole_channel_chunks WHERE command_id=$1 AND part=$2', [commandId, value.chunk.part])).rows[0];
      if (prior && prior.sha256 !== value.chunk.sha256) fail('WHOLE_CHANNEL_RESULT_CONFLICT');
      if (!input.received_at) {
        // Even a duplicate partial chunk is not authority to complete an expired
        // delivery. Only a sealed whole-channel receipt survives lease handoff.
        if (historical || task.state !== 'leased' || !task.live) fail('STALE_LEASE');
        await this.assertBusinessFence(client, task);
        if (!input.result_manifest) await client.query('UPDATE remote_ingestion.whole_channel_inputs SET result_manifest=$2 WHERE command_id=$1', [commandId, value.manifest]);
        if (!prior) await client.query('INSERT INTO remote_ingestion.whole_channel_chunks(command_id,part,sha256,payload) VALUES($1,$2,$3,$4)',
          [commandId, value.chunk.part, value.chunk.sha256, payload]);
        const rows = await this.parts(client, commandId);
        if (rows.length === value.manifest.parts) {
          const result = decodeWholeChannelParts(value.manifest, rows);
          const plan = input.input_json.plan;
          if (result.version !== 1 || result.generation !== generation || result.plan_id !== plan.plan_id
              || result.channel_id !== plan.channel_id || result.input_sha256 !== input.input_sha256) fail('WHOLE_CHANNEL_RESULT_IDENTITY');
          await client.query('UPDATE remote_ingestion.whole_channel_inputs SET received_at=clock_timestamp() WHERE command_id=$1', [commandId]);
          const receiptPayload = await encodeResult({ version: 1, generation, batch_id: commandId,
            command_id: commandId, outcome: 'success', data: { whole_channel_result: commandId, sha256: value.manifest.sha256 } });
          const { sha256: receiptSha256 } = await decodeResult(receiptPayload);
          await client.query(`UPDATE remote_ingestion.channel_commands SET state='received',batch_id=$1,
            sha256=$2,payload_gzip=$3,received_at=clock_timestamp() WHERE command_id=$1`, [commandId, receiptSha256, receiptPayload]);
          input.received_at = true;
        }
      } else if (!prior) fail('WHOLE_CHANNEL_RESULT_CONFLICT');
      return { durable: true, command_id: commandId, part: value.chunk.part, sha256: value.chunk.sha256,
        complete: Boolean(input.received_at), result_sha256: value.manifest.sha256 };
    });
  }

  async parts(client, commandId) {
    return (await client.query('SELECT part,sha256,payload FROM remote_ingestion.whole_channel_chunks WHERE command_id=$1 ORDER BY part', [commandId])).rows
      .map(row => ({ part: row.part, sha256: row.sha256, data: row.payload.toString('base64') }));
  }

  async result(client, commandId) {
    const input = (await client.query('SELECT * FROM remote_ingestion.whole_channel_inputs WHERE command_id=$1', [uuid(commandId)])).rows[0];
    if (!input?.received_at) fail('WHOLE_CHANNEL_NOT_RECEIVED');
    if (input.pruned_at) fail('WHOLE_CHANNEL_RESULT_ARCHIVED');
    return { input: input.input_json, result: decodeWholeChannelParts(input.result_manifest, await this.parts(client, commandId)) };
  }

  async pruneApplied({ retentionDays = 7, limit = 32 } = {}) {
    if (!Number.isSafeInteger(retentionDays) || retentionDays < 1 || !Number.isSafeInteger(limit) || limit < 1 || limit > 256) throw new TypeError('bounded whole-channel retention required');
    return this.store.transaction(async client => {
      // Same task-first lock order as receipt/application. Pending, failed and
      // API-waiting work retains all evidence. Keep digests for old ACK replay.
      const tasks = (await client.query(`SELECT t.task_id FROM remote_ingestion.whole_channel_inputs w
        JOIN remote_ingestion.tasks t ON t.task_id=w.task_id
        WHERE w.pruned_at IS NULL AND t.state='applied' AND t.applied_at<clock_timestamp()-($1*interval '1 day')
        ORDER BY t.applied_at LIMIT $2 FOR UPDATE OF t SKIP LOCKED`, [retentionDays,limit])).rows;
      if (!tasks.length) return 0;
      const inputs = (await client.query(`UPDATE remote_ingestion.whole_channel_inputs SET input_json=NULL,pruned_at=clock_timestamp()
        WHERE task_id=ANY($1::uuid[]) AND pruned_at IS NULL RETURNING command_id`, [tasks.map(row=>row.task_id)])).rows;
      await client.query('UPDATE remote_ingestion.whole_channel_chunks SET payload=NULL WHERE command_id=ANY($1::uuid[])', [inputs.map(row=>row.command_id)]);
      return inputs.length;
    });
  }
}
