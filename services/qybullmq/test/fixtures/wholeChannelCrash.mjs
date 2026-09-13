import { readFile } from 'node:fs/promises';
import { RemoteResultSpool } from '../../src/remoteNodes/spool.js';
import { executeWholeChannelCommand } from '../../src/remoteNodes/wholeChannelNode.js';
import { wholeChannelParts } from '../../src/remoteNodes/wholeChannelProtocol.js';

const [directory, fixturePath, crashAt] = process.argv.slice(2);
const { input, lease, command, detail } = JSON.parse(await readFile(fixturePath, 'utf8'));
const spool = new RemoteResultSpool({ directory });
await spool.init();
const save = spool.save.bind(spool);
spool.save = async (name, bytes) => {
  if (name === 'whole-pending.json' && crashAt === 'before_pointer') process.kill(process.pid, 'SIGKILL');
  return save(name, bytes);
};
const { manifest, parts } = wholeChannelParts(input);
await executeWholeChannelCommand({ spool, lease, command, signal: new AbortController().signal,
  client: { transport: 'nats', wholeChannelInput: async (_lease, id, part) => ({ command_id: id,
    generation: lease.generation, manifest, chunk: parts[part] }), uploadWholeChannel: () => { throw new Error('unexpected upload'); } },
  youtube: { openChannel: () => { throw new Error('resume snapshot must not rescan'); },
    fetchDetail: async id => {
      if (id === 'second' && crashAt === 'during_collection') process.kill(process.pid, 'SIGKILL');
      return { ...detail, id };
    } },
});
