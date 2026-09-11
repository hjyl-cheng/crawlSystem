import { RemoteChannelNetworkSession } from '../../src/remoteNodes/channelNetworkSession.js';
import { RemoteResultSpool } from '../../src/remoteNodes/spool.js';
import { createRemoteNodeClient } from '../../src/remoteNodes/client.js';
import { createLocalRotaClient } from '../../src/remoteNodes/localRotaClient.js';

// Only loopback test services; the parent kills this process after real grant
// activation and durable spool persistence, without running finally blocks.
process.once('message', async config => {
  try {
    const client = createRemoteNodeClient({ ...config.center, allowLoopbackHttp: true });
    const localRota = createLocalRotaClient(config.relay);
    const spool = new RemoteResultSpool({ directory: config.directory });
    const session = new RemoteChannelNetworkSession({ client, localRota, spool, slot: 'worker-1',
      withRuntime: (_route, invoke) => invoke(), renewMs: 100 });
    await session.run(config.lease, { signal: AbortSignal.timeout(30000) }, async () => {
      process.send({ active: true });
      await new Promise(() => {});
    });
  } catch (error) { process.send({ error: error.code || error.name }); process.exitCode = 1; }
});
