import { readFile } from 'node:fs/promises';
import pg from 'pg';
import { RemoteNodeStore } from '../src/remoteNodes/store.js';
import { createRemoteNodeGateway } from '../src/remoteNodes/gateway.js';
import { assertIsolatedRemoteDatabase } from '../src/remoteNodes/isolation.js';
import { RemoteChannelPlanStore } from '../src/remoteNodes/channelPlanStore.js';

// Explicit test URL only: never fall back to POSTGRES_* or the production db.js.
const connectionString = process.env.REMOTE_NODE_TEST_DATABASE_URL;
if (!connectionString) throw new Error('REMOTE_NODE_TEST_DATABASE_URL is required');
const pool = new pg.Pool({ connectionString, max: 3, connectionTimeoutMillis: 5000,
  application_name: 'remote-node-isolated-gateway' });
try {
  await assertIsolatedRemoteDatabase(pool);
  await pool.query(await readFile(new URL('../src/remoteNodes/schema.sql', import.meta.url), 'utf8'));
  const store = new RemoteNodeStore({ pool });
  const server = createRemoteNodeGateway({ store, channelPlans: new RemoteChannelPlanStore({ store }) });
  server.listen(Number(process.env.REMOTE_NODE_TEST_PORT || 3187), '127.0.0.1', () => {
    console.log(JSON.stringify({ event: 'isolated_gateway_listening', address: server.address() }));
  });
  const stop = () => server.close(() => pool.end());
  process.once('SIGTERM', stop);
  process.once('SIGINT', stop);
} catch (error) {
  await pool.end();
  throw error;
}
