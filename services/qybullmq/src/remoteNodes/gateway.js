import { createServer } from 'node:http';
import { MAX_GZIP_BYTES, RemoteProtocolError, uuid } from './protocol.js';

async function body(request, limit) {
  if (Number(request.headers['content-length'] || 0) > limit) {
    request.resume();
    throw new RemoteProtocolError('BODY_TOO_LARGE', 413);
  }
  return new Promise((resolve, reject) => {
    let size = 0;
    const chunks = [];
    request.on('data', (chunk) => {
      size += chunk.length;
      if (size <= limit) chunks.push(chunk);
      else { chunks.length = 0; reject(new RemoteProtocolError('BODY_TOO_LARGE', 413)); }
    });
    request.on('end', () => resolve(Buffer.concat(chunks)));
    request.on('error', reject);
    request.on('aborted', () => reject(new RemoteProtocolError('REQUEST_ABORTED', 400)));
  });
}

async function json(request, limit = 4096) {
  if (request.headers['content-encoding']) throw new RemoteProtocolError('UNSUPPORTED_ENCODING', 415);
  try { return JSON.parse((await body(request, limit)).toString('utf8')); }
  catch (error) {
    if (error instanceof RemoteProtocolError) throw error;
    throw new RemoteProtocolError('INVALID_JSON', 400);
  }
}

// TLS termination belongs to the deployment. The isolated runner binds loopback only.
export function createRemoteNodeGateway({ store, channelPlans = null, routes = null, youtubeSessions = null, workerConnections = null, deploymentAdmin = null, maxConcurrentRequests = 8 }) {
  let active = 0;
  const server = createServer(async (request, response) => {
    const send = (status, value) => {
      response.writeHead(status, { 'content-type': 'application/json', 'cache-control': 'no-store' });
      response.end(JSON.stringify(value));
    };
    if (active >= maxConcurrentRequests) {
      request.resume();
      send(503, { error: 'GATEWAY_BUSY' });
      return;
    }
    active++;
    try {
      const path = new URL(request.url, 'http://gateway.invalid').pathname;
      const token = /^Bearer ([^\s]+)$/.exec(request.headers.authorization || '')?.[1];
      if(deploymentAdmin && request.method==='POST' && ['/internal/node-deployments/prepare','/internal/node-deployments/status'].includes(path)){
        deploymentAdmin.authenticate(token);
        const value=await json(request,512*1024);
        send(200,path.endsWith('/prepare')?await deploymentAdmin.prepare(value):await deploymentAdmin.status(value));return;
      }
      const nodeId = await store.authenticate(token);
      if (youtubeSessions && request.method === 'POST' && ['/v1/youtube/session','/v1/youtube/checkpoint'].includes(path)) {
        send(200, path.endsWith('/session') ? await youtubeSessions.get(nodeId, await json(request))
          : await youtubeSessions.checkpoint(nodeId, await json(request, 512 * 1024)));
        return;
      }
      if (workerConnections && request.method === 'POST' && path === '/v1/node/heartbeat') {
        send(200, await workerConnections.heartbeat(nodeId, await json(request)));
        return;
      }
      if (routes && request.method === 'POST' && ['/v1/network/grant', '/v1/network/release', '/v1/network/abandon'].includes(path)) {
        const value = await json(request);
        send(200, path.endsWith('/grant') ? await routes.grant(nodeId, value)
          : path.endsWith('/release') ? await routes.release(nodeId, value) : await routes.abandon(nodeId, value));
        return;
      }
      if (request.method === 'POST' && path === '/v1/work/claim') {
        const value = await json(request);
        send(200, { lease: workerConnections
          ? await workerConnections.claim(nodeId, value)
          : await store.claim(nodeId, uuid(value?.claim_id), value.slot ?? null) });
        return;
      }
      const channel = /^\/v1\/channel-plans\/([^/]+)\/(commands|results)$/.exec(path);
      if (channelPlans && request.method === 'POST' && channel) {
        const taskId = uuid(channel[1]);
        if (channel[2] === 'commands') {
          const value = await json(request);
          send(200, await channelPlans.poll(nodeId, { task_id: taskId, generation: value.generation }));
        } else {
          if (request.headers['content-encoding'] !== 'gzip') throw new RemoteProtocolError('GZIP_REQUIRED', 415);
          const compressed = await body(request, MAX_GZIP_BYTES);
          // The store decodes once and validates the body's generation against the lease.
          send(200, await channelPlans.receive(nodeId, { task_id: taskId }, compressed));
        }
        return;
      }
      const work = /^\/v1\/work\/([^/]+)\/(heartbeat|results)$/.exec(path);
      if (request.method === 'POST' && work) {
        const taskId = uuid(work[1]);
        if (work[2] === 'heartbeat') {
          const value = await json(request);
          send(200, await store.heartbeat(nodeId, taskId, value?.generation));
        } else {
          if (request.headers['content-encoding'] !== 'gzip') throw new RemoteProtocolError('GZIP_REQUIRED', 415);
          send(200, await store.receive(nodeId, taskId, await body(request, MAX_GZIP_BYTES)));
        }
        return;
      }
      const receipt = /^\/v1\/receipts\/([^/]+)$/.exec(path);
      if (request.method === 'GET' && receipt) {
        send(200, await store.receipt(nodeId, uuid(receipt[1])));
        return;
      }
      throw new RemoteProtocolError('NOT_FOUND', 404);
    } catch (error) {
      request.resume();
      send(error instanceof RemoteProtocolError ? error.status : 503,
        { error: error instanceof RemoteProtocolError ? error.code : 'GATEWAY_UNAVAILABLE' });
    } finally { active--; }
  });
  server.requestTimeout = 30000;
  server.headersTimeout = 10000;
  server.keepAliveTimeout = 5000;
  server.maxRequestsPerSocket = 100;
  server.on('connection', (socket) => socket.setTimeout(30000, () => socket.destroy()));
  return server;
}
