import test from 'node:test';
import assert from 'node:assert/strict';
import {createServer} from 'node:tls';
import {connect as tcpConnect} from 'node:net';
import {readFile} from 'node:fs/promises';
import {connect} from '@nats-io/transport-node';
import {createRemoteNatsClient} from '../src/remoteNodes/natsClient.js';

const brokerPort=process.env.REMOTE_NATS_RECOVERY_TCP_PORT;
test('an existing worker reconnects when its old socket stops carrying traffic', {skip:!brokerPort,timeout:20000},async t=>{
  const sockets=new Set();const targets=new Set();let blackhole=false;
  const proxy=createServer({key:await readFile(process.env.REMOTE_NATS_RECOVERY_KEY),cert:await readFile(process.env.REMOTE_NATS_RECOVERY_CA)},socket=>{
    sockets.add(socket);socket.on('error',()=>{});socket.on('close',()=>sockets.delete(socket));
    const target=tcpConnect({host:'127.0.0.1',port:Number(process.env.REMOTE_NATS_RECOVERY_WS_PORT)},()=>{socket.pipe(target);target.pipe(socket);});
    targets.add(target);target.on('error',()=>socket.destroy());socket.on('close',()=>{targets.delete(target);target.destroy();});
    if(blackhole){socket.pause();target.pause();}
  });
  proxy.on('tlsClientError',()=>{});
  await new Promise(r=>proxy.listen(0,'127.0.0.1',r));
  const responder=await connect({servers:`nats://127.0.0.1:${brokerPort}`});
  const subscription=responder.subscribe('qy.remote.rpc.11111111-1111-4111-8111-111111111111.*',{
    callback(error,msg){assert.ifError(error);msg.respond(Buffer.from(JSON.stringify({ok:true,value:{ack:true}})));},
  });
  await responder.flush();
  const client=await createRemoteNatsClient({url:`wss://127.0.0.1:${proxy.address().port}/node-messages`,
    nodeId:'11111111-1111-4111-8111-111111111111',token:'a'.repeat(64),slot:'incremental-1',timeoutMs:200,
    transportHealthTimeoutMs:200,tls:{caFile:process.env.REMOTE_NATS_RECOVERY_CA}});
  t.after(async()=>{for(const socket of sockets)socket.destroy();for(const target of targets)target.destroy();await client.close();subscription.unsubscribe();await responder.close();await new Promise(r=>proxy.close(r));});
  assert.deepEqual(await client.workerHeartbeat({}),{ack:true});
  // Only the established connection breaks. Fresh connections can already
  // reach the center, exactly as in the offline-worker incident.
  for(const socket of sockets)socket.pause();
  for(const target of targets)target.pause();
  await assert.rejects(client.workerHeartbeat({}),{code:'NATS_TRANSPORT_UNAVAILABLE'});
  assert.deepEqual(await client.workerHeartbeat({}),{ack:true},'the same worker client must recover without restarting its process');
});
