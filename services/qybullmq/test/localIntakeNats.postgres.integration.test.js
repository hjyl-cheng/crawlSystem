import test from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import {setTimeout as delay} from 'node:timers/promises';
import {connectNats} from '../src/remoteNodes/natsConnection.js';
import {createTransportSignals} from '../src/remoteNodes/transportSignals.js';
import {createLocalIntakeSignals,forwardLocalIntakeSignals,LOCAL_INTAKE_SUBJECT} from '../src/remoteNodes/localIntakeSignals.js';
import {assertIsolatedRemoteDatabase} from '../src/remoteNodes/isolation.js';
const url=process.env.REMOTE_NATS_TEST_URL;
test('20 local subscribers share one SQL listener; only committed own-worker hints wake them',{skip:!url,timeout:30000},async t=>{
  const pool=new pg.Pool({connectionString:process.env.REMOTE_NODE_TEST_DATABASE_URL,max:2});
  await assertIsolatedRemoteDatabase(pool);
  const count=async()=>Number((await pool.query("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND application_name='remote-node-nats-notifications'")).rows[0].count);
  const before=await count();let signals,nc,stop;const clients=[];
  t.after(async()=>{stop?.();await Promise.all(clients.map(c=>c.close()));await nc?.close();await signals?.close();await pool.end();});
  const tls={caFile:process.env.REMOTE_NATS_TEST_CA};
  nc=await connectNats({servers:url,tls,user:'center',pass:process.env.REMOTE_NATS_TEST_PASSWORD});
  signals=await createTransportSignals({connectionString:process.env.REMOTE_NODE_TEST_DATABASE_URL});
  stop=forwardLocalIntakeSignals(signals,nc);
  for(let i=0;i<20;i++)clients.push(await createLocalIntakeSignals({url,tls,password:process.env.REMOTE_NATS_TEST_LOCAL_PASSWORD,workerId:'worker-'+i,allowLoopback:true}));
  assert.equal(await count(),before+1,'20 Workers must not open 20 LISTEN connections');
  const seen=new Set();const waits=clients.map((c,i)=>{const w=c.watch('local-intake:worker-'+i,{timeoutMs:10000});w.wait.then(()=>seen.add(i));return w;});
  const tx=await pool.connect();try{
    await tx.query('BEGIN');await tx.query("SELECT pg_notify('qy_remote_transport','local-intake:worker-0')");await tx.query('ROLLBACK');
    await delay(150);assert.equal(seen.size,0);
    await tx.query("SELECT pg_notify('qy_remote_transport','unrelated')");await delay(100);assert.equal(seen.size,0);
    await tx.query("SELECT pg_notify('qy_remote_transport','local-intake:worker-0')");
    await Promise.race([waits[0].wait,delay(1500).then(()=>assert.fail('committed notification was not relayed'))]);
    assert.deepEqual([...seen],[0]);
    // Force the center LISTEN backend to reconnect. All clients reread SQL on loss.
    const pid=(await tx.query("SELECT pid FROM pg_stat_activity WHERE datname=current_database() AND application_name='remote-node-nats-notifications'")).rows[0].pid;
    await tx.query('SELECT pg_terminate_backend($1)',[pid]);
    await Promise.race([Promise.all(waits.map(w=>w.wait)),delay(2000).then(()=>assert.fail('disconnect did not wake subscribers'))]);
    await delay(1250);assert.equal(await count(),before+1);
    const w=clients[1].watch('local-intake:worker-1',{timeoutMs:1500});
    await tx.query("SELECT pg_notify('qy_remote_transport','local-intake:worker-1')");const at=Date.now();await w.wait;assert.ok(Date.now()-at<1000,'relay resumes after LISTEN reconnect');
  }finally{tx.release();waits.forEach(w=>w.cancel());}
  // Dedicated credentials cannot publish fake control hints or inspect node traffic.
  const restricted=await connectNats({servers:url,tls,user:'local-intake',pass:process.env.REMOTE_NATS_TEST_LOCAL_PASSWORD});
  try{
    const failures=[];const statuses=(async()=>{for await(const s of restricted.status())if(s.type==='error')failures.push(s.error??s.data??s);})();
    restricted.publish(LOCAL_INTAKE_SUBJECT,Buffer.from('*'));restricted.subscribe('qy.remote.>');await restricted.flush();
    for(let i=0;i<50&&failures.length<2;i++)await delay(20);
    assert.equal(failures.length,2,'broker rejects publishing and unrelated subscriptions');
    await restricted.close();await statuses;
  }finally{await restricted.close();}
});
