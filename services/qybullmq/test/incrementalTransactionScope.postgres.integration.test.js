import test from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import { createIncrementalTransactionScope } from '../src/remoteNodes/incrementalTransactionScope.js';
import { RemoteNodeStore } from '../src/remoteNodes/store.js';
import { assertIsolatedRemoteDatabase } from '../src/remoteNodes/isolation.js';

for(const [transport,url] of Object.entries({direct:process.env.REMOTE_NODE_TEST_DATABASE_URL,pooled:process.env.REMOTE_NODE_TEST_TRANSACTION_DATABASE_URL}))
test(`shared context preserves independent PostgreSQL transactions for 55 Plans (${transport})`,{skip:!url},async t=>{
  const pool=new pg.Pool({connectionString:url,max:12});t.after(()=>pool.end());
  await assertIsolatedRemoteDatabase(pool);
  const store=new RemoteNodeStore({pool});
  const withScope=scope=>async action=>scope.getStore()?action(scope.getStore()):
    store.transaction(client=>scope.run(client,()=>action(client)));
  const transactionIds=new Set();
  await Promise.all(Array.from({length:55},async(_,index)=>{
    const scope=createIncrementalTransactionScope(),transaction=withScope(scope);
    try {
      const work=transaction(async client=>{
        const {id}=(await client.query("SELECT txid_current()::text id,set_config('test.plan_owner',$1,true)",[String(index)])).rows[0];
        assert(!transactionIds.has(id));transactionIds.add(id);
        for(let step=0;step<3;step++)await transaction(async nested=>{
          assert.equal(nested,client);
          const row=(await nested.query("SELECT txid_current()::text id,current_setting('test.plan_owner') owner")).rows[0];
          assert.deepEqual(row,{id,owner:String(index)});
        });
        if(index%5===0)throw new Error('rollback one Plan');
      });
      if(index%5===0)await assert.rejects(work,/rollback one Plan/);else await work;
      assert.equal(scope.getStore(),undefined);
    }finally{scope.disable();}
  }));
  assert.equal(transactionIds.size,55);assert.equal(pool.waitingCount,0);
  const outer=createIncrementalTransactionScope(),inner=createIncrementalTransactionScope();
  try{
    await withScope(outer)(async first=>{
      const firstId=(await first.query('SELECT txid_current()::text id')).rows[0].id;
      await assert.rejects(withScope(inner)(async second=>{
        assert.notEqual(second,first);
        const secondId=(await second.query('SELECT txid_current()::text id')).rows[0].id;
        assert.notEqual(secondId,firstId);
        await withScope(outer)(async borrowed=>assert.equal(borrowed,first));
        throw new Error('nested rollback');
      }),/nested rollback/);
      assert.equal(outer.getStore(),first);assert.equal(inner.getStore(),undefined);
      assert.equal((await first.query('SELECT txid_current()::text id')).rows[0].id,firstId);
    });
  }finally{outer.disable();inner.disable();}
});
