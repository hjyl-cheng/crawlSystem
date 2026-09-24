import test from 'node:test';
import assert from 'node:assert/strict';
import { setImmediate } from 'node:timers/promises';
import { createIncrementalTransactionScope } from '../src/remoteNodes/incrementalTransactionScope.js';

test('55 interleaved Plans retain only their own transaction', async () => {
  const scopes=Array.from({length:55},()=>createIncrementalTransactionScope());
  try {
    await Promise.all(scopes.map((scope,index)=>scope.run({index},async()=>{
      for(let step=0;step<5;step++){
        await setImmediate();
        assert.equal(scope.getStore().index,index);
        for(const other of scopes)if(other!==scope)assert.equal(other.getStore(),undefined);
      }
    })));
    for(const scope of scopes)assert.equal(scope.getStore(),undefined);
  } finally {for(const scope of scopes)scope.disable();}
});

test('nested different Plans preserve both owners and restore parent after failure', async () => {
  const outer=createIncrementalTransactionScope(),inner=createIncrementalTransactionScope();
  const a={},b={};
  try {
    await outer.run(a,async()=>{
      assert.equal(inner.getStore(),undefined);
      await assert.rejects(inner.run(b,async()=>{
        await setImmediate();
        assert.equal(inner.getStore(),b);assert.equal(outer.getStore(),a);
        throw new Error('rollback inner');
      }),/rollback inner/);
      assert.equal(outer.getStore(),a);assert.equal(inner.getStore(),undefined);
    });
    assert.equal(outer.getStore(),undefined);
  }finally{outer.disable();inner.disable();}
});

test('closing one Plan invalidates late callbacks without closing another Plan', async () => {
  const done=createIncrementalTransactionScope(),live=createIncrementalTransactionScope();
  let release;const pending=new Promise(resolve=>{release=resolve;});
  try{
    const late=done.run({},async()=>{await pending;assert.equal(done.getStore(),undefined);assert.throws(()=>done.run({},()=>{}),/SCOPE_CLOSED/);});
    await live.run({live:true},async()=>{
      done.disable();release();await late;assert.equal(live.getStore().live,true);
    });
  }finally{done.disable();live.disable();}
});
