import test from 'node:test';
import assert from 'node:assert/strict';
import {EventEmitter} from 'node:events';
import pg from 'pg';
import {SupervisionGuards} from '../src/remoteNodes/supervisionGuards.js';

// Keep pg.Pool's real capacity/timeout behavior; only the database socket is
// replaced. Mirror the release's global guard plus all four supervision groups.
class Client extends EventEmitter {
 constructor(){super();this._queryable=true;}
 connect(cb){queueMicrotask(()=>cb());}
 async query(){return {rows:[{locked:true,pid:1}]};}
 end(cb){this._ending=true;queueMicrotask(()=>{this.emit('end');cb?.();});}
}
test('global release guard and four supervision groups fit the caller-reserved pool', {timeout:3000},async()=>{
 const keepAlive=setInterval(()=>{},1000);
 const pool=new pg.Pool({Client,max:5,connectionTimeoutMillis:100});
 const guards=new SupervisionGuards({pool,groups:4});let budget;
 try{
  budget=await pool.connect();
  const keys=new Map();
  for(let n=1;keys.size<4;n++){const key=`remote-full-crawl-supervisor:fixture/full-crawl-${n}`;keys.set(guards.groupFor(key),key);}
  for(const key of keys.values())assert.ok(await guards.acquire(key,()=>{}));
  assert.equal(pool.totalCount,5);
 }finally{await guards.close();budget?.release();await pool.end();clearInterval(keepAlive);}
});
test('a smaller dedicated pool still grows to the required guard group count',()=>{
 const pool={options:{max:1}};new SupervisionGuards({pool,groups:4});assert.equal(pool.options.max,4);
});
