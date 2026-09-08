import test from 'node:test';
import assert from 'node:assert/strict';
import express from 'express';
import {controlledMigrationGuard} from '../src/controlledMigrationGuard.js';
test('controlled mode allows batch pause/resume/stop but rejects unrelated writes',async()=>{
 const app=express();app.use(controlledMigrationGuard({CONTROLLED_MIGRATION_ONLY:'true'}));app.use((_req,res)=>res.json({ok:true}));
 const server=app.listen(0,'127.0.0.1');await new Promise(r=>server.once('listening',r));
 try{for(const [method,path,status] of [
  ['POST','/api/migration/batches/migration-example/pause',200],['POST','/api/migration/batches/migration-example/resume',200],['POST','/api/migration/batches/migration-example/stop',200],
  ['POST','/api/migration/channels/batch',200],['POST','/api/migration/system-retries/example/retry',200],['GET','/api/migration/batches',200],
  ['POST','/api/migration/batches/example/delete',423],['DELETE','/api/migration/batches/example/stop',423],['POST','/api/queues/clear',423]
 ])assert.equal((await fetch(`http://127.0.0.1:${server.address().port}${path}`,{method})).status,status,`${method} ${path}`);
 }finally{await new Promise(r=>server.close(r))}
});
