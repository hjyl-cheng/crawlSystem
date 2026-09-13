import test from 'node:test';
import assert from 'node:assert/strict';
import {migrationBatchTiming} from './migrationBatchPanel.js';
test('stale migration counts never produce a current rate or ETA',()=>{
  const result=migrationBatchTiming({statistics_age_seconds:5217,active_seconds:5356*60,sample_active_seconds:312000,total_count:380000,status:'running'},316000);
  assert.equal(result.stale,true);assert.equal(result.rate,null);assert.equal(result.etaHours,null);
});
test('migration average uses the matching sample duration, not time elapsed since that sample',()=>{
  const result=migrationBatchTiming({statistics_age_seconds:60,active_seconds:3660,sample_active_seconds:3600,total_count:2000,status:'running'},1000);
  assert.equal(result.stale,false);assert.equal(result.rate,1000);assert.equal(result.etaHours,1);
});
test('paused batches and empty or invalid samples have no ETA',()=>{
  assert.equal(migrationBatchTiming({statistics_age_seconds:10,active_seconds:3600,sample_active_seconds:3600,total_count:2000,status:'paused'},1000).etaHours,null);
  assert.equal(migrationBatchTiming({active_seconds:0,status:'running'},0).rate,null);
});
