import assert from 'node:assert/strict';
import test from 'node:test';
import { latestStorageConfig } from '../scripts/manageBusinessLatestStorage.mjs';

const env={EXPECTED_BUSINESS_DATABASE:'business_test',BUSINESS_DATABASE_URL:'postgresql://example/business_test'};
test('storage administration defaults to read only and requires explicit cutover evidence',()=>{
  assert.equal(latestStorageConfig(env,[]).action,'inspect');
  assert.throws(()=>latestStorageConfig(env,['install']),/writes require/);
  assert.throws(()=>latestStorageConfig({...env,CONFIRM_BUSINESS_LATEST_STORAGE:'wrong'},['install','--apply']),/writes require/);
  const confirmed={...env,CONFIRM_BUSINESS_LATEST_STORAGE:'business_test'};
  assert.equal(latestStorageConfig(confirmed,['install','--apply']).action,'install');
  assert.throws(()=>latestStorageConfig(confirmed,['enable','--apply']),/reader-ready/);
  assert.equal(latestStorageConfig({...confirmed,EXPECTED_BUSINESS_WATERMARK:'batch',
    BUSINESS_STORAGE_ACTOR:'operator',BUSINESS_STORAGE_REASON:'latest-only business storage'},
  ['enable','--apply','--reader-ready']).action,'enable');
});
