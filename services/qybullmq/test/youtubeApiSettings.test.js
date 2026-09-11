import test from 'node:test';
import assert from 'node:assert/strict';
import {createYoutubeApiSettingsLoader} from '../src/youtubeApiSettings.js';

test('API settings keep stored priority, normalization and per-center cache expiry',async()=>{
 let calls=0;let now=1000;let stored={api_keys:[' key-a ','key-a','key-b'],timeout_ms:999999,batch_size:0,daily_request_limit:12,fallback_mode:'disabled'};
 const load=createYoutubeApiSettingsLoader({environment:{YOUTUBE_DATA_API_KEY:'environment-key'},nowImpl:()=>now,
   query:async()=>{calls++;return {rows:[{value_json:stored}]};}});
 assert.deepEqual(await load(),{apiKeys:['key-a','key-b'],timeoutMs:60000,batchSize:1,dailyRequestLimit:12,fallbackMode:'disabled'});
 stored={api_key:'replacement'};await load();assert.equal(calls,1);now+=30001;
 assert.deepEqual((await load()).apiKeys,['replacement']);assert.equal(calls,2);
 const other=createYoutubeApiSettingsLoader({query:async()=>({rows:[]}),environment:{YOUTUBE_DATA_API_KEYS:'c,d; c\ne'}});
 assert.deepEqual((await other()).apiKeys,['c','d','e']);assert.equal(calls,2);
});
test('settings read failure preserves the original environment fallback and daily budget',async()=>{
 const load=createYoutubeApiSettingsLoader({query:async()=>{throw new Error('offline');},environment:{YOUTUBE_DATA_API_KEY:'key',YOUTUBE_DATA_API_DAILY_REQUEST_LIMIT:'0',YOUTUBE_DATA_API_FALLBACK_MODE:'disabled'}});
 assert.deepEqual(await load(),{apiKeys:['key'],timeoutMs:12000,batchSize:50,dailyRequestLimit:0,fallbackMode:'disabled'});
});
