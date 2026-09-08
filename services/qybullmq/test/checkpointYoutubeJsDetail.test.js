import assert from 'node:assert/strict';
import test from 'node:test';
import {createCheckpointYoutubeJsDetail} from '../src/checkpointYoutubeJsDetail.js';
test('checkpoint repair calls the shared Full Crawl fallback and retains API type uncertainty',async()=>{
 let requested;
 const capture=createCheckpointYoutubeJsDetail({fetchDetail:async()=>assert.fail('must consume API evidence'),fallback:async request=>{
  requested=request;
  return request.validate({id:'v1',title:'API title',published_at:'2020-01-01T00:00:00Z',duration_seconds:50,
    view_count_text:'5',access_status:'public',comments_disabled:true,
    video_detail_fallback:{source:'youtube_data_api_batch',youtubejs_exhausted:true}});
 }});
 const result=await capture({run_id:'root-run',source_content_id:'v1',attempts:2});
 assert.equal(result.error,null);assert.equal(result.detail.title,'API title');
 assert.equal(requested.requestId,JSON.stringify(['full','root-run','v1']));
 assert.equal(requested.consumer,'full');assert.equal(requested.attempt,3);
});
test('checkpoint network failures propagate without a second extractor',async()=>{
 const error=new Error('network unavailable');let calls=0;
 const capture=createCheckpointYoutubeJsDetail({fetchDetail:async(_id,options)=>{
  calls++;assert.equal(options.requireContentType,true);throw error;
 },fallback:async request=>request.fetch()});
 const result=await capture({run_id:'r1',source_content_id:'v1'});
 assert.equal(result.error,error);assert.equal(result.detail,null);assert.equal(calls,1);
});
