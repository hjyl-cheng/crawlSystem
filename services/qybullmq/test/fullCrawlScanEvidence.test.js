import assert from 'node:assert/strict';
import test from 'node:test';
import {fullCrawlUploadScan,closeRepairedFullCrawlScan} from '../src/fullCrawlScanEvidence.js';
import {fullCrawlUploadsDocument,fullCrawlUploadsHash,fullCrawlTargetHash} from '../src/fullCrawlYoutubeJsModel.js';
import {YOUTUBEJS_API_FULL_CRAWL_FETCH_CONTRACT} from '../src/fullCrawlFetchContract.js';
function fixture(){
 const document=fullCrawlUploadsDocument({entries:[{video_id:'v1',position:1,url:'https://www.youtube.com/watch?v=v1'}],scan:{complete:true}});
 const run={run_id:'r1',detail_status:'done',content_limit:30,result_json:{fetch_contract:YOUTUBEJS_API_FULL_CRAWL_FETCH_CONTRACT,full_crawl:{uploads:{document,selected_count:1,uploads_hash:fullCrawlUploadsHash(document),target_hash:fullCrawlTargetHash(document.entries)}}}};
 const rows=[{source_content_id:'v1',source_url:document.entries[0].source_url,position:1,target:document.entries[0],detail_status:'done',disposition:'terminal_excluded'}];
 return {run,rows};
}
test('checkpoint repair must close the frozen Full Crawl receipt before finalization',async()=>{
 const {run,rows}=fixture();
 let writes=0;
 const client={query:async(sql,params)=>{
  if(sql.startsWith('UPDATE')){writes++;run.result_json.full_crawl.fetch=JSON.parse(params[1]);return {rowCount:1};}
  return {rows:sql.includes('content_candidates')?rows:[run]};
 }};
 await closeRepairedFullCrawlScan(client,'r1');
 await closeRepairedFullCrawlScan(client,'r1');
 assert.equal(writes,1);
 assert.doesNotThrow(()=>fullCrawlUploadScan(run,rows));
});

for(const defect of ['missing','queued','wrong_target'])test(`checkpoint repair refuses ${defect} candidates`,async()=>{
 const {run,rows}=fixture();
 if(defect==='missing')rows.length=0;
 if(defect==='queued')rows[0].detail_status='queued';
 if(defect==='wrong_target')rows[0].source_content_id='another';
 const client={query:async(sql)=>{
  assert.ok(!sql.startsWith('UPDATE'));
  return {rows:sql.includes('content_candidates')?rows:[run]};
 }};
 await assert.rejects(closeRepairedFullCrawlScan(client,'r1'),/checkpoint is incomplete or conflicting/);
});
