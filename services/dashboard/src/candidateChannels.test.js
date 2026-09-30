import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import {readFile} from 'node:fs/promises';
import {CANDIDATE_STATUS_LABELS,CANDIDATE_STATUSES,candidateFilters,candidateListSql,candidateScope,loadCandidateList,loadCandidateStatistics} from './candidateChannels.js';

test('filters accept only known statuses, real dates and bounded paging',()=>{
  assert.deepEqual(candidateFilters({}),{limit:100,offset:0,batch:'',status:'',search:'',from:'',to:''});
  const f=candidateFilters({status:'accepted',from:'2026-09-30',to:'2026-02-30',limit:'9999',offset:'-4',q:'  receitas ',batch:'b-1'});
  assert.deepEqual(f,{limit:500,offset:0,batch:'b-1',status:'accepted',search:'receitas',from:'2026-09-30',to:''});
  assert.equal(candidateFilters({status:"accepted' OR 1=1"}).status,'');
  assert.equal(candidateFilters({batch:'x'.repeat(201)}).batch,'');
});

test('every read is limited to Query batches and the search-discovery source, with values bound as parameters',()=>{
  const all=candidateScope(candidateFilters({}),['b-1','b-2']);
  assert.match(all.where,/c\.dispatch_batch_id=ANY\(\$1::text\[\]\) AND c\.source_json->>'source'=\$2/);
  assert.deepEqual(all.args,[['b-1','b-2'],'youtube_search_discovery']);
  assert.deepEqual(candidateScope(candidateFilters({batch:'legacy'}),['b-1']).args[0],[],'a non-Query batch selects nothing');
  const scoped=candidateScope(candidateFilters({batch:'b-1',status:'queued',q:'50%_off\\',from:'2026-09-01',to:'2026-09-30'}),['b-1','b-2']);
  assert.deepEqual(scoped.args,[['b-1'],'youtube_search_discovery','queued','%50\\%\\_off\\\\%','2026-09-01','2026-09-30']);
  assert.match(scoped.where,/AT TIME ZONE 'Asia\/Shanghai'/);
  const statistics=candidateScope(candidateFilters({status:'queued'}),[],{includeStatus:false});
  assert.doesNotMatch(statistics.where,/c\.status=/);
  const list=candidateListSql(candidateFilters({limit:'20',offset:'40'}),['b-1']);
  assert.deepEqual(list.args.slice(-2),[21,40]);
  assert.match(list.sql,/ORDER BY c\.created_at DESC,c\.candidate_id DESC\s+LIMIT \$3 OFFSET \$4/);
  assert.doesNotMatch(list.sql,/count\(/);
});

test('list detects a next page without counting, and statistics sum pending validation states',async()=>{
  const reads=[];
  const rows=Array.from({length:3},(_,i)=>({candidate_id:i}));
  const page=await loadCandidateList({filters:candidateFilters({limit:'2'}),read:async(sql,args)=>{reads.push(sql);return {rows:sql.includes('channel_candidates')?rows:[{dispatch_batch_id:'b-1'}]};}});
  assert.equal(page.candidates.length,2);assert.equal(page.hasNext,true);assert.deepEqual(page.batches,[{dispatch_batch_id:'b-1'}]);
  assert.ok(reads.every(sql=>!/count\(/.test(sql)));
  const cache={keys:[],async get(key,load){this.keys.push(key);return {value:await load(),generatedAt:'now',stale:false};}};
  const stats=await loadCandidateStatistics({cache,filters:candidateFilters({status:'queued'}),
    readStatistics:async sql=>{if(!sql.includes('GROUP BY'))return {rows:[{dispatch_batch_id:'b-1'}]};return {rows:[{status:'discovered',n:'2'},{status:'queued',n:'3'},{status:'validating',n:'1'},{status:'accepted',n:'4'},{status:'rejected',n:'5'}]};}});
  assert.equal(stats.all,15);assert.equal(stats.pending,6);assert.equal(stats.total,3);assert.equal(stats.byStatus.existing,0);
  assert.equal(JSON.parse(cache.keys[0]).includes('queued'),false,'status shares one cached aggregate');
});

test('candidate page renders escaped rows, labels and keeps filters in paging links',async()=>{
  const source=await readFile(new URL('./server.js',import.meta.url),'utf8');
  const start=source.indexOf('function candidateListPage('),end=source.indexOf('function publicationComparisonStatus(',start);
  const h=value=>String(value??'').replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;').replaceAll('"','&quot;');
  const render=vm.runInNewContext(source.slice(start,end)+';candidateListPage',{URLSearchParams,h,layout:({body})=>body,fmtInt:String,timeText:v=>v?String(v):'-',
    statusClass:()=> 'warn',channelIdentityCell:c=>`<b>${h(c.title)}</b>`,statisticsPanel:path=>`<section data-statistics-url="${path}"></section>`,
    CANDIDATE_STATUSES,CANDIDATE_STATUS_LABELS});
  const page=render({filters:candidateFilters({status:'rejected',q:'x',limit:'1',offset:'1'}),hasNext:true,batches:[{dispatch_batch_id:'b-1',status:'running'}],
    candidates:[{title:'<script>',status:'rejected',reject_reason:'below_min_subscribers',query_text:'receitas',page_no:2,rank_position:7,dispatch_batch_id:'b-1',search_subscriber_count:null}]});
  assert.match(page,/data-statistics-url="\/api\/candidates\/statistics"/);
  assert.doesNotMatch(page,/<b><script>/);
  assert.match(page,/不符合/);assert.match(page,/below_min_subscribers/);assert.match(page,/第 2 页 · 排名 7/);assert.match(page,/搜索未提供/);
  assert.match(page,/href="\/candidates\?status=rejected&amp;q=x&amp;limit=1&amp;offset=2"/);
  assert.match(page,/href="\/candidates\?status=rejected&amp;q=x&amp;limit=1&amp;offset=0"/);
});
