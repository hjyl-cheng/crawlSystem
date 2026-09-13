import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import {readFile} from 'node:fs/promises';
const source=await readFile(new URL('./server.js',import.meta.url),'utf8');
function loader(name,next,context){
 const start=source.indexOf(`async function ${name}(`),end=source.indexOf(`async function ${next}(`,start);
 assert.ok(start>=0&&end>start);return vm.runInNewContext(source.slice(start,end)+`;${name}`,context);
}
const rows=Array.from({length:3},(_,i)=>({channel_id:`channel-${i}`}));
const helpers={intValue:(v,d)=>v===undefined?d:Number(v),utcDayOffset:()=> '2026-09-12',normalizeClockFilter:()=> 'all',console,
 addChannelStatusFilter:()=>{},MIGRATION_FINALIZED_SQL:"fp.status IN ('ready_auto','ready_partial')",DORMANT_REASON:'dormant'};
test('daily Clock first page survives unavailable full statistics and determines next page independently',async()=>{
 let reads=0;
 const load=loader('dailyClockListData','channelListData',{...helpers,dailyClockScopeSql:()=> 'WITH scope AS (SELECT 1)',pool:{query:async(sql,args)=>{reads++;assert.ok(!sql.includes('count(*)'),'first page must not run whole-list statistics');assert.equal(args.at(-2),3);return {rows};}}});
 const page=await load({query:{limit:'2'}});assert.equal(page.available,true);assert.equal(page.channels.length,2);assert.equal(page.hasNext,true);assert.equal(page.total,null);assert.equal(reads,1);
});
test('channel first page never waits for or executes whole-list statistics',async()=>{
 const load=loader('channelListData','queryDashboardData',{...helpers,db:()=>{throw Error('statistics unavailable');},channelSummaryRows:async args=>{assert.equal(args.limit,3);return rows;}});
 const page=await load({query:{limit:'2'}});assert.equal(page.channels.length,2);assert.equal(page.hasNext,true);assert.equal(page.total,null);
});
test('short final page has no next link even while statistics are unavailable',async()=>{
 const load=loader('channelListData','queryDashboardData',{...helpers,db:()=>{throw Error('statistics unavailable');},channelSummaryRows:async()=>rows.slice(0,1)});
 const page=await load({query:{limit:'2',offset:'10'}});assert.equal(page.hasNext,false);assert.equal(page.channels.length,1);
});

test('Migration statistics still mount when the list query is unavailable',()=>{
 const start=source.indexOf('function migrationChannelListPage('),end=source.indexOf('function migrationChannelDetailPage(',start);
 const render=vm.runInNewContext(source.slice(start,end)+';migrationChannelListPage',{
   URLSearchParams,MIGRATION_WORK_STATUSES:[],h:String,layout:({body})=>body,
   statisticsPanel:()=>'<section data-statistics-url="/stats">loading</section>',
   migrationBatchPanel:()=>'',renderMigrationSystemRetries:()=>'',
 });
 const page=render({configured:true,available:false,channels:[],filters:{limit:50,offset:0},error:'list unavailable'});
 assert.match(page,/data-statistics-url/);assert.match(page,/list unavailable/);
});
