import test from 'node:test';
import assert from 'node:assert/strict';
import {selectIntakeWorkers,intakeStatus} from '../src/remoteNodes/intakeSelection.js';

test('count changes preserve admitted slots, prefer connected new slots, and validate installed limits',()=>{
 const rows=Array.from({length:20},(_,i)=>({slot:`incremental-${i+1}`,activation_requested:[2,8,12].includes(i),alive:i!==0}));
 const selected=selectIntakeWorkers(rows,5);
 assert.deepEqual(selected.slice(0,3).map(r=>r.slot),['incremental-3','incremental-9','incremental-13']);
 assert.deepEqual(selected.slice(3).map(r=>r.slot),['incremental-2','incremental-4']);
 assert.equal(selectIntakeWorkers(rows,0).length,0);
 for(const n of [-1,21,2.5])assert.throws(()=>selectIntakeWorkers(rows,n),{code:'INVALID_EXECUTION_COUNT'});
});
test('partial reduction distinguishes running, draining, standby and available Workers',()=>{
 const state=intakeStatus([
  {requested:true,connected:true,enabled:true,active:true,readyForTasks:true},
  {requested:false,connected:true,enabled:false,active:true,readyForTasks:false},
  {requested:false,connected:true,enabled:false,active:false,readyForTasks:false},
 ]);
 assert.equal(state.allowedCount,1);assert.equal(state.draining,true);
 assert.deepEqual(state.counts,{deployed:3,connected:3,allowed:1,ready:1,active:2,running:1,draining:1,idle:0,standby:1,collecting:1,processing:0,awaiting:0,unready:0,finishing:1,offline:0,overdue:0,recovering:0,blocked:0});
});

test('centrally pending results are not reported as active network collection',()=>{
 const state=intakeStatus([{requested:true,connected:true,enabled:false,active:true,processing:false,awaitingRecovery:true}]);
 assert.equal(state.counts.collecting,0);assert.equal(state.counts.awaiting,1);assert.equal(state.counts.finishing,0);
});

test('every connected Worker is counted once, including enabled workers without an execution route',()=>{
 const workers=[...Array.from({length:44},()=>({requested:true,connected:true,enabled:true,active:true,processing:true,executionPhase:'collecting'})),
  {requested:true,connected:true,enabled:true,active:true,processing:false,awaitingRecovery:true},
  ...Array.from({length:2},()=>({requested:true,connected:true,enabled:true,active:false,processing:false,readyForTasks:false}))];
 const {counts}=intakeStatus(workers);
 assert.equal(counts.collecting,44);assert.equal(counts.awaiting,1);assert.equal(counts.unready,2);
 assert.equal(['collecting','processing','awaiting','unready','idle','finishing','standby','offline'].reduce((n,k)=>n+counts[k],0),47);
});

test('received results and task preparation are not reported as network collection',()=>{
 const {counts}=intakeStatus(['preparing','processing','collecting'].map(executionPhase=>({requested:true,connected:true,enabled:true,active:true,processing:true,executionPhase})));
 assert.equal(counts.collecting,1);assert.equal(counts.processing,2);
});
