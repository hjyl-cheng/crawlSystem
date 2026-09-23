import {performance} from 'node:perf_hooks';

// Diagnostic state, never a substitute for persisted task/route ownership.
export class ExecutionProgress {
  constructor({attemptId,jobId,claimTimeoutMs,stopTimeoutMs,admissionTimeoutMs=30000,now=()=>performance.now()}) {
    Object.assign(this,{attemptId,jobId,claimTimeoutMs,stopTimeoutMs,admissionTimeoutMs,now});
    this.sequence=0;this.cancellationRequested=false;this.recoveryReason=null;
    this.move('admitting','pending_country_handoff');
  }
  move(phase,operation=null) {
    const now=this.now();const utc=new Date().toISOString();
    if(this.phase!==phase){this.phase=phase;this.phaseAt=now;this.phaseStartedAt=utc;}
    this.operation=operation;this.operationAt=now;this.operationStartedAt=operation?utc:null;
    this.progressAt=now;this.lastProgressAt=utc;this.sequence++;
  }
  cancel(reason) {
    if(this.cancellationRequested)return;
    this.cancellationRequested=true;this.cancelledAt=this.now();this.recoveryReason=reason;
  }
  snapshot() {
    const now=this.now();const phaseAge=now-this.phaseAt;
    const limit=this.phase==='admitting'?this.admissionTimeoutMs:this.phase==='awaiting_claim'?this.claimTimeoutMs
      : ['binding','stopping','recovering'].includes(this.phase)?this.stopTimeoutMs
      : ['collecting','receiving','applying'].includes(this.phase)?15*60*1000:null;
    const blocked=this.phase==='blocked' || (['stopping','recovering'].includes(this.phase) && phaseAge>=this.stopTimeoutMs) || (this.cancellationRequested && this.phase!=='finished' && now-this.cancelledAt>=this.stopTimeoutMs);
    const overdue=limit!==null && phaseAge>=limit;
    return {attemptId:this.attemptId,jobId:this.jobId,taskId:this.taskId??null,generation:this.generation??null,
      executionPhase:blocked?'blocked':this.phase,phaseStartedAt:this.phaseStartedAt,lastProgressAt:this.lastProgressAt,
      progressAgeSeconds:Math.max(0,(now-this.progressAt)/1000),phaseAgeMs:phaseAge,progressSequence:this.sequence,
      currentOperation:this.operation,operationStartedAt:this.operationStartedAt,
      operationAgeSeconds:this.operation?Math.max(0,(now-this.operationAt)/1000):null,
      progressHealth:blocked?'blocked':overdue?'overdue':(this.cancellationRequested && this.phase!=='finished') || ['stopping','recovering'].includes(this.phase)?'recovering':'healthy',
      cancellationRequested:this.cancellationRequested,recoveryReason:this.recoveryReason,
      canAbort:!this.cancellationRequested && overdue && ['admitting','awaiting_claim'].includes(this.phase)};
  }
}
