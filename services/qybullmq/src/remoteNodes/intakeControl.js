import {RemoteProtocolError} from './protocol.js';

// The stored limit and the operator's start/pause switch are independent.
// Existing desired slot rows remain the execution gate for every collector.
export function intakeControl(record, currentCount, workerCount) {
  return {configuredCount:Math.min(record?.configured_count ?? (currentCount || workerCount),workerCount),
    intakeEnabled:record?.intake_enabled ?? currentCount>0};
}

export function changeIntakeControl(previous,value) {
  const byCount=Number.isInteger(value.allowedCount);
  const next={...previous};
  if(byCount){
    if(value.allowedCount<0 || value.allowedCount>value.workerCount)throw new RemoteProtocolError('INVALID_EXECUTION_COUNT',400);
    if(previous.configuredCount!==value.expectedAllowedCount && previous.configuredCount!==value.allowedCount)throw new RemoteProtocolError('EXECUTION_CONTROL_CHANGED');
    next.configuredCount=value.allowedCount;
  }else{
    if(previous.intakeEnabled!==value.expectedRequested && previous.intakeEnabled!==value.enabled)throw new RemoteProtocolError('EXECUTION_CONTROL_CHANGED');
    if(value.enabled && !previous.configuredCount)throw new RemoteProtocolError('INVALID_EXECUTION_COUNT',400);
    next.intakeEnabled=value.enabled;
  }
  return {...next,effectiveCount:next.intakeEnabled?next.configuredCount:0};
}

export async function readIntakeControl(client,nodeKey,currentCount,workerCount) {
  const row=(await client.query('SELECT configured_count,intake_enabled FROM remote_ingestion.intake_controls WHERE node_key=$1',[nodeKey])).rows[0];
  return intakeControl(row,currentCount,workerCount);
}
export async function saveIntakeControl(client,nodeKey,state) {
  await client.query(`INSERT INTO remote_ingestion.intake_controls(node_key,configured_count,intake_enabled)
    VALUES($1,$2,$3) ON CONFLICT(node_key) DO UPDATE SET configured_count=EXCLUDED.configured_count,
    intake_enabled=EXCLUDED.intake_enabled,updated_at=clock_timestamp()`,[nodeKey,state.configuredCount,state.intakeEnabled]);
}
