import {RemoteProtocolError} from './protocol.js';

// Worker count is the capacity.  The database row only retains the operator's
// start/pause switch for backwards compatibility with the old schema.
export function intakeControl(record, currentCount, workerCount) {
  const count=Number.isSafeInteger(workerCount)&&workerCount>=0?workerCount:Math.max(0,currentCount||0);
  return {configuredCount:count, intakeEnabled:record?.intake_enabled ?? currentCount>0};
}

export function changeIntakeControl(previous,value,workerCount=value?.workerCount) {
  if(Number.isInteger(value?.allowedCount)||Number.isInteger(value?.expectedAllowedCount))
    throw new RemoteProtocolError('EXECUTION_COUNT_CONTROL_REMOVED',400);
  if(typeof value?.enabled!=='boolean'||typeof value?.expectedRequested!=='boolean')
    throw new RemoteProtocolError('INVALID_EXECUTION_CONTROL',400);
  if(previous.intakeEnabled!==value.expectedRequested && previous.intakeEnabled!==value.enabled)
    throw new RemoteProtocolError('EXECUTION_CONTROL_CHANGED');
  const count=Number.isSafeInteger(workerCount)&&workerCount>=0?workerCount:previous.configuredCount;
  return {configuredCount:count,intakeEnabled:value.enabled,effectiveCount:value.enabled?count:0};
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
