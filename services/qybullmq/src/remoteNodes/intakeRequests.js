// No network calls or collection work belong in these short transactions.
// Holding the desired row prevents an older reconciliation from overwriting a
// newer request. Busy Worker rows are revisited after they finish their write.
export async function reconcileIntakeRequests(store, nodeId = null) {
  return store.transaction(async client => {
    const requests=(await client.query(`SELECT * FROM remote_ingestion.node_intake_requests
      WHERE ($1::uuid IS NULL OR node_id=$1) AND EXISTS (
        SELECT 1 FROM remote_ingestion.worker_connections w WHERE w.node_id=node_intake_requests.node_id
          AND w.deployment_id=node_intake_requests.deployment_id
          AND w.retirement_id IS NULL AND w.activation_requested IS DISTINCT FROM (w.slot=ANY(node_intake_requests.selected_slots))
      ) ORDER BY node_id LIMIT 100 FOR UPDATE SKIP LOCKED`,[nodeId])).rows;
    let changed=0;
    for(const request of requests){
      const result=await client.query(`WITH available AS (
        SELECT node_id,slot FROM remote_ingestion.worker_connections
        WHERE node_id=$1 AND deployment_id=$2 AND retirement_id IS NULL
          AND activation_requested IS DISTINCT FROM (slot=ANY($3::text[]))
        ORDER BY slot FOR UPDATE SKIP LOCKED
      ) UPDATE remote_ingestion.worker_connections w
        SET activation_requested=(w.slot=ANY($3::text[])) FROM available a
        WHERE w.node_id=a.node_id AND w.slot=a.slot`,[request.node_id,request.deployment_id,request.selected_slots]);
      changed+=result.rowCount;
    }
    return changed;
  });
}
