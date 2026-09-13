import {RemoteProtocolError} from './protocol.js';

// The fixed local fleet is separate from remote deployment counts. Never add
// a requested count to observed capacity: retries would grow it repeatedly.
export function createDeploymentCapacity({pool,client,localChannelSlots}) {
  if(!Number.isInteger(localChannelSlots)||localChannelSlots<0||localChannelSlots>500)throw new TypeError('explicit local channel slot count required');
  return {
    async ensure() {
      const {rows}=await pool.query(`SELECT COALESCE(sum(d.worker_count),0)::int AS workers
        FROM remote_ingestion.node_deployments d JOIN remote_ingestion.nodes n USING(node_id) WHERE n.state='active'`);
      const required=localChannelSlots+rows[0].workers;
      if(required>500)throw new RemoteProtocolError('REMOTE_NETWORK_CAPACITY_LIMIT',409);
      if(required===0)return;
      try {
        const result=await client.ensureCapacity({role:'channel',minimum_slots:required});
        if(result?.ok!==true || result.role!=='channel' || !Number.isInteger(result.provisioned) || result.provisioned<required)throw new Error('capacity not provisioned');
        return {required,provisioned:result.provisioned};
      }catch{throw new RemoteProtocolError('REMOTE_NETWORK_CAPACITY_UNAVAILABLE',503);}
    },
  };
}
