// Independent advisory locks share a bounded number of sessions. Every operation
// on one session, including a whole recovery transaction, runs exclusively.
export class SupervisionGuards {
  constructor({pool,groups=4}) {
    if(!pool || !Number.isSafeInteger(groups) || groups<1)throw new TypeError('guard pool and positive group count required');
    this.pool=pool;this.groups=groups;this.sessions=new Map();this.closed=false;
    // The caller may reserve additional long-lived sessions (the full-crawl
    // release guard shares this pool). Never consume that reservation.
    if(pool.options)pool.options.max=Math.max(pool.options.max??0,groups);
  }
  groupFor(key) {
    let hash=2166136261;
    for(const byte of Buffer.from(key))hash=Math.imul(hash^byte,16777619)>>>0;
    return hash%this.groups;
  }
  canAcquire(key) { return !this.sessions.get(this.groupFor(key))?.pending; }
  lost(group) {
    if(group.lost)return;
    group.lost=true;
    if(this.sessions.get(group.id)===group)this.sessions.delete(group.id);
    // Invalidate every member synchronously, before any asynchronous cleanup.
    for(const lease of group.leases.values()){lease.alive=false;try{lease.onLost();}catch{}}
    group.leases.clear();
    if(group.client){const client=group.client;group.client=null;client.release(true);}
  }
  run(group,action) {
    group.pending++;
    const task=group.tail.then(async()=>{
      if(group.lost)throw new Error('SUPERVISION_SESSION_LOST');
      return action();
    });
    const settled=task.finally(()=>{group.pending--;});
    group.tail=settled.catch(()=>{});return settled;
  }
  async acquire(key,onLost) {
    if(this.closed)throw new Error('SUPERVISION_GUARDS_CLOSED');
    const id=this.groupFor(key);
    let group=this.sessions.get(id);
    if(!group){group={id,client:null,tail:Promise.resolve(),pending:0,leases:new Map(),lost:false};this.sessions.set(id,group);}
    return this.run(group,async()=>{
      if(this.closed)throw new Error('SUPERVISION_GUARDS_CLOSED');
      if(group.leases.has(key))return null; // PG locks are reentrant; never acquire twice.
      if(!group.client){
        try{
          group.client=await this.pool.connect();
          group.client.on('error',()=>this.lost(group));
          group.client.on('end',()=>this.lost(group));
        }catch(error){this.lost(group);throw error;}
      }
      let row;
      try{row=(await group.client.query('SELECT pg_try_advisory_lock(781138012,hashtext($1)) AS locked,pg_backend_pid() AS pid',[key])).rows[0];}
      catch(error){this.lost(group);throw error;}
      if(group.lost)throw new Error('SUPERVISION_SESSION_LOST');
      if(!row.locked)return null;
      const lease={alive:true,backendPid:row.pid,onLost,
        withSession:action=>this.run(group,async()=>{
          if(!lease.alive)throw new Error('SUPERVISION_SESSION_LOST');
          try{return await action(group.client);}
          finally{
            // A failed recovery must never leave an open transaction for another
            // slot. Recovery normally commits/rolls back; this is a final fence.
            if(!group.lost)try{await group.client.query('ROLLBACK');}catch{this.lost(group);}
          }
        }),
        release:()=>{
          if(!lease.alive)return Promise.resolve();
          lease.alive=false;
          return this.run(group,async()=>{
            try{await group.client.query('SELECT pg_advisory_unlock(781138012,hashtext($1))',[key]);group.leases.delete(key);}
            catch(error){this.lost(group);throw error;}
          });
        }};
      group.leases.set(key,lease);return lease;
    });
  }
  async close() {
    this.closed=true;
    const groups=[...this.sessions.values()];
    await Promise.all(groups.map(g=>g.tail));
    for(const group of groups)this.lost(group);
  }
}
