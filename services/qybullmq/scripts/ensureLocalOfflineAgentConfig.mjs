import { fileURLToPath } from "node:url";

import { closeDb, withTransaction } from "../src/db.js";

export const LOCAL_OFFLINE_AGENT_CONFIG_NAME = "local-offline-qy-channel-profile";

export async function ensureLocalOfflineAgentConfig(client) {
  await client.query(
    "SELECT pg_advisory_xact_lock(hashtext($1))",
    [LOCAL_OFFLINE_AGENT_CONFIG_NAME],
  );
  const result = await client.query(
    `INSERT INTO crawler.agent_configs (
       name,provider,model,endpoint,secret_ref,prompt_template_id,
       batch_size,min_batch_size,max_workers,timeout_ms,max_retries,tools_json,
       enabled,is_default,updated_at
     ) VALUES (
       $1,'local-offline','qy-channel-profile',NULL,NULL,NULL,
       20,1,1,600000,2,'[]'::jsonb,
       true,false,now()
     )
     ON CONFLICT (name) DO UPDATE
     SET provider='local-offline',
         model='qy-channel-profile',
         endpoint=NULL,
         secret_ref=NULL,
         prompt_template_id=NULL,
         batch_size=20,
         min_batch_size=1,
         max_workers=1,
         timeout_ms=600000,
         max_retries=2,
         tools_json='[]'::jsonb,
         enabled=true,
         updated_at=now()
     RETURNING config_id,name,provider,model,prompt_template_id,batch_size,
               max_workers,timeout_ms,max_retries,tools_json,enabled,is_default`,
    [LOCAL_OFFLINE_AGENT_CONFIG_NAME],
  );
  return result.rows[0];
}

async function main() {
  if (!process.argv.includes("--apply")) {
    throw new Error("refusing to apply: pass --apply explicitly");
  }
  const config = await withTransaction(ensureLocalOfflineAgentConfig);
  console.log(JSON.stringify({ ok: true, config }));
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  main().catch((error) => {
    console.error(error?.stack || String(error));
    process.exitCode = 1;
  }).finally(async () => {
    await closeDb().catch(() => {});
  });
}
