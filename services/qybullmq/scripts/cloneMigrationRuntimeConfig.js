import { createHash } from "node:crypto";
import pg from "pg";

const { Pool } = pg;

const sourceUrl = String(process.env.SOURCE_DATABASE_URL || "").trim();
const targetUrl = String(process.env.TARGET_DATABASE_URL || "").trim();
const pipelineCycleId = String(process.env.MIGRATION_PIPELINE_CYCLE_ID || "legacy-results-full-v1").trim();

if (!sourceUrl || !targetUrl) throw new Error("SOURCE_DATABASE_URL and TARGET_DATABASE_URL are required");
if (!pipelineCycleId) throw new Error("MIGRATION_PIPELINE_CYCLE_ID is required");

const source = new Pool({
  connectionString: sourceUrl,
  max: 1,
  options: "-c default_transaction_read_only=on -c statement_timeout=10000",
});
const target = new Pool({ connectionString: targetUrl, max: 1 });

function digest(rows) {
  return createHash("sha256").update(JSON.stringify(rows)).digest("hex");
}

async function main() {
  const [templates, configs, settings] = await Promise.all([
    source.query(`
      SELECT template_id,name,version,template_text,output_schema_json,status,is_default,created_at,updated_at
      FROM crawler.agent_prompt_templates
      ORDER BY template_id
    `),
    source.query(`
      SELECT config_id,name,provider,model,endpoint,secret_ref,prompt_template_id,
             batch_size,min_batch_size,max_workers,timeout_ms,max_retries,tools_json,
             enabled,is_default,created_at,updated_at
      FROM crawler.agent_configs
      ORDER BY config_id
    `),
    source.query(`
      SELECT setting_key,value_json,updated_at
      FROM crawler.settings
      WHERE setting_key<>'query_scheduler'
      ORDER BY setting_key
    `),
  ]);

  const client = await target.connect();
  try {
    await client.query("BEGIN");
    const existing = await client.query(`
      SELECT
        (SELECT count(*)::int FROM crawler.agent_prompt_templates) AS templates,
        (SELECT count(*)::int FROM crawler.agent_configs) AS configs,
        (SELECT count(*)::int FROM crawler.settings) AS settings,
        (SELECT count(*)::int FROM crawler.agent_profiles) AS profiles
    `);
    const state = existing.rows[0];
    if (Object.values(state).some((value) => Number(value) !== 0)) {
      throw new Error(`migration runtime config target is not empty: ${JSON.stringify(state)}`);
    }

    for (const row of templates.rows) {
      await client.query(`
        INSERT INTO crawler.agent_prompt_templates (
          template_id,name,version,template_text,output_schema_json,status,is_default,created_at,updated_at
        ) VALUES ($1,$2,$3,$4,$5::jsonb,$6,$7,$8,$9)
      `, [
        row.template_id,
        row.name,
        row.version,
        row.template_text,
        JSON.stringify(row.output_schema_json ?? {}),
        row.status,
        row.is_default,
        row.created_at,
        row.updated_at,
      ]);
    }

    for (const row of configs.rows) {
      await client.query(`
        INSERT INTO crawler.agent_configs (
          config_id,name,provider,model,endpoint,secret_ref,prompt_template_id,
          batch_size,min_batch_size,max_workers,timeout_ms,max_retries,tools_json,
          enabled,is_default,created_at,updated_at
        ) VALUES (
          $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13::jsonb,$14,$15,$16,$17
        )
      `, [
        row.config_id,
        row.name,
        row.provider,
        row.model,
        row.endpoint,
        row.secret_ref,
        row.prompt_template_id,
        row.batch_size,
        row.min_batch_size,
        row.max_workers,
        row.timeout_ms,
        row.max_retries,
        JSON.stringify(row.tools_json ?? []),
        row.enabled,
        row.is_default,
        row.created_at,
        row.updated_at,
      ]);
    }

    for (const row of settings.rows) {
      await client.query(
        "INSERT INTO crawler.settings (setting_key,value_json,updated_at) VALUES ($1,$2::jsonb,$3)",
        [row.setting_key, JSON.stringify(row.value_json ?? {}), row.updated_at],
      );
    }

    await client.query(
      "INSERT INTO crawler.settings (setting_key,value_json,updated_at) VALUES ('query_scheduler',$1::jsonb,now())",
      [JSON.stringify({
        status: "stopped",
        query_set_id: null,
        query_quality_min_score: 0,
        chunk_size: 3,
        max_discover_backlog: 3,
        pipeline_cycle_id: pipelineCycleId,
        stop_reason: "qy_control_plane_initialized",
        updated_at: new Date().toISOString(),
        updated_by: "cloneMigrationRuntimeConfig",
      })],
    );
    await client.query(`
      SELECT setval(
        pg_get_serial_sequence('crawler.agent_prompt_templates','template_id'),
        COALESCE((SELECT max(template_id) FROM crawler.agent_prompt_templates),1),
        EXISTS (SELECT 1 FROM crawler.agent_prompt_templates)
      )
    `);
    await client.query(`
      SELECT setval(
        pg_get_serial_sequence('crawler.agent_configs','config_id'),
        COALESCE((SELECT max(config_id) FROM crawler.agent_configs),1),
        EXISTS (SELECT 1 FROM crawler.agent_configs)
      )
    `);
    await client.query("COMMIT");

    console.log(JSON.stringify({
      prompt_templates: templates.rows.length,
      agent_configs: configs.rows.length,
      settings: settings.rows.length + 1,
      query_scheduler_status: "stopped",
      pipeline_cycle_id: pipelineCycleId,
      prompt_templates_sha256: digest(templates.rows),
      agent_configs_sha256: digest(configs.rows),
      settings_sha256: digest(settings.rows),
    }));
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}

try {
  await main();
} finally {
  await Promise.allSettled([source.end(), target.end()]);
}
