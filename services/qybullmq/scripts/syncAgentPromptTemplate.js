import {
  DEFAULT_AGENT_OUTPUT_SCHEMA,
  DEFAULT_AGENT_PROMPT_TEMPLATE,
  agentPromptHash,
} from "../src/agentConfig.js";
import { closeDb, withTransaction } from "../src/db.js";

const TEMPLATE_NAME = "YouTube Agent Prompt - 3.txt";
const TARGET_CONFIG_NAMES = ["default", "grokapi-console"];

try {
  const result = await withTransaction(async (client) => {
    await client.query("SELECT pg_advisory_xact_lock(hashtext($1))", [TEMPLATE_NAME]);

    const existingConfigs = await client.query(
      `SELECT config_id, name
       FROM crawler.agent_configs
       WHERE name = ANY($1::text[])
       ORDER BY config_id`,
      [TARGET_CONFIG_NAMES],
    );
    const existingNames = new Set(existingConfigs.rows.map((row) => row.name));
    const missingNames = TARGET_CONFIG_NAMES.filter((name) => !existingNames.has(name));
    if (missingNames.length > 0) {
      throw new Error(`agent configs not found: ${missingNames.join(", ")}`);
    }

    const outputSchemaJson = JSON.stringify(DEFAULT_AGENT_OUTPUT_SCHEMA);
    const matchingTemplate = await client.query(
      `SELECT template_id, name, version
       FROM crawler.agent_prompt_templates
       WHERE name=$1
         AND template_text=$2
         AND output_schema_json=$3::jsonb
       ORDER BY version DESC
       LIMIT 1`,
      [TEMPLATE_NAME, DEFAULT_AGENT_PROMPT_TEMPLATE, outputSchemaJson],
    );

    let templateRow = matchingTemplate.rows[0];
    if (!templateRow) {
      const versionResult = await client.query(
        `SELECT COALESCE(MAX(version), 0) + 1 AS next_version
         FROM crawler.agent_prompt_templates
         WHERE name=$1`,
        [TEMPLATE_NAME],
      );
      const version = Number(versionResult.rows[0]?.next_version ?? 1);
      const inserted = await client.query(
        `INSERT INTO crawler.agent_prompt_templates (
           name, version, template_text, output_schema_json, status, is_default, updated_at
         )
         VALUES ($1, $2, $3, $4::jsonb, 'draft', false, now())
         RETURNING template_id, name, version`,
        [TEMPLATE_NAME, version, DEFAULT_AGENT_PROMPT_TEMPLATE, outputSchemaJson],
      );
      templateRow = inserted.rows[0];
    }

    await client.query(
      `UPDATE crawler.agent_prompt_templates
       SET status=CASE WHEN status='active' THEN 'archived' ELSE status END,
           is_default=false,
           updated_at=now()
       WHERE status='active' OR is_default=true`,
    );
    await client.query(
      `UPDATE crawler.agent_prompt_templates
       SET status='active', is_default=true, updated_at=now()
       WHERE template_id=$1`,
      [templateRow.template_id],
    );
    const configs = await client.query(
      `UPDATE crawler.agent_configs
       SET prompt_template_id=$1, updated_at=now()
       WHERE name = ANY($2::text[])
       RETURNING config_id, name, enabled`,
      [templateRow.template_id, TARGET_CONFIG_NAMES],
    );
    return {
      template: templateRow,
      configs: configs.rows,
    };
  });
  console.log(JSON.stringify({
    ok: true,
    prompt_hash: agentPromptHash(DEFAULT_AGENT_PROMPT_TEMPLATE),
    prompt_chars: DEFAULT_AGENT_PROMPT_TEMPLATE.length,
    ...result,
  }));
} finally {
  await closeDb();
}
