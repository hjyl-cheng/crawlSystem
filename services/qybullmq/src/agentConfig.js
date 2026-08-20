import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { query } from "./db.js";
import {
  CHANNEL_CATEGORY_LEVEL_1_OPTIONS,
  CHANNEL_CATEGORY_OPTIONS,
} from "./agentTaxonomy.js";

const LEGACY_AGENT_PROMPT_TEMPLATE_V2 = `请分析 input_urls 中每一个 YouTube 频道及其对应博主，并为每个链接返回博主画像和受众画像字段。

input_urls:
{{input_urls_json}}

country_required_input_urls:
{{country_required_urls_json}}

country_resolved_input_urls:
{{country_resolved_urls_json}}

你必须逐条处理 input_urls 数组中的每一个链接。

处理规则：
- input_urls 中有多少个链接，最终 JSON 数组就必须有多少个对象。
- 每个对象的 input_url 必须与输入链接一一对应，顺序也必须一致。
- 不允许合并多个链接的结果。
- 不允许跳过任何链接。
- 不允许因为某个链接无法访问就省略该对象。
- 对每个 input_url 都要单独搜索和分析公开信息。
- 所有非 country 字段都必须有内容；除 creator_gender 可按规则返回 unknown 外，其他画像字段禁止返回 unknown、null、N/A、空字符串、空数组。
- country_required_input_urls 中的频道必须分析并返回具体 country。
- country_resolved_input_urls 中的频道已经由 YouTube About 确认 country，不要再次分析，必须返回 country: null。
- 如果公开信息不足，必须基于视频标题、频道名称、频道语言、内容主题、评论/受众语境、公开视频信息做合理预估。
- 预估字段也必须填写具体值。
- 只返回用户要求的画像字段；除 input_url 外，不要返回 channel_url、channel_name、video_title、search_used、confidence 或其他字段。
- 只返回严格 JSON，不要 Markdown，不要解释。

字段定义和取值要求：
- input_url：必须原样复制输入链接，用于和结果一一对应。
- country：仅对 country_required_input_urls 判断频道/创作者主要国家或地区。优先使用 YouTube About、频道简介、社媒、语言、内容语境判断，并输出英文国家名或地区名。country_resolved_input_urls 必须返回 null。
- creator_gender：创作者性别画像。只能输出 "male"、"female"、"unknown"。个人男性用 male，个人女性用 female；多人团队、机构/媒体/俱乐部/公司账号、无法可靠判断性别时统一用 unknown。
- creator_age_range：创作者具体年龄画像。Agent 分析后必须输出一个具体年龄数值，格式为整数。
- creator_language：创作者主要内容语言。输出英文语言名，例如 "Portuguese"、"English"、"Spanish"。
- audience_region：受众国家/地区分布。必须输出数组格式，固定 6 项：Top 5 具体国家/地区 + 1 个 "Other"。percentage 总和必须等于 100。
- audience_age_gender：受众年龄与性别分布。必须输出数组格式，固定 6 个年龄段，且年龄段只能使用以下 6 项并保持顺序一致：18-24、25-34、35-44、45-54、55-64、65+。每一项格式必须为 {"age_range": "18-24", "male": 数字, "female": 数字}。male 表示该年龄段男性占整体受众的百分比，female 表示该年龄段女性占整体受众的百分比；6 个年龄段所有 male + female 的总和必须等于 100。
- audience_language：受众语言分布。必须输出数组格式，所有项 percentage 总和必须等于 100。
- audience_interests：受众兴趣主题分布。必须输出数组格式，固定 5 项兴趣主题，percentage 总和必须等于 100。
- active_subscriber_ratio：活跃粉丝占比。必须输出 0-100 的具体整数百分比。
  【重要：此指标定义为“最近30天内至少观看或互动过一次的订阅粉丝占比”。】
  请遵循以下四步推理法进行严密估算（请在内心完成推理，最终只输出整数数值）：
  1. 计算基础比值：计算近期平均播放量与总订阅数（subscriberCount）的比例（即单视频播放订阅比）。
  2. 考虑时间窗放大效应（3-5倍率）：因为30天内会有多次视频更新，去重后的月度活跃观众数通常是单视频播放量的 3 至 5 倍。因此，需将“基础比值”乘以 3~5 作为初始放大基准。
  3. 考虑频道体量修正（大频道折减）：
     - 订阅数 < 5万：属于中小型频道，粉丝粘性极高，放大基准可维持在较高水平；
     - 订阅数 5万 - 50万：中型频道，放大基准进行 0.8-0.9 的折减；
     - 订阅数 > 50万：大型或超大型频道，死粉沉淀不可避免，放大基准需进行 0.6-0.7 的折减。
  4. 最终区间锚定与修正：
     - 优秀高粘性频道（互动率高、近期更新频繁、垂直度高，如科技、财经、个人Vlog）：最终活跃比应落在 [45 - 75] 区间；
     - 正常健康频道（定期更新、播放数据稳定）：最终活跃比应落在 [25 - 44] 区间；
     - 低活跃/老旧/泛娱乐频道（更新极慢、有很多早期累积的僵尸粉，或影视剪辑、搞笑沙雕图等路人流量为主的频道）：最终活跃比应落在 [8 - 24] 区间；
     - 异常/僵尸粉频道（互动极差、长期停更）：低于 8。
  【绝对禁止】：严禁直接将单视频播放比（如 2% 或 3%）或单纯的互动率数字作为结果填入此字段。必须执行上述放大与修正推理，确保输出数据符合行业真实的月度活跃分布。
- channel_tags：频道内容标签与权重分布。必须输出对象，包含以下两个子字段：
  1. tags：数组格式，固定输出 10 个英文内容标签。标签应精准定义频道的内容领域、创作题材、垂直行业或风格调性，例如 "Python Tutorial"、"Tech Review"、"Machine Learning"、"Gadgets"、"Software Engineering"。
  2. top_5_distribution：数组格式，固定 6 项，代表前 5 个最核心标签的权重比例 + 1 个 "Other"。前 5 项格式为 {"tag": "英文标签名", "percentage": 数字}，tag 必须完全对应 tags 数组中的前 5 个标签；第 6 项必须为 {"tag": "Other", "percentage": 数字}。percentage 必须为 0-100 的整数，且 6 项 percentage 总和必须严格等于 100。请根据博主近期的视频主题分布、播放量占比和发布频率合理估算。
- channel_categories：频道所属分类。必须输出为字符串数组格式，允许选择多个分类，建议选择最符合的 1-4 个主要分类。所有分类词必须严格且只能在以下候选列表中选择，严禁自行创造候选列表之外的新词汇：
  ${JSON.stringify(CHANNEL_CATEGORY_OPTIONS)}

返回格式：
[
  {
    "input_url": "原始输入链接",
    "country": "Brazil",
    "creator_gender": "unknown",
    "creator_age_range": 35,
    "creator_language": "Portuguese",
    "audience_region": [
      {"region": "Brazil", "percentage": 78},
      {"region": "Portugal", "percentage": 7},
      {"region": "United States", "percentage": 5},
      {"region": "Angola", "percentage": 3},
      {"region": "Mozambique", "percentage": 2},
      {"region": "Other", "percentage": 5}
    ],
    "audience_age_gender": [
      {"age_range": "18-24", "male": 3, "female": 12},
      {"age_range": "25-34", "male": 5, "female": 28},
      {"age_range": "35-44", "male": 5, "female": 22},
      {"age_range": "45-54", "male": 3, "female": 13},
      {"age_range": "55-64", "male": 2, "female": 5},
      {"age_range": "65+", "male": 1, "female": 1}
    ],
    "audience_language": [
      {"language": "Portuguese", "percentage": 94},
      {"language": "Spanish", "percentage": 3},
      {"language": "Other", "percentage": 3}
    ],
    "audience_interests": [
      {"interest": "Beauty Tips", "percentage": 30},
      {"interest": "Makeup Tutorial", "percentage": 25},
      {"interest": "Product Reviews", "percentage": 20},
      {"interest": "Skincare", "percentage": 15},
      {"interest": "Lifestyle", "percentage": 10}
    ],
    "active_subscriber_ratio": 38,
    "channel_tags": {
      "tags": ["Makeup Tutorial", "Beauty Tips", "Cosmetics Review", "Mature Skin", "Skincare", "Product Reviews", "Lifestyle", "Short-form Video", "Audience Engagement", "Creator Updates"],
      "top_5_distribution": [
        {"tag": "Makeup Tutorial", "percentage": 25},
        {"tag": "Beauty Tips", "percentage": 22},
        {"tag": "Cosmetics Review", "percentage": 18},
        {"tag": "Mature Skin", "percentage": 14},
        {"tag": "Skincare", "percentage": 11},
        {"tag": "Other", "percentage": 10}
      ]
    },
    "channel_categories": ["Makeup", "Skincare"]
  }
]`;

export const DEFAULT_AGENT_PROMPT_TEMPLATE = readFileSync(
  new URL("./agentPromptTemplate.txt", import.meta.url),
  "utf8",
).trim();

export const DEFAULT_AGENT_OUTPUT_SCHEMA = {
  type: "array",
  items: {
    type: "object",
    additionalProperties: false,
    required: [
      "input_url",
      "country",
      "creator_gender",
      "creator_age_range",
      "creator_language",
      "audience_region",
      "audience_age_gender",
      "audience_language",
      "active_subscriber_ratio",
      "channel_tags",
      "channel_categories",
    ],
    properties: {
      input_url: { type: "string" },
      country: { type: "string", minLength: 1 },
      creator_gender: { type: "string", enum: ["male", "female", "brand_team"] },
      creator_age_range: { type: "integer", minimum: 0 },
      creator_language: { type: "string", minLength: 1 },
      audience_region: { type: "array", minItems: 6, maxItems: 6 },
      audience_age_gender: { type: "array", minItems: 6, maxItems: 6 },
      audience_language: { type: "array", minItems: 1 },
      active_subscriber_ratio: { type: "integer", minimum: 0, maximum: 100 },
      channel_tags: { type: "object" },
      channel_categories: {
        type: "object",
        additionalProperties: false,
        required: ["level_1", "level_2"],
        properties: {
          level_1: { type: "string", enum: CHANNEL_CATEGORY_LEVEL_1_OPTIONS },
          level_2: {
            type: "array",
            minItems: 1,
            maxItems: 3,
            uniqueItems: true,
            items: { type: "string", enum: CHANNEL_CATEGORY_OPTIONS },
          },
        },
      },
    },
  },
};

export function agentPromptHash(templateText) {
  return createHash("sha256").update(String(templateText ?? "")).digest("hex");
}

function intEnv(name, fallback, min, max) {
  const value = Number(process.env[name] ?? fallback);
  if (!Number.isFinite(value)) return fallback;
  return Math.max(min, Math.min(max, Math.floor(value)));
}

async function selectActiveAgentConfig() {
  const rows = await query(
    `SELECT c.*, t.name AS prompt_name, t.version AS prompt_version,
            t.template_text, t.output_schema_json, t.status AS prompt_status,
            NULL AS prompt_hash
     FROM crawler.agent_configs c
     LEFT JOIN crawler.agent_prompt_templates t ON t.template_id = c.prompt_template_id
     WHERE c.enabled = true
     ORDER BY c.config_id ASC
     LIMIT 1`,
  );
  const row = rows.rows[0] ?? null;
  if (!row) return null;
  return {
    ...row,
    prompt_hash: agentPromptHash(row.template_text ?? ""),
  };
}

export async function listEnabledAgentConfigs() {
  const rows = await query(
    `SELECT c.*, t.name AS prompt_name, t.version AS prompt_version,
            t.template_text, t.output_schema_json, t.status AS prompt_status,
            NULL AS prompt_hash
     FROM crawler.agent_configs c
     LEFT JOIN crawler.agent_prompt_templates t ON t.template_id = c.prompt_template_id
     WHERE c.enabled = true
     ORDER BY c.config_id ASC`,
  );
  return rows.rows.map((row) => ({
    ...row,
    prompt_hash: agentPromptHash(row.template_text ?? ""),
  }));
}

export async function getAgentConfigById(configId) {
  const id = Number(configId);
  if (!Number.isFinite(id) || id <= 0) return null;
  const rows = await query(
    `SELECT c.*, t.name AS prompt_name, t.version AS prompt_version,
            t.template_text, t.output_schema_json, t.status AS prompt_status,
            NULL AS prompt_hash
     FROM crawler.agent_configs c
     LEFT JOIN crawler.agent_prompt_templates t ON t.template_id = c.prompt_template_id
     WHERE c.config_id = $1
     LIMIT 1`,
    [Math.floor(id)],
  );
  const row = rows.rows[0] ?? null;
  if (!row) return null;
  return {
    ...row,
    prompt_hash: agentPromptHash(row.template_text ?? ""),
  };
}

export async function getLocalOfflineAgentConfig() {
  const rows = await query(
    `SELECT c.*,NULL::text AS prompt_name,NULL::int AS prompt_version,
            NULL::text AS template_text,NULL::jsonb AS output_schema_json,
            NULL::text AS prompt_status,NULL::text AS prompt_hash
     FROM crawler.agent_configs c
     WHERE c.enabled=true AND lower(c.provider)='local-offline'
     ORDER BY c.is_default DESC,c.config_id ASC
     LIMIT 1`,
  );
  return rows.rows[0] ?? null;
}

export async function ensureDefaultAgentConfig() {
  const existing = await selectActiveAgentConfig();
  if (existing) return existing;
  const configCount = await query("SELECT count(*)::int AS count FROM crawler.agent_configs");
  if (Number(configCount.rows[0]?.count ?? 0) > 0) return null;

  let template = await query(
    `SELECT *
     FROM crawler.agent_prompt_templates
     WHERE is_default = true AND status = 'active'
     ORDER BY updated_at DESC
     LIMIT 1`,
  );

  if (template.rows.length === 0) {
    template = await query(
      `INSERT INTO crawler.agent_prompt_templates (
         name, version, template_text, output_schema_json, status, is_default, updated_at
       )
       VALUES ('Default YouTube Agent Prompt', 1, $1, $2::jsonb, 'active', true, now())
       ON CONFLICT (name, version)
       DO UPDATE SET template_text = EXCLUDED.template_text,
                     output_schema_json = EXCLUDED.output_schema_json,
                     status = 'active',
                     is_default = true,
                     updated_at = now()
       RETURNING *`,
      [DEFAULT_AGENT_PROMPT_TEMPLATE, JSON.stringify(DEFAULT_AGENT_OUTPUT_SCHEMA)],
    );
  }

  const templateId = template.rows[0].template_id;
  const config = await query(
    `SELECT *
     FROM crawler.agent_configs
     WHERE is_default = true
     LIMIT 1`,
  );

  if (config.rows.length === 0) {
    const batchSize = intEnv("AGENT_BATCH_SIZE", 30, 1, 50);
    await query(
      `INSERT INTO crawler.agent_configs (
         name, provider, model, endpoint, secret_ref, prompt_template_id,
         batch_size, min_batch_size, max_workers, timeout_ms, max_retries, tools_json,
         enabled, is_default, updated_at
       )
       VALUES (
         'default', 'rules', 'rules-agent-v1', NULL, NULL, $1,
         $2, $3, 1, 120000, 2, '[{"type":"web_search"}]'::jsonb,
         true, false, now()
       )
       ON CONFLICT (name)
       DO UPDATE SET prompt_template_id = COALESCE(crawler.agent_configs.prompt_template_id, EXCLUDED.prompt_template_id),
                     updated_at = now()`,
      [
        templateId,
        batchSize,
        batchSize,
      ],
    );
  }

  return selectActiveAgentConfig();
}

export async function getActiveAgentConfig() {
  return (await selectActiveAgentConfig()) ?? (await ensureDefaultAgentConfig());
}

export async function createAgentTemplateDraft({ name, templateText, outputSchemaJson }) {
  const cleanName = String(name || "YouTube Agent Prompt").trim();
  const cleanTemplate = String(templateText || "").trim();
  if (!cleanTemplate) throw new Error("template_text is required");
  const versionRows = await query(
    `SELECT COALESCE(MAX(version), 0) + 1 AS next_version
     FROM crawler.agent_prompt_templates
     WHERE name = $1`,
    [cleanName],
  );
  const version = Number(versionRows.rows[0]?.next_version ?? 1);
  const row = await query(
    `INSERT INTO crawler.agent_prompt_templates (
       name, version, template_text, output_schema_json, status, is_default, updated_at
     )
     VALUES ($1, $2, $3, $4::jsonb, 'draft', false, now())
     RETURNING *`,
    [
      cleanName,
      version,
      cleanTemplate,
      JSON.stringify(outputSchemaJson ?? DEFAULT_AGENT_OUTPUT_SCHEMA),
    ],
  );
  return row.rows[0];
}

export async function publishAgentTemplate(templateId) {
  const id = Number(templateId);
  if (!Number.isFinite(id) || id <= 0) throw new Error("valid template_id is required");
  const found = await query(
    `SELECT *
     FROM crawler.agent_prompt_templates
     WHERE template_id = $1
     LIMIT 1`,
    [id],
  );
  if (found.rows.length === 0) throw new Error("template not found");

  await query(
    `UPDATE crawler.agent_prompt_templates
     SET status = CASE WHEN status = 'active' THEN 'archived' ELSE status END,
         is_default = false,
         updated_at = now()
     WHERE is_default = true OR status = 'active'`,
  );
  await query(
    `UPDATE crawler.agent_prompt_templates
     SET status = 'active',
         is_default = true,
         updated_at = now()
     WHERE template_id = $1`,
    [id],
  );
  await query(
    `UPDATE crawler.agent_configs
     SET prompt_template_id = $1, updated_at = now()
     WHERE is_default = true`,
    [id],
  );
  return (await query(
    `SELECT *
     FROM crawler.agent_prompt_templates
     WHERE template_id = $1`,
    [id],
  )).rows[0];
}

export async function updateDefaultAgentConfig(values) {
  await ensureDefaultAgentConfig();
  const active = await getActiveAgentConfig();
  const promptTemplateId = Number(values.prompt_template_id ?? active.prompt_template_id);
  const next = {
    provider: String(values.provider ?? active.provider ?? "rules").trim() || "rules",
    model: String(values.model ?? active.model ?? "rules-agent-v1").trim() || "rules-agent-v1",
    endpoint: String(values.endpoint ?? active.endpoint ?? "").trim() || null,
    secret_ref: String(values.secret_ref ?? active.secret_ref ?? "").trim() || null,
    prompt_template_id: Number.isFinite(promptTemplateId) && promptTemplateId > 0 ? promptTemplateId : active.prompt_template_id,
    batch_size: Math.max(1, Math.min(50, Math.floor(Number(values.batch_size ?? active.batch_size ?? 50)))),
    timeout_ms: Math.max(5000, Math.min(3600000, Math.floor(Number(values.timeout_ms ?? active.timeout_ms ?? 120000)))),
    max_retries: Math.max(0, Math.min(10, Math.floor(Number(values.max_retries ?? active.max_retries ?? 2)))),
    tools_json: values.tools_json ?? active.tools_json ?? [{ type: "web_search" }],
    enabled: values.enabled == null ? Boolean(active.enabled) : Boolean(values.enabled),
  };
  next.min_batch_size = next.batch_size;

  const row = await query(
    `UPDATE crawler.agent_configs
     SET provider = $2,
         model = $3,
         endpoint = $4,
         secret_ref = $5,
         prompt_template_id = $6,
         batch_size = $7,
         min_batch_size = $8,
         timeout_ms = $9,
         max_retries = $10,
         tools_json = $11::jsonb,
         enabled = $12,
         updated_at = now()
     WHERE config_id = $1
     RETURNING *`,
    [
      active.config_id,
      next.provider,
      next.model,
      next.endpoint,
      next.secret_ref,
      next.prompt_template_id,
      next.batch_size,
      next.min_batch_size,
      next.timeout_ms,
      next.max_retries,
      JSON.stringify(next.tools_json),
      next.enabled,
    ],
  );
  return row.rows[0];
}
