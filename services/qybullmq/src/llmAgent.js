import {
  CHANNEL_CATEGORY_LEVEL_1_OPTIONS,
  CHANNEL_CATEGORY_OPTIONS,
  CHANNEL_CATEGORY_TREE,
  DEFAULT_CHANNEL_CATEGORY,
} from "./agentTaxonomy.js";
import { countryRequiresAgent, normalizeCrawlerCountry } from "./agentCountryPolicy.js";
import {
  buildFirstPartyIdentityContext,
  compactFirstPartyIdentity,
} from "./agentIdentityContext.js";
import { agentRetryDelayMs, classifyAgentFailure } from "./agentRetryPolicy.js";
import { persistentFetch } from "./httpClient.js";

export { buildFirstPartyIdentityContext, compactFirstPartyIdentity };

const AGE_GENDER_LABELS = ["18-24", "25-34", "35-44", "45-54", "55-64", "65+"];
const ALLOWED_CATEGORY_LEVEL_1 = new Set(CHANNEL_CATEGORY_LEVEL_1_OPTIONS);
const ALLOWED_CATEGORIES = new Set(CHANNEL_CATEGORY_OPTIONS);
const CATEGORY_PARENT = new Map(
  Object.entries(CHANNEL_CATEGORY_TREE).flatMap(([level1, level2Items]) => (
    level2Items.map((level2) => [level2, level1])
  )),
);

function text(value) {
  const out = String(value ?? "").trim();
  return out || null;
}

function inputContentIds(item) {
  return [...new Set(
    (Array.isArray(item?.input_content_ids) ? item.input_content_ids : [])
      .map(text)
      .filter(Boolean),
  )].sort();
}

function int(value) {
  const n = Number(value);
  return Number.isFinite(n) ? Math.round(n) : null;
}

function percent(value, fallback = 0) {
  const n = int(value);
  return Math.max(0, Math.min(100, n == null ? fallback : n));
}

function normalizePercentTotal(items, valueKey, labelKey, labels = null) {
  const source = Array.isArray(items) ? items : [];
  const rows = labels
    ? labels.map((label, index) => ({
      [labelKey]: label,
      [valueKey]: percent(source.find((item) => item?.[labelKey] === label)?.[valueKey] ?? source[index]?.[valueKey], 0),
    }))
    : source.map((item) => ({
      [labelKey]: text(item?.[labelKey]) ?? "Other",
      [valueKey]: percent(item?.[valueKey], 0),
    }));
  if (rows.length === 0) return [];
  const sum = rows.reduce((total, item) => total + Number(item[valueKey] ?? 0), 0);
  if (sum <= 0) {
    const base = Math.floor(100 / rows.length);
    return rows.map((item, index) => ({ ...item, [valueKey]: index === rows.length - 1 ? 100 - base * (rows.length - 1) : base }));
  }
  let running = 0;
  return rows.map((item, index) => {
    if (index === rows.length - 1) return { ...item, [valueKey]: 100 - running };
    const value = Math.round((Number(item[valueKey] ?? 0) / sum) * 100);
    running += value;
    return { ...item, [valueKey]: value };
  });
}

function normalizeAudienceAgeGender(agent) {
  const source = Array.isArray(agent?.audience_age_gender) ? agent.audience_age_gender : [];
  const rows = AGE_GENDER_LABELS.map((label, index) => {
    const item = source.find((row) => row?.age_range === label) ?? source[index] ?? {};
    return {
      age_range: label,
      male: percent(item.male, 0),
      female: percent(item.female, 0),
    };
  });
  const sum = rows.reduce((total, item) => total + item.male + item.female, 0);
  if (sum <= 0) {
    return AGE_GENDER_LABELS.map((label, index) => ({
      age_range: label,
      male: [8, 14, 10, 7, 4, 7][index],
      female: [8, 14, 10, 7, 4, 7][index],
    }));
  }
  let running = 0;
  return rows.map((item, index) => {
    if (index === rows.length - 1) {
      const remaining = Math.max(0, 100 - running);
      const current = Math.max(1, item.male + item.female);
      const male = Math.round((item.male / current) * remaining);
      return { ...item, male, female: remaining - male };
    }
    const current = Math.max(1, item.male + item.female);
    const total = Math.round(((item.male + item.female) / sum) * 100);
    const male = Math.round((item.male / current) * total);
    running += total;
    return { ...item, male, female: total - male };
  });
}

function normalizeChannelTags(agent) {
  const raw = agent?.channel_tags;
  const rawTags = Array.isArray(raw?.tags) ? raw.tags : [];
  const tags = [...rawTags, "Lifestyle", "Short-form Video", "Audience Engagement", "Creator Updates", "Entertainment", "Digital Culture"]
    .map((tag) => text(tag))
    .filter(Boolean)
    .filter((tag, index, arr) => arr.findIndex((other) => other.toLowerCase() === tag.toLowerCase()) === index)
    .slice(0, 10);
  while (tags.length < 10) tags.push(`General Topic ${tags.length + 1}`);
  const distribution = Array.isArray(raw?.top_5_distribution) ? raw.top_5_distribution : [];
  const distributionPercent = (tag, index, fallback) => {
    const exact = distribution.find((item) => text(item?.tag)?.toLowerCase() === tag.toLowerCase());
    return exact?.percentage ?? distribution[index]?.percentage ?? fallback;
  };
  const top5 = tags.slice(0, 5).map((tag, index) => ({
    tag,
    percentage: percent(distributionPercent(tag, index, [25, 22, 18, 14, 11][index]), 0),
  }));
  const other = {
    tag: "Other",
    percentage: percent(distributionPercent("Other", 5, 10), 0),
  };
  return {
    tags,
    top_5_distribution: normalizePercentTotal([...top5, other], "percentage", "tag"),
  };
}

export function validChannelCategories(value) {
  if (!value || Array.isArray(value) || typeof value !== "object") return false;
  const level1 = text(value.level_1);
  if (!level1 || !ALLOWED_CATEGORY_LEVEL_1.has(level1)) return false;
  const level2 = Array.isArray(value.level_2) ? value.level_2.map((item) => text(item)) : [];
  if (level2.length < 1 || level2.length > 3 || level2.some((item) => !item)) return false;
  if (new Set(level2).size !== level2.length) return false;
  return level2.every((item) => CHANNEL_CATEGORY_TREE[level1].includes(item));
}

function normalizeChannelCategories(agent) {
  const raw = agent?.channel_categories;
  if (validChannelCategories(raw)) {
    return {
      level_1: text(raw.level_1),
      level_2: raw.level_2.map((item) => text(item)),
    };
  }

  // Accept legacy flat arrays during rolling deploys, but store the new tree shape.
  const legacy = Array.isArray(raw) ? raw.map((item) => text(item)).filter(Boolean) : [];
  const firstLevel2 = legacy.find((item) => ALLOWED_CATEGORIES.has(item));
  const level1 = CATEGORY_PARENT.get(firstLevel2);
  if (level1) {
    const level2 = [...new Set(legacy.filter((item) => CATEGORY_PARENT.get(item) === level1))].slice(0, 3);
    if (level2.length > 0) return { level_1: level1, level_2: level2 };
  }
  return {
    level_1: DEFAULT_CHANNEL_CATEGORY.level_1,
    level_2: [...DEFAULT_CHANNEL_CATEGORY.level_2],
  };
}

function agentValue(value, inputUrl, confidence = "medium") {
  return {
    value,
    reason: null,
    source: "agent",
    evidence: ["Agent profile analysis from public YouTube/channel context"],
    confidence,
    source_urls: [inputUrl],
  };
}

function crawlerValue(value, inputUrl) {
  return {
    value,
    reason: null,
    source: "crawler",
    evidence: ["YouTube About country field"],
    confidence: "high",
    source_urls: [inputUrl],
  };
}

export function normalizeAgentMetrics(agent, inputUrl, { countryRequired = true, crawlerCountry = null } = {}) {
  const regionItems = Array.isArray(agent?.audience_region) ? agent.audience_region.slice(0, 6) : [];
  while (regionItems.length < 6) regionItems.push({ region: regionItems.length === 5 ? "Other" : `Other ${regionItems.length + 1}`, percentage: 0 });
  regionItems[5].region = "Other";
  const normalized = {
    country: countryRequired ? text(agent?.country) : text(crawlerCountry),
    creator_gender: ["male", "female", "brand_team"].includes(String(agent?.creator_gender ?? "").toLowerCase())
      ? String(agent.creator_gender).toLowerCase()
      : "brand_team",
    creator_age_range: int(agent?.creator_age_range) ?? 0,
    creator_language: text(agent?.creator_language) ?? "Portuguese",
    audience_region: normalizePercentTotal(regionItems, "percentage", "region").slice(0, 6),
    audience_age_gender: normalizeAudienceAgeGender(agent),
    audience_language: normalizePercentTotal(
      Array.isArray(agent?.audience_language) && agent.audience_language.length ? agent.audience_language : [{ language: "Portuguese", percentage: 100 }],
      "percentage",
      "language",
    ),
    active_subscriber_ratio: percent(agent?.active_subscriber_ratio, 30),
    channel_tags: normalizeChannelTags(agent),
    channel_categories: normalizeChannelCategories(agent),
  };
  return {
    audience_profile_agent: {
      ...(normalized.country
        ? { country: countryRequired ? agentValue(normalized.country, inputUrl) : crawlerValue(normalized.country, inputUrl) }
        : {}),
      creator_gender: agentValue(normalized.creator_gender, inputUrl),
      creator_age_range: agentValue(normalized.creator_age_range, inputUrl),
      creator_language: agentValue(normalized.creator_language, inputUrl),
      audience_region: agentValue(normalized.audience_region, inputUrl),
      audience_age_gender: agentValue(normalized.audience_age_gender, inputUrl),
      audience_language: agentValue(normalized.audience_language, inputUrl),
      active_subscriber_ratio: agentValue(normalized.active_subscriber_ratio, inputUrl),
      channel_tags: agentValue(normalized.channel_tags, inputUrl),
      channel_categories: agentValue(normalized.channel_categories, inputUrl),
    },
  };
}

export function buildPrompt(template, items) {
  const inputUrls = items.map((item) => item.input_url);
  const countryResolvedItems = items.filter((item) => !item.country_required && text(item.country));
  const countryResolvedSet = new Set(countryResolvedItems.map((item) => item.input_url));
  const countryRequiredUrls = items.filter((item) => !countryResolvedSet.has(item.input_url)).map((item) => item.input_url);
  const countryResolvedUrls = countryResolvedItems.map((item) => item.input_url);
  const countryResolvedValues = Object.fromEntries(
    countryResolvedItems.map((item) => [item.input_url, text(item.country)]),
  );
  const urlsJson = JSON.stringify(inputUrls, null, 2);
  const requiredJson = JSON.stringify(countryRequiredUrls, null, 2);
  const resolvedJson = JSON.stringify(countryResolvedUrls, null, 2);
  const resolvedValuesJson = JSON.stringify(countryResolvedValues, null, 2);
  const contentIdentityContext = Object.fromEntries(
    items
      .map((item) => [item.input_url, inputContentIds(item)])
      .filter(([, contentIds]) => contentIds.length > 0),
  );
  const contentIdentityJson = JSON.stringify(contentIdentityContext, null, 2);
  const firstPartyIdentityJson = JSON.stringify(buildFirstPartyIdentityContext(items), null, 2);
  const base = text(template) ?? "Analyze these YouTube channels and return strict JSON for input_urls: {{input_urls_json}}";
  let prompt = base;
  if (prompt.includes("{{input_urls_json}}")) prompt = prompt.replaceAll("{{input_urls_json}}", urlsJson);
  else if (prompt.includes("{{input_urls}}")) prompt = prompt.replaceAll("{{input_urls}}", urlsJson);
  else prompt = `${prompt.trim()}\n\ninput_urls:\n${urlsJson}`;
  prompt = prompt.replaceAll("{{country_required_urls_json}}", requiredJson);
  prompt = prompt.replaceAll("{{country_resolved_urls_json}}", resolvedJson);
  prompt = prompt.replaceAll("{{country_resolved_values_json}}", resolvedValuesJson);
  return `${prompt.trim()}

Country requirement override (this section takes precedence over conflicting rules or examples above):
- country_required_input_urls: ${requiredJson}
- country_resolved_input_urls: ${resolvedJson}
- country_resolved_values: ${resolvedValuesJson}
- Analyze and return every non-country profile field for every input URL.
- Only research country for country_required_input_urls; country must be a concrete English country or region name for those URLs.
- Do not research country for country_resolved_input_urls; copy the concrete value supplied in country_resolved_values.
- crawler_content_identity_context: ${contentIdentityJson}
- When a URL has Content IDs in crawler_content_identity_context, those IDs are part of this request's input context.
- crawler_first_party_identity_context: ${firstPartyIdentityJson}
- crawler_first_party_identity_context contains already collected first-party YouTube text: title, About, recent video titles and descriptions, and channel-owner comments only.
- Use that block as the primary evidence for creator_gender and creator_age_range.
- Do not infer creator_gender or creator_age_range from avatars, faces, voices, names alone, or content-niche stereotypes.
- Missing text in this block is not proof that no public evidence exists; you may still use other permitted YouTube sources.
- Keep one output object per input URL, in exactly the input order.`;
}

export function endpointFor(config) {
  const endpoint = text(config?.endpoint ?? config?.base_url ?? config?.baseUrl);
  if (!endpoint) return null;
  const provider = String(config?.provider ?? "").toLowerCase();
  if (/:(?:stream)?generateContent\/?$/i.test(endpoint)) return endpoint;
  if (provider.includes("gemini")) {
    const model = text(config?.model) ?? "gemini-2.5-flash";
    if (/\/models\/[^/]+\/?$/i.test(endpoint)) return `${endpoint.replace(/\/$/, "")}:generateContent`;
    return `${endpoint.replace(/\/$/, "")}/models/${encodeURIComponent(model)}:generateContent`;
  }
  if (/\/chat\/completions\/?$/i.test(endpoint) || /\/responses\/?$/i.test(endpoint)) return endpoint;
  return `${endpoint.replace(/\/$/, "")}/chat/completions`;
}

export function normalizeAgentTools(value) {
  if (Array.isArray(value)) return value;
  if (value && typeof value === "object") return [value];
  return [];
}

export function geminiApiKeyHeaders(apiKey) {
  const key = text(apiKey);
  return key ? { "x-goog-api-key": key } : {};
}

export function geminiGenerateContentBody(prompt, toolsJson) {
  const tools = normalizeAgentTools(toolsJson);
  return {
    contents: [{ role: "user", parts: [{ text: prompt }] }],
    ...(tools.length > 0 ? { tools } : {}),
    generationConfig: { temperature: 0.4 },
  };
}

function responseText(data) {
  if (typeof data === "string") return data;
  if (typeof data?.output_text === "string") return data.output_text;
  if (typeof data?.text === "string") return data.text;
  const choice = data?.choices?.[0];
  if (typeof choice?.message?.content === "string") return choice.message.content;
  if (Array.isArray(choice?.message?.content)) return choice.message.content.map((part) => part?.text ?? part?.content ?? "").join("");
  const parts = data?.candidates?.[0]?.content?.parts;
  if (Array.isArray(parts)) return parts.map((part) => part?.text ?? "").join("");
  if (Array.isArray(data?.output)) {
    return data.output.flatMap((item) => item?.content ?? []).map((part) => part?.text ?? "").join("");
  }
  return JSON.stringify(data);
}

function firstCompleteJsonValue(source, startIndex) {
  const opening = source[startIndex];
  const closing = opening === "[" ? "]" : opening === "{" ? "}" : null;
  if (!closing) return null;
  const stack = [];
  let inString = false;
  let escaped = false;
  for (let index = startIndex; index < source.length; index += 1) {
    const char = source[index];
    if (inString) {
      if (escaped) escaped = false;
      else if (char === "\\") escaped = true;
      else if (char === '"') inString = false;
      continue;
    }
    if (char === '"') {
      inString = true;
      continue;
    }
    if (char === "[" || char === "{") stack.push(char);
    else if (char === "]" || char === "}") {
      const expected = char === "]" ? "[" : "{";
      if (stack.pop() !== expected) return null;
      if (stack.length === 0) return source.slice(startIndex, index + 1);
    }
  }
  return null;
}

export function parseJsonPayload(raw) {
  const cleaned = String(raw ?? "").trim().replace(/^```(?:json)?\s*/i, "").replace(/\s*```$/i, "").trim();
  try {
    return JSON.parse(cleaned);
  } catch {
    const arrayStart = cleaned.indexOf("[");
    const arrayValue = arrayStart >= 0 ? firstCompleteJsonValue(cleaned, arrayStart) : null;
    if (arrayValue) return JSON.parse(arrayValue);
    const objectStart = cleaned.indexOf("{");
    const objectValue = objectStart >= 0 ? firstCompleteJsonValue(cleaned, objectStart) : null;
    if (objectValue) return JSON.parse(objectValue);
    throw new Error("agent response did not contain parseable JSON");
  }
}

function sseEvents(raw) {
  const events = [];
  let event = "message";
  let data = [];
  const flush = () => {
    if (data.length > 0) events.push({ event, data: data.join("\n") });
    event = "message";
    data = [];
  };
  for (const line of String(raw ?? "").split(/\r?\n/)) {
    if (line === "") {
      flush();
      continue;
    }
    if (line.startsWith(":")) continue;
    if (line.startsWith("event:")) {
      event = line.slice(6).trim() || "message";
      continue;
    }
    if (line.startsWith("data:")) data.push(line.slice(5).trimStart());
  }
  flush();
  return events;
}

function sseError(payload) {
  const source = payload?.error && typeof payload.error === "object" ? payload.error : payload;
  const message = text(source?.message) ?? text(source?.error) ?? "agent SSE error";
  const explicitStatus = Number(source?.status ?? source?.status_code ?? source?.http_status);
  const status = Number.isFinite(explicitStatus) && explicitStatus >= 400
    ? explicitStatus
    : Number(message.match(/\b([45]\d{2})\b/)?.[1]) || null;
  const error = new Error(status ? `agent HTTP ${status}: ${message}` : `agent SSE error: ${message}`);
  if (status) error.status = status;
  return error;
}

export function parseAgentResponseBody(raw, contentType = "") {
  const body = String(raw ?? "");
  try {
    return JSON.parse(body);
  } catch {
    // Some OpenAI-compatible gateways stream events even when non-streaming was requested.
  }
  const looksLikeSse = /text\/event-stream/i.test(String(contentType))
    || /^(?:event|data|retry):/m.test(body)
    || /^:/m.test(body);
  if (!looksLikeSse) return body;

  let finalEnvelope = null;
  const contentParts = [];
  for (const item of sseEvents(body)) {
    if (!item.data || item.data.trim() === "[DONE]") continue;
    let payload;
    try {
      payload = JSON.parse(item.data);
    } catch {
      if (item.event === "message") contentParts.push(item.data);
      continue;
    }
    if (item.event === "error" || payload?.error) throw sseError(payload);

    const choice = payload?.choices?.[0];
    if (typeof choice?.delta?.content === "string") contentParts.push(choice.delta.content);
    else if (Array.isArray(choice?.delta?.content)) {
      contentParts.push(choice.delta.content.map((part) => part?.text ?? part?.content ?? "").join(""));
    } else if (typeof choice?.message?.content === "string") {
      finalEnvelope = payload;
    } else if (typeof payload?.output_text === "string") {
      contentParts.push(payload.output_text);
    }
    if (payload?.choices || payload?.output || payload?.candidates) finalEnvelope = payload;
  }
  if (contentParts.length > 0) {
    return {
      choices: [{ message: { content: contentParts.join("") }, finish_reason: "stop" }],
    };
  }
  if (finalEnvelope) return finalEnvelope;
  throw new Error("agent SSE response contained no usable data");
}

export function chatCompletionBody(model, prompt) {
  return {
    model,
    messages: [{ role: "user", content: prompt }],
    temperature: 0.2,
    stream: false,
  };
}

function extractRows(payload) {
  if (Array.isArray(payload)) return payload;
  if (Array.isArray(payload?.results)) return payload.results;
  if (Array.isArray(payload?.data)) return payload.data;
  throw new Error("agent JSON response must be an array");
}

async function postJsonWithRetry(url, body, headers, timeoutMs, maxRetries) {
  let lastError = null;
  const attempts = Math.max(1, Math.min(Number(maxRetries ?? 2) + 1, 5));
  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    try {
      const response = await persistentFetch(url, {
        method: "POST",
        headers: { "content-type": "application/json", ...headers },
        body: JSON.stringify(body),
        signal: AbortSignal.timeout(timeoutMs),
      });
      const textBody = await response.text();
      if (!response.ok) {
        const requestError = new Error(`agent HTTP ${response.status}: ${textBody.slice(0, 500)}`);
        requestError.status = response.status;
        throw requestError;
      }
      return parseAgentResponseBody(textBody, response.headers.get("content-type") ?? "");
    } catch (error) {
      lastError = error;
      if (attempt >= attempts) break;
      if (!classifyAgentFailure(error).retryable) break;
      await new Promise((resolve) => setTimeout(resolve, agentRetryDelayMs(attempt)));
    }
  }
  throw lastError;
}

async function callAgentOnce(config, apiKey, items) {
  const provider = String(config?.provider ?? "").toLowerCase();
  const model = text(config?.model) ?? "gpt-4o-mini";
  const endpoint = endpointFor(config);
  if (!endpoint) throw new Error("agent endpoint is empty");
  const prompt = buildPrompt(config?.template_text, items);
  if (provider.includes("gemini") || endpoint.includes("generativelanguage.googleapis.com")) {
    const data = await postJsonWithRetry(
      endpoint,
      geminiGenerateContentBody(prompt, config?.tools_json),
      geminiApiKeyHeaders(apiKey),
      Number(config?.timeout_ms ?? 120000),
      Number(config?.max_retries ?? 2),
    );
    return responseText(data);
  }
  const headers = apiKey ? { authorization: /^bearer\s+/i.test(apiKey) ? apiKey : `Bearer ${apiKey}` } : {};
  const useResponses = /\/responses\/?$/i.test(endpoint);
  const body = useResponses
    ? { model, input: prompt, ...(Array.isArray(config?.tools_json) && config.tools_json.length > 0 ? { tools: config.tools_json } : {}) }
    : chatCompletionBody(model, prompt);
  const data = await postJsonWithRetry(endpoint, body, headers, Number(config?.timeout_ms ?? 120000), Number(config?.max_retries ?? 2));
  return responseText(data);
}

function mapAgentRows(payload, inputUrls) {
  const rows = extractRows(payload);
  const expected = new Set(inputUrls);
  const byUrl = new Map();
  rows.forEach((item, index) => {
    const inputUrl = text(item?.input_url) ?? (rows.length === inputUrls.length ? inputUrls[index] : null);
    if (!inputUrl || !expected.has(inputUrl) || byUrl.has(inputUrl)) return;
    byUrl.set(inputUrl, { ...item, input_url: inputUrl });
  });
  return byUrl;
}

function agentRowMissingFields(row, item) {
  const missing = [];
  const requiredText = item?.country_required
    ? ["country", "creator_language"]
    : ["creator_language"];
  for (const field of requiredText) {
    if (!text(row?.[field])) missing.push(field);
  }
  if (!["male", "female", "brand_team"].includes(String(row?.creator_gender ?? "").toLowerCase())) missing.push("creator_gender");
  if (int(row?.creator_age_range) == null || int(row?.creator_age_range) < 0) missing.push("creator_age_range");
  if (!Array.isArray(row?.audience_region) || row.audience_region.length !== 6) missing.push("audience_region");
  if (!Array.isArray(row?.audience_age_gender) || row.audience_age_gender.length !== 6) missing.push("audience_age_gender");
  if (!Array.isArray(row?.audience_language) || row.audience_language.length === 0) missing.push("audience_language");
  if (int(row?.active_subscriber_ratio) == null) missing.push("active_subscriber_ratio");
  if (!Array.isArray(row?.channel_tags?.tags) || row.channel_tags.tags.length !== 10) missing.push("channel_tags.tags");
  if (!Array.isArray(row?.channel_tags?.top_5_distribution) || row.channel_tags.top_5_distribution.length !== 6) {
    missing.push("channel_tags.top_5_distribution");
  }
  if (!validChannelCategories(row?.channel_categories)) missing.push("channel_categories");
  return missing;
}

async function applyBatch({
  config,
  apiKeys,
  items,
  out,
  errors,
  callAgent = callAgentOnce,
  wait = (delayMs) => new Promise((resolve) => setTimeout(resolve, delayMs)),
}) {
  if (items.length === 0) return;
  const inputUrls = items.map((item) => item.input_url);
  let lastError = null;
  const validationRetries = Math.max(0, Math.min(Number(config?.max_retries ?? 2), 4));
  for (const apiKey of apiKeys.length > 0 ? apiKeys : [null]) {
    for (let validationAttempt = 0; validationAttempt <= validationRetries; validationAttempt += 1) {
      try {
        const textBody = await callAgent(config, apiKey, items);
        const byUrl = mapAgentRows(parseJsonPayload(textBody), inputUrls);
        if (byUrl.size === 0) throw new Error(`agent empty response: returned 0/${inputUrls.length}`);
        const resolved = items.filter((item) => {
          const row = byUrl.get(item.input_url);
          return row && agentRowMissingFields(row, item).length === 0;
        });
        if (resolved.length === 0) {
          const invalid = items
            .map((item) => ({ input_url: item.input_url, missing: agentRowMissingFields(byUrl.get(item.input_url), item) }))
            .filter((item) => item.missing.length > 0);
          throw new Error(`agent response incomplete: ${JSON.stringify(invalid).slice(0, 1000)}`);
        }
        for (const item of resolved) {
          out.set(item.channel_id, {
            metrics: normalizeAgentMetrics(byUrl.get(item.input_url), item.input_url, {
              countryRequired: item.country_required,
              crawlerCountry: item.country,
            }),
            agent_model: text(config?.model) ?? text(config?.provider) ?? "agent",
            agent_applied: true,
            country_required: item.country_required,
            input_content_ids: inputContentIds(item),
          });
        }
        const missing = items.filter((item) => !resolved.includes(item));
        if (missing.length > 0) {
          await applyBatch({ config, apiKeys, items: missing, out, errors, callAgent, wait });
        }
        return;
      } catch (error) {
        lastError = error;
        const failure = classifyAgentFailure(error);
        if (failure.kind === "invalid_response" && validationAttempt < validationRetries) {
          await wait(agentRetryDelayMs(validationAttempt + 1));
          continue;
        }
        break;
      }
    }
    const failure = classifyAgentFailure(lastError);
    if (!apiKey || !["authentication", "rate_limit"].includes(failure.kind)) break;
  }
  const failure = classifyAgentFailure(lastError);
  if (items.length > 1 && failure.splittable) {
    const mid = Math.max(1, Math.floor(items.length / 2));
    await applyBatch({ config, apiKeys, items: items.slice(0, mid), out, errors, callAgent, wait });
    await applyBatch({ config, apiKeys, items: items.slice(mid), out, errors, callAgent, wait });
    return;
  }
  const message = String(lastError?.message ?? lastError);
  for (const item of items) {
    errors.push({
      channel_id: item.channel_id,
      input_url: item.input_url,
      error: message,
      error_kind: failure.kind,
      retryable: failure.retryable,
    });
  }
}

export async function runAgentBatch({
  config,
  apiKeys = [],
  channels = [],
  includeCountry = true,
  callAgent = callAgentOnce,
  wait,
}) {
  const provider = String(config?.provider ?? "rules").toLowerCase();
  if (!channels.length || provider === "rules") return { results: new Map(), errors: [], skipped: provider === "rules" };
  const items = channels.map((item) => ({
    ...item,
    country: normalizeCrawlerCountry(item.country),
    country_required: item.country_source == null
      ? ((item.country_required ?? includeCountry) || !normalizeCrawlerCountry(item.country))
      : countryRequiresAgent(item),
  }));
  const out = new Map();
  const errors = [];
  await applyBatch({ config, apiKeys, items, out, errors, callAgent, wait });
  return { results: out, errors, skipped: false };
}
