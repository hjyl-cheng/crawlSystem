import { agentPromptHash } from "./agentConfig.js";
import {
  CHANNEL_CATEGORY_TREE,
} from "./agentTaxonomy.js";
import { observationFactsHash } from "./crawlObservationStore.js";
import {
  AGENT_TAXONOMY_VERSION,
  PUBLICATION_CONTRACT_VERSION,
  PUBLICATION_POLICY_VERSION,
} from "./publicationContract.js";
import { buildPublicationSourceTrace } from "./publicationSourceTrace.js";
import { normalizePublicationUrl } from "./publicationUrl.js";

export const AGENT_FACT_KEYS = Object.freeze([
  "country",
  "creator_language",
  "creator_gender",
  "creator_age_range",
  "audience_region",
  "audience_language",
  "audience_age_gender",
  "active_subscriber_ratio",
  "channel_categories",
  "channel_tags",
]);

const AGE_GENDER_LABELS = Object.freeze([
  "18-24",
  "25-34",
  "35-44",
  "45-54",
  "55-64",
  "65+",
]);
const CONFIDENCE_LEVELS = new Set(["low", "medium", "high"]);
const CREATOR_GENDERS = new Set(["male", "female", "brand_team"]);
const PROMPT_VARIANTS = new Set(["country_required", "country_resolved", "with_country"]);
const LOCAL_EXECUTION_VARIANT = "local_offline";
const HASH_PATTERN = /^sha256:[0-9a-f]{64}$/;
const RAW_HASH_PATTERN = /^[0-9a-f]{64}$/;

function hasOwn(value, key) {
  return value != null && Object.prototype.hasOwnProperty.call(value, key);
}

function object(value) {
  return value && typeof value === "object" && !Array.isArray(value) ? value : {};
}

function text(value) {
  const output = String(value ?? "").trim();
  return output || null;
}

function integer(value) {
  if (value === null || value === undefined || value === "") return null;
  const parsed = Number(value);
  return Number.isSafeInteger(parsed) ? parsed : null;
}

function positiveInteger(value) {
  const parsed = integer(value);
  return parsed !== null && parsed > 0 ? parsed : null;
}

function compareText(left, right) {
  return Buffer.compare(Buffer.from(String(left ?? ""), "utf8"), Buffer.from(String(right ?? ""), "utf8"));
}

function uniqueTextList(value, { sort = true } = {}) {
  const values = Array.isArray(value) ? value : [];
  const output = [...new Set(values.map(text).filter(Boolean))];
  return sort ? output.sort(compareText) : output;
}

function compactIssue(issue) {
  return Object.fromEntries(Object.entries(issue).filter(([, value]) => value !== undefined));
}

function issueKey(issue) {
  return [issue.domain, issue.code, issue.field].map((value) => value ?? "").join("\u0000");
}

function sortedIssues(issues) {
  const unique = new Map();
  for (const issue of issues) unique.set(issueKey(issue), compactIssue(issue));
  return [...unique.values()].sort((left, right) => (
    compareText(left.domain, right.domain)
    || compareText(left.code, right.code)
    || compareText(left.field, right.field)
  ));
}

function requiredRunInteger(value, field) {
  const parsed = positiveInteger(value);
  if (parsed === null) throw new TypeError(`${field} must be a positive integer`);
  return parsed;
}

function requiredRunText(value, field) {
  const normalized = text(value);
  if (!normalized) throw new TypeError(`${field} is required`);
  return normalized;
}

function promptHashFor(config) {
  const supplied = text(config?.prompt_hash);
  if (RAW_HASH_PATTERN.test(supplied ?? "")) return supplied;
  const template = text(config?.template_text);
  return template ? agentPromptHash(template) : null;
}

export function buildAgentPublicationRun({
  agentConfig,
  agentModel,
  promptVariant,
  executionVariant = null,
  runtimeIdentity = null,
  inputContentIds = [],
  taxonomyVersion = AGENT_TAXONOMY_VERSION,
}) {
  const config = object(agentConfig);
  if (!Array.isArray(inputContentIds)) throw new TypeError("inputContentIds must be an array");
  const inputIds = uniqueTextList(inputContentIds);
  const model = requiredRunText(agentModel ?? config.model, "agentModel");
  const provider = requiredRunText(config.provider, "agentConfig.provider");
  const configId = requiredRunInteger(config.config_id, "agentConfig.config_id");
  const localExecution = executionVariant === LOCAL_EXECUTION_VARIANT
    || provider.toLocaleLowerCase("en") === "local-offline";
  if (localExecution) {
    const identity = object(runtimeIdentity);
    const runtimeModelId = requiredRunText(identity.runtime_model_id, "runtimeIdentity.runtime_model_id");
    if (runtimeModelId !== model) {
      throw new TypeError("local agentModel must match runtimeIdentity.runtime_model_id");
    }
    const processorVersion = requiredRunText(
      identity.processor_version,
      "runtimeIdentity.processor_version",
    );
    const modelBundleVersion = requiredRunText(
      identity.model_bundle_version,
      "runtimeIdentity.model_bundle_version",
    );
    const modelBundleHash = requiredRunText(
      identity.model_bundle_hash,
      "runtimeIdentity.model_bundle_hash",
    );
    const priorCatalogVersion = requiredRunText(
      identity.prior_catalog_version,
      "runtimeIdentity.prior_catalog_version",
    );
    const priorCatalogHash = requiredRunText(
      identity.prior_catalog_hash,
      "runtimeIdentity.prior_catalog_hash",
    );
    const taxonomy = requiredRunText(taxonomyVersion, "taxonomyVersion");
    if (requiredRunText(identity.taxonomy_version, "runtimeIdentity.taxonomy_version") !== taxonomy) {
      throw new TypeError("runtimeIdentity taxonomy does not match taxonomyVersion");
    }
    if (!HASH_PATTERN.test(modelBundleHash) || !HASH_PATTERN.test(priorCatalogHash)) {
      throw new TypeError("local runtime artifact hashes must use sha256:<hex>");
    }
    const inputContentHash = observationFactsHash(inputIds);
    const versionHash = observationFactsHash({
      executor_mode: LOCAL_EXECUTION_VARIANT,
      provider,
      model: runtimeModelId,
      agent_config_id: configId,
      processor_version: processorVersion,
      model_bundle_version: modelBundleVersion,
      model_bundle_hash: modelBundleHash,
      prior_catalog_version: priorCatalogVersion,
      prior_catalog_hash: priorCatalogHash,
      taxonomy_version: taxonomy,
    });
    return {
      agent_model: runtimeModelId,
      agent_config_id: configId,
      prompt_template_id: null,
      prompt_hash: null,
      prompt_variant: LOCAL_EXECUTION_VARIANT,
      execution_variant: LOCAL_EXECUTION_VARIANT,
      taxonomy_version: taxonomy,
      agent_version_hash: versionHash,
      input_content_ids: inputIds,
      input_content_hash: inputContentHash,
    };
  }
  const promptTemplateId = requiredRunInteger(
    config.prompt_template_id,
    "agentConfig.prompt_template_id",
  );
  const promptHash = promptHashFor(config);
  if (!promptHash) throw new TypeError("agentConfig prompt hash is required");
  const variant = requiredRunText(promptVariant, "promptVariant");
  if (!PROMPT_VARIANTS.has(variant)) throw new TypeError(`unsupported promptVariant: ${variant}`);
  const taxonomy = requiredRunText(taxonomyVersion, "taxonomyVersion");
  const tools = Array.isArray(config.tools_json) ? config.tools_json : [];
  const inputContentHash = observationFactsHash(inputIds);
  const versionHash = observationFactsHash({
    executor_mode: "basic",
    provider,
    model,
    agent_config_id: configId,
    prompt_template_id: promptTemplateId,
    prompt_hash: promptHash,
    prompt_variant: variant,
    taxonomy_version: taxonomy,
    tools,
  });
  return {
    agent_model: model,
    agent_config_id: configId,
    prompt_template_id: promptTemplateId,
    prompt_hash: promptHash,
    prompt_variant: variant,
    taxonomy_version: taxonomy,
    agent_version_hash: versionHash,
    input_content_ids: inputIds,
    input_content_hash: inputContentHash,
  };
}

function percentage(value) {
  const parsed = integer(value);
  return parsed !== null && parsed >= 0 && parsed <= 100 ? parsed : null;
}

function percentageRows(value, labelKey, { count = null, finalLabel = null } = {}) {
  if (!Array.isArray(value) || (count !== null && value.length !== count) || value.length === 0) {
    return false;
  }
  const labels = [];
  let total = 0;
  for (const rowValue of value) {
    const row = object(rowValue);
    const label = text(row[labelKey]);
    const share = percentage(row.percentage);
    if (!label || share === null) return false;
    labels.push(label);
    total += share;
  }
  return new Set(labels.map((label) => label.toLocaleLowerCase("en"))).size === labels.length
    && total === 100
    && (finalLabel === null || labels[labels.length - 1] === finalLabel);
}

function validAgeGender(value) {
  if (!Array.isArray(value) || value.length !== AGE_GENDER_LABELS.length) return false;
  let total = 0;
  for (let index = 0; index < AGE_GENDER_LABELS.length; index += 1) {
    const row = object(value[index]);
    const male = percentage(row.male);
    const female = percentage(row.female);
    if (text(row.age_range) !== AGE_GENDER_LABELS[index] || male === null || female === null) {
      return false;
    }
    total += male + female;
  }
  return total === 100;
}

function validCategories(value) {
  const categories = object(value);
  const level1 = text(categories.level_1);
  const level2 = uniqueTextList(categories.level_2, { sort: false });
  return Boolean(
    level1
    && Array.isArray(categories.level_2)
    && level2.length === categories.level_2.length
    && level2.length >= 1
    && level2.length <= 3
    && Array.isArray(CHANNEL_CATEGORY_TREE[level1])
    && level2.every((category) => CHANNEL_CATEGORY_TREE[level1].includes(category)),
  );
}

function validTags(value) {
  const tagsValue = object(value);
  const tags = uniqueTextList(tagsValue.tags, { sort: false });
  const distribution = Array.isArray(tagsValue.top_5_distribution)
    ? tagsValue.top_5_distribution
    : [];
  if (!Array.isArray(tagsValue.tags)
      || tags.length !== 10
      || tags.length !== tagsValue.tags.length
      || distribution.length !== 6) {
    return false;
  }
  let total = 0;
  for (let index = 0; index < distribution.length; index += 1) {
    const row = object(distribution[index]);
    const expectedTag = index === 5 ? "Other" : tags[index];
    const share = percentage(row.percentage);
    if (text(row.tag) !== expectedTag || share === null) return false;
    total += share;
  }
  return total === 100;
}

function validFactValue(field, value) {
  if (["country", "creator_language"].includes(field)) return Boolean(text(value));
  if (field === "creator_gender") return CREATOR_GENDERS.has(text(value));
  if (field === "creator_age_range") {
    const age = integer(value);
    return age !== null && age >= 0;
  }
  if (field === "audience_region") {
    return percentageRows(value, "region", { count: 6, finalLabel: "Other" });
  }
  if (field === "audience_language") return percentageRows(value, "language");
  if (field === "audience_age_gender") return validAgeGender(value);
  if (field === "active_subscriber_ratio") return percentage(value) !== null;
  if (field === "channel_categories") return validCategories(value);
  if (field === "channel_tags") return validTags(value);
  return false;
}

function evidenceValueValid(value) {
  if (typeof value === "string") return Boolean(text(value));
  if (value && typeof value === "object") return Object.keys(value).length > 0;
  return false;
}

function factCurrent(field, rawFact) {
  const fact = object(rawFact);
  const requiredKeys = ["value", "confidence", "evidence", "source_urls", "reason", "source"];
  const missingKeys = requiredKeys.filter((key) => !hasOwn(fact, key));
  const invalidFields = [];
  if (hasOwn(fact, "value") && !validFactValue(field, fact.value)) invalidFields.push("value");
  if (hasOwn(fact, "confidence") && !CONFIDENCE_LEVELS.has(text(fact.confidence))) {
    invalidFields.push("confidence");
  }
  if (hasOwn(fact, "evidence") && (
    !Array.isArray(fact.evidence)
    || fact.evidence.length === 0
    || fact.evidence.some((value) => !evidenceValueValid(value))
  )) {
    invalidFields.push("evidence");
  }
  const sourceUrls = uniqueTextList(fact.source_urls);
  const normalizedUrlValues = sourceUrls.map((url) => normalizePublicationUrl(url));
  const normalizedUrls = uniqueTextList(normalizedUrlValues.filter(Boolean));
  if (hasOwn(fact, "source_urls") && (
    !Array.isArray(fact.source_urls)
    || normalizedUrls.length === 0
    || normalizedUrlValues.some((url) => !url)
  )) {
    invalidFields.push("source_urls");
  }
  if (hasOwn(fact, "source") && !text(fact.source)) invalidFields.push("source");
  if (hasOwn(fact, "reason") && fact.reason !== null && !text(fact.reason)) {
    invalidFields.push("reason");
  }
  const complete = missingKeys.length === 0 && invalidFields.length === 0;
  const payload = {
    value: hasOwn(fact, "value") ? fact.value : null,
    confidence: text(fact.confidence),
    evidence: Array.isArray(fact.evidence) ? fact.evidence : [],
    source_urls: normalizedUrls,
    reason: fact.reason === null ? null : text(fact.reason),
    source: text(fact.source),
  };
  return {
    field,
    complete,
    missing_keys: missingKeys,
    invalid_fields: invalidFields,
    fact_hash: complete ? observationFactsHash(payload) : null,
    payload,
  };
}

function compareMetadata(issues, field, actual, expected) {
  if (actual === null || actual === undefined) {
    issues.push({ domain: "agent", code: "agent_metadata_incomplete", field });
  } else if (expected !== null && expected !== undefined && actual !== expected) {
    issues.push({ domain: "agent", code: "agent_configuration_mismatch", field });
  }
}

function legacyInputContext(metrics) {
  const context = object(metrics.publication_context);
  const ids = context.input_content_ids ?? metrics.agent_input_content_ids ?? metrics.input_content_ids;
  const hash = context.input_content_hash ?? metrics.input_content_hash;
  return {
    present: ids !== undefined || hash !== undefined,
    ids: Array.isArray(ids) ? uniqueTextList(ids) : null,
    hash: text(hash),
  };
}

export function buildAgentPublicationCurrent({ row: rowValue, config: configValue, source }) {
  const profile = object(rowValue);
  const config = object(configValue);
  const metrics = object(profile.metrics_json);
  const factsObject = object(metrics.audience_profile_agent);
  const factResults = AGENT_FACT_KEYS.map((field) => factCurrent(field, factsObject[field]));
  const issues = [];
  for (const fact of factResults) {
    if (!fact.complete) issues.push({
      domain: "agent",
      code: "agent_fact_incomplete",
      field: fact.field,
    });
  }
  if (profile.agent_mode !== "basic") issues.push({ domain: "agent", code: "agent_mode_unsupported" });
  if (profile.status !== "success") issues.push({ domain: "agent", code: "agent_current_not_success" });

  const channelId = text(profile.channel_id);
  if (!channelId) issues.push({ domain: "agent", code: "agent_channel_id_missing" });
  const inputUrl = normalizePublicationUrl(profile.input_url);
  if (!inputUrl) issues.push({ domain: "agent", code: "agent_input_url_invalid" });

  const outputHash = text(profile.current_output_hash);
  const expectedOutputHash = observationFactsHash(metrics);
  if (!HASH_PATTERN.test(outputHash ?? "")) {
    issues.push({ domain: "agent", code: "agent_output_hash_missing" });
  } else if (outputHash !== expectedOutputHash) {
    issues.push({ domain: "agent", code: "agent_output_hash_mismatch" });
  }

  const inputIds = Array.isArray(profile.input_content_ids)
    ? uniqueTextList(profile.input_content_ids)
    : null;
  const inputHash = text(profile.input_content_hash);
  if (inputIds === null) {
    issues.push({ domain: "agent", code: "agent_input_content_ids_missing" });
  }
  if (!HASH_PATTERN.test(inputHash ?? "")) {
    issues.push({ domain: "agent", code: "agent_input_content_hash_missing" });
  } else if (inputIds !== null && inputHash !== observationFactsHash(inputIds)) {
    issues.push({ domain: "agent", code: "agent_input_content_hash_mismatch" });
  }
  if (Array.isArray(profile.input_content_ids)
      && inputIds.length !== profile.input_content_ids.map(text).filter(Boolean).length) {
    issues.push({ domain: "agent", code: "agent_input_content_ids_not_canonical" });
  }
  const legacyInput = legacyInputContext(metrics);
  if (legacyInput.present && (
    (legacyInput.ids !== null && observationFactsHash(legacyInput.ids) !== inputHash)
    || (legacyInput.hash && legacyInput.hash !== inputHash)
  )) {
    issues.push({ domain: "agent", code: "agent_input_content_legacy_mismatch" });
  }

  const profileConfigId = positiveInteger(profile.agent_config_id);
  const profilePromptTemplateId = positiveInteger(profile.prompt_template_id);
  const profilePromptHash = text(profile.prompt_hash);
  const profilePromptVariant = text(profile.prompt_variant);
  const profileModel = text(profile.agent_model);
  const profileTaxonomy = text(profile.taxonomy_version);
  const profileVersionHash = text(profile.agent_version_hash);
  const configId = positiveInteger(config.config_id);
  const configPromptTemplateId = positiveInteger(config.prompt_template_id);
  const configPromptHash = promptHashFor(config);
  const localExecution = text(config.provider)?.toLocaleLowerCase("en") === "local-offline"
    || profilePromptVariant === LOCAL_EXECUTION_VARIANT;
  const runtimeIdentity = object(metrics.profile_processing_context);
  compareMetadata(
    issues,
    "agent_model",
    profileModel,
    localExecution ? text(runtimeIdentity.runtime_model_id) : text(config.model),
  );
  compareMetadata(issues, "agent_config_id", profileConfigId, configId);
  if (localExecution) {
    if (profile.prompt_template_id !== null) {
      issues.push({ domain: "agent", code: "agent_metadata_invalid", field: "prompt_template_id" });
    }
    if (profile.prompt_hash !== null) {
      issues.push({ domain: "agent", code: "agent_metadata_invalid", field: "prompt_hash" });
    }
    if (profilePromptVariant !== LOCAL_EXECUTION_VARIANT) {
      issues.push({ domain: "agent", code: "agent_metadata_invalid", field: "prompt_variant" });
    }
  } else {
    compareMetadata(
      issues,
      "prompt_template_id",
      profilePromptTemplateId,
      configPromptTemplateId,
    );
    compareMetadata(issues, "prompt_hash", profilePromptHash, configPromptHash);
    compareMetadata(issues, "prompt_variant", profilePromptVariant, null);
  }
  compareMetadata(issues, "taxonomy_version", profileTaxonomy, AGENT_TAXONOMY_VERSION);
  compareMetadata(issues, "agent_version_hash", profileVersionHash, null);
  if (!localExecution && !PROMPT_VARIANTS.has(profilePromptVariant)) {
    issues.push({ domain: "agent", code: "agent_metadata_invalid", field: "prompt_variant" });
  }
  if (!localExecution && !RAW_HASH_PATTERN.test(profilePromptHash ?? "")) {
    issues.push({ domain: "agent", code: "agent_metadata_invalid", field: "prompt_hash" });
  }
  if (!HASH_PATTERN.test(profileVersionHash ?? "")) {
    issues.push({ domain: "agent", code: "agent_metadata_invalid", field: "agent_version_hash" });
  }

  let expectedRun = null;
  try {
    expectedRun = buildAgentPublicationRun({
      agentConfig: config,
      agentModel: profileModel,
      promptVariant: profilePromptVariant,
      executionVariant: localExecution ? LOCAL_EXECUTION_VARIANT : null,
      runtimeIdentity: localExecution ? runtimeIdentity : null,
      inputContentIds: inputIds ?? [],
      taxonomyVersion: profileTaxonomy,
    });
  } catch {
    issues.push({ domain: "agent", code: "agent_configuration_incomplete" });
  }
  if (expectedRun && profileVersionHash !== expectedRun.agent_version_hash) {
    issues.push({ domain: "agent", code: "agent_version_hash_mismatch" });
  }

  const summary = object(object(source?.complete_observation).result_summary_json);
  for (const [field, expected] of [
    ["output_hash", outputHash],
    ["input_content_hash", inputHash],
    ["agent_version_hash", profileVersionHash],
  ]) {
    const actual = text(summary[field]);
    if (!actual) issues.push({ domain: "agent", code: "agent_source_summary_missing", field });
    else if (expected && actual !== expected) {
      issues.push({ domain: "agent", code: "agent_source_summary_mismatch", field });
    }
  }

  const trace = buildPublicationSourceTrace(source, {
    observation_id: profile.last_observation_id,
    observed_at: profile.last_observed_at,
    facts_hash: object(source?.cursor).current_facts_hash,
    business_hash: outputHash,
  }, "agent");
  issues.push(...trace.issues);

  const facts = Object.fromEntries(factResults.map((fact) => [fact.field, fact.payload]));
  const payload = {
    channel_id: channelId,
    agent_mode: "basic",
    input_url: inputUrl,
    facts,
    agent_model: profileModel,
    agent_config_id: profileConfigId,
    prompt_template_id: profilePromptTemplateId,
    prompt_hash: profilePromptHash,
    prompt_variant: profilePromptVariant,
    agent_version_hash: profileVersionHash,
    output_hash: outputHash,
    input_content_ids: inputIds,
    input_content_hash: inputHash,
    taxonomy_version: profileTaxonomy,
  };
  const readinessIssues = sortedIssues(issues);
  const ready = readinessIssues.length === 0;
  return {
    ready,
    contract_version: PUBLICATION_CONTRACT_VERSION,
    policy_version: PUBLICATION_POLICY_VERSION,
    taxonomy_version: profileTaxonomy,
    result_hash: ready ? observationFactsHash(payload) : null,
    output_hash: outputHash,
    payload,
    metadata: {
      agent_model: profileModel,
      agent_config_id: profileConfigId,
      prompt_template_id: profilePromptTemplateId,
      prompt_hash: profilePromptHash,
      prompt_variant: profilePromptVariant,
      agent_version_hash: profileVersionHash,
      taxonomy_version: profileTaxonomy,
    },
    input_content: {
      content_count: inputIds?.length ?? null,
      content_hash: inputHash,
    },
    facts: factResults.map((fact) => ({
      field: fact.field,
      complete: fact.complete,
      missing_keys: fact.missing_keys,
      invalid_fields: fact.invalid_fields,
      fact_hash: fact.fact_hash,
    })),
    source_refs: trace,
    issues: readinessIssues,
  };
}
