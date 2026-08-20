import { spawn } from "node:child_process";

const PROFILE_FIELDS = Object.freeze([
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
]);

function text(value) {
  const output = String(value ?? "").trim();
  return output || null;
}

function finiteProbability(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? Math.max(0, Math.min(1, parsed)) : 0;
}

function confidenceLabel(fact) {
  const score = Math.min(
    finiteProbability(fact?.model_confidence),
    finiteProbability(fact?.evidence_confidence),
  );
  if (score >= 0.8) return "high";
  if (score >= 0.55) return "medium";
  return "low";
}

function canonicalize(value) {
  if (value === null || typeof value !== "object") return value;
  if (Array.isArray(value)) return value.map(canonicalize);
  return Object.fromEntries(
    Object.keys(value).sort().map((key) => [key, canonicalize(value[key])]),
  );
}

function equalJson(left, right) {
  return JSON.stringify(canonicalize(left)) === JSON.stringify(canonicalize(right));
}

function runtimeProcessor(result) {
  const processor = result?.analysis_result?.processor;
  if (!processor || typeof processor !== "object" || Array.isArray(processor)) {
    throw new TypeError("local profile result is missing analysis_result.processor");
  }
  return processor;
}

export function localProfileRuntimeModelId(result) {
  const processor = runtimeProcessor(result);
  const versions = [
    processor.version,
    processor.model_bundle_version,
    processor.prior_catalog_version,
    processor.taxonomy_version,
  ].map(text);
  if (versions.some((value) => !value)) {
    throw new TypeError("local profile runtime did not return complete version identity");
  }
  return ["qy-channel-profile", ...versions].join(":");
}

function evidenceFor(field, fact) {
  const references = Array.isArray(fact?.evidence_refs)
    ? [...new Set(fact.evidence_refs.map(text).filter(Boolean))]
    : [];
  if (references.length > 0) return references;
  return [
    [
      `Local ${field} estimate`,
      `source_type=${text(fact?.source_type) ?? "unspecified"}`,
      `truth_status=${text(fact?.truth_status) ?? "estimated"}`,
      `evidence_strength=${text(fact?.evidence_strength) ?? "unknown"}`,
    ].join("; "),
  ];
}

function localFact(field, fact, inputUrl) {
  if (!fact || typeof fact !== "object" || Array.isArray(fact) || !("value" in fact)) {
    throw new TypeError(`local profile result is missing facts.${field}`);
  }
  if (fact.value == null || fact.abstained === true) {
    throw new TypeError(`local profile complete_estimate did not resolve facts.${field}`);
  }
  return {
    value: fact.value,
    reason: null,
    source: `local_profile:${text(fact.source_type) ?? "estimate"}`,
    evidence: evidenceFor(field, fact),
    confidence: confidenceLabel(fact),
    source_urls: [inputUrl],
  };
}

function validateRuntimeResult(result) {
  const channelId = text(result?.channel_id);
  const inputUrl = text(result?.input_url);
  const analysis = result?.analysis_result;
  if (!channelId) throw new TypeError("local profile result channel_id is required");
  if (!inputUrl) throw new TypeError(`local profile result input_url is required for ${channelId}`);
  if (!analysis || typeof analysis !== "object" || Array.isArray(analysis)) {
    throw new TypeError(`local profile result analysis_result is required for ${channelId}`);
  }
  if (text(analysis.channel_id) !== channelId || text(analysis.input_url) !== inputUrl) {
    throw new TypeError(`local profile result identity mismatch for ${channelId}`);
  }
  if (analysis.analysis_status !== "completed_with_estimates") {
    throw new TypeError(`local profile result is incomplete for ${channelId}`);
  }
  if (!result.payload || typeof result.payload !== "object" || Array.isArray(result.payload)) {
    throw new TypeError(`local profile compatibility payload is required for ${channelId}`);
  }
  for (const field of PROFILE_FIELDS) {
    if (!equalJson(result.payload[field], analysis.facts?.[field]?.value)) {
      throw new TypeError(`local profile compatibility value mismatch for ${channelId}.${field}`);
    }
  }
  if (!Array.isArray(result.input_content_ids)) {
    throw new TypeError(`local profile input_content_ids must be an array for ${channelId}`);
  }
  return { channelId, inputUrl, analysis };
}

function metricsFromRuntimeResult(result) {
  const { inputUrl, analysis } = validateRuntimeResult(result);
  const runtimeModelId = localProfileRuntimeModelId(result);
  const processor = runtimeProcessor(result);
  const facts = Object.fromEntries(
    PROFILE_FIELDS.map((field) => [field, localFact(field, analysis.facts[field], inputUrl)]),
  );
  return {
    audience_profile_agent: facts,
    profile_processing_context: {
      executor: "local-offline",
      runtime_model_id: runtimeModelId,
      snapshot_as_of: analysis.snapshot?.as_of ?? null,
      snapshot_hash: analysis.snapshot?.snapshot_hash ?? null,
      input_content_hash: analysis.snapshot?.input_content_hash ?? null,
      processor_version: processor.version,
      model_bundle_version: processor.model_bundle_version,
      model_bundle_hash: processor.model_bundle_hash ?? null,
      taxonomy_version: processor.taxonomy_version,
      prior_catalog_version: processor.prior_catalog_version,
      prior_catalog_hash: processor.prior_catalog_hash ?? null,
      diagnostics: Array.isArray(analysis.diagnostics) ? analysis.diagnostics : [],
    },
  };
}

function positiveInteger(value, fallback, maximum) {
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed <= 0) return fallback;
  return Math.min(parsed, maximum);
}

function appendBounded(chunks, chunk, currentBytes, maximumBytes, streamName, child) {
  const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
  const nextBytes = currentBytes + buffer.length;
  if (nextBytes > maximumBytes) {
    child.kill("SIGKILL");
    throw new Error(`local profile ${streamName} exceeded ${maximumBytes} bytes`);
  }
  chunks.push(buffer);
  return nextBytes;
}

export async function invokeLocalProfileRuntime(
  request,
  { config = {}, environment = process.env } = {},
) {
  const python = text(environment.LOCAL_PROFILE_PYTHON_BIN) ?? "python3";
  const modelBundle = text(environment.LOCAL_PROFILE_MODEL_BUNDLE);
  if (!modelBundle) throw new Error("LOCAL_PROFILE_MODEL_BUNDLE is required");
  const args = [
    "-m",
    "qy_channel_profile",
    "analyze-db-runtime",
    "--model-bundle",
    modelBundle,
    "--content-limit",
    String(positiveInteger(environment.LOCAL_PROFILE_CONTENT_LIMIT, 30, 100)),
    "--statement-timeout-seconds",
    String(positiveInteger(environment.LOCAL_PROFILE_DB_TIMEOUT_SECONDS, 60, 300)),
  ];
  const priorCatalog = text(environment.LOCAL_PROFILE_PRIOR_CATALOG);
  if (priorCatalog) args.push("--prior-catalog", priorCatalog);

  const timeoutMs = positiveInteger(
    environment.LOCAL_PROFILE_TIMEOUT_MS ?? config.timeout_ms,
    10 * 60 * 1000,
    60 * 60 * 1000,
  );
  const maximumStdoutBytes = positiveInteger(
    environment.LOCAL_PROFILE_MAX_STDOUT_BYTES,
    64 * 1024 * 1024,
    256 * 1024 * 1024,
  );
  const maximumStderrBytes = positiveInteger(
    environment.LOCAL_PROFILE_MAX_STDERR_BYTES,
    2 * 1024 * 1024,
    16 * 1024 * 1024,
  );

  return new Promise((resolve, reject) => {
    const child = spawn(python, args, {
      env: environment,
      stdio: ["pipe", "pipe", "pipe"],
      shell: false,
    });
    const stdout = [];
    const stderr = [];
    let stdoutBytes = 0;
    let stderrBytes = 0;
    let streamError = null;
    const timer = setTimeout(() => {
      child.kill("SIGKILL");
      streamError = new Error(`local profile runtime timed out after ${timeoutMs}ms`);
    }, timeoutMs);
    timer.unref?.();

    child.stdout.on("data", (chunk) => {
      if (streamError) return;
      try {
        stdoutBytes = appendBounded(stdout, chunk, stdoutBytes, maximumStdoutBytes, "stdout", child);
      } catch (error) {
        streamError = error;
      }
    });
    child.stderr.on("data", (chunk) => {
      if (streamError) return;
      try {
        stderrBytes = appendBounded(stderr, chunk, stderrBytes, maximumStderrBytes, "stderr", child);
      } catch (error) {
        streamError = error;
      }
    });
    child.once("error", (error) => {
      clearTimeout(timer);
      reject(error);
    });
    child.once("close", (code, signal) => {
      clearTimeout(timer);
      if (streamError) {
        reject(streamError);
        return;
      }
      const output = Buffer.concat(stdout).toString("utf8").trim();
      const diagnostic = Buffer.concat(stderr).toString("utf8").trim();
      let parsed;
      try {
        parsed = JSON.parse(output);
      } catch (error) {
        reject(new Error(
          `local profile runtime returned invalid JSON (exit=${code}, signal=${signal ?? "none"}): `
          + `${diagnostic || error.message}`.slice(0, 2000),
        ));
        return;
      }
      if (code !== 0 && !Array.isArray(parsed?.errors)) {
        reject(new Error(
          `local profile runtime failed (exit=${code}, signal=${signal ?? "none"}): ${diagnostic}`.slice(0, 2000),
        ));
        return;
      }
      resolve(parsed);
    });
    child.stdin.end(`${JSON.stringify(request)}\n`);
  });
}

function latestRunMap(value) {
  if (value instanceof Map) return value;
  if (value && typeof value === "object" && !Array.isArray(value)) return new Map(Object.entries(value));
  throw new TypeError("loadLatestRunIds must return a Map or object");
}

function runtimeErrorKind(message) {
  if (/^SnapshotNotFound:/i.test(String(message ?? ""))) return "snapshot_not_found";
  return "local_runtime";
}

export class LocalOfflineProfileExecutor {
  constructor({
    invokeRuntime = invokeLocalProfileRuntime,
    loadLatestRunIds,
  } = {}) {
    if (typeof invokeRuntime !== "function") throw new TypeError("invokeRuntime is required");
    if (typeof loadLatestRunIds !== "function") throw new TypeError("loadLatestRunIds is required");
    this.invokeRuntime = invokeRuntime;
    this.loadLatestRunIds = loadLatestRunIds;
  }

  async execute({ config, channelIds }) {
    if (String(config?.provider ?? "").toLowerCase() !== "local-offline") {
      throw new TypeError("LocalOfflineProfileExecutor requires provider=local-offline");
    }
    const ids = [...new Set((channelIds ?? []).map(text).filter(Boolean))];
    if (ids.length === 0 || ids.length > 50) {
      throw new TypeError("channelIds must contain between 1 and 50 unique Channel IDs");
    }
    const report = await this.invokeRuntime({ channel_ids: ids }, { config });
    const runtimeResults = Array.isArray(report?.results) ? report.results : [];
    const runtimeErrors = Array.isArray(report?.errors) ? report.errors : [];
    const latest = latestRunMap(await this.loadLatestRunIds(
      runtimeResults.map((result) => text(result?.channel_id)).filter(Boolean),
    ));
    const results = new Map();
    const errors = runtimeErrors.map((item) => ({
      channel_id: text(item?.channel_id),
      input_url: text(item?.input_url),
      error: text(item?.error) ?? "local profile runtime failed",
      error_kind: runtimeErrorKind(item?.error),
      retryable: !/^SnapshotNotFound:/i.test(String(item?.error ?? "")),
    }));

    for (const result of runtimeResults) {
      let identity;
      try {
        identity = validateRuntimeResult(result);
        const expected = text(result.source_latest_run_id);
        const actual = text(latest.get(identity.channelId));
        if (expected !== actual) {
          errors.push({
            channel_id: identity.channelId,
            input_url: identity.inputUrl,
            error: `local profile snapshot is stale: expected latest_run_id=${expected ?? "null"}, actual=${actual ?? "null"}`,
            error_kind: "stale_snapshot",
            retryable: true,
          });
          continue;
        }
        results.set(identity.channelId, {
          metrics: metricsFromRuntimeResult(result),
          agent_model: localProfileRuntimeModelId(result),
          agent_applied: true,
          country_required: false,
          execution_variant: "local_offline",
          input_content_ids: [...new Set(result.input_content_ids.map(text).filter(Boolean))],
          source_latest_run_id: expected,
        });
      } catch (error) {
        errors.push({
          channel_id: text(result?.channel_id),
          input_url: text(result?.input_url),
          error: String(error?.message ?? error),
          error_kind: "invalid_local_result",
          retryable: false,
        });
      }
    }
    return { results, errors, skipped: false };
  }
}
