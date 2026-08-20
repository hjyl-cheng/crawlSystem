const DESCRIPTION_STATUSES = new Set(["exact", "empty", "unavailable", "unresolved"]);

function normalizedKey(value) {
  return String(value).normalize("NFKC").toLocaleLowerCase();
}

function flattenStrings(values) {
  const output = [];
  const visit = (value) => {
    if (Array.isArray(value)) {
      for (const item of value) visit(item);
      return;
    }
    if (typeof value !== "string") return;
    const trimmed = value.trim();
    if (trimmed) output.push(trimmed);
  };
  for (const value of values) visit(value);
  return output;
}

export function normalizeVideoKeywords(...values) {
  const seen = new Set();
  const output = [];
  for (const value of flattenStrings(values)) {
    const key = normalizedKey(value);
    if (seen.has(key)) continue;
    seen.add(key);
    output.push(value);
  }
  return output;
}

export function extractVideoHashtags(...values) {
  const seen = new Set();
  const output = [];
  const pattern = /(^|[^\p{L}\p{N}\p{M}\p{Join_Control}_])#([\p{L}\p{N}\p{M}\p{Join_Control}_]+)/gu;
  for (const value of flattenStrings(values)) {
    for (const match of value.matchAll(pattern)) {
      const hashtag = `#${match[2]}`;
      const key = normalizedKey(hashtag);
      if (seen.has(key)) continue;
      seen.add(key);
      output.push(hashtag);
    }
  }
  return output;
}

export function descriptionState(value, {
  source = null,
  missingStatus = "unresolved",
} = {}) {
  if (typeof value === "string") {
    const exact = value.trim().length > 0;
    return {
      description: exact ? value : "",
      description_status: exact ? "exact" : "empty",
      description_source: source,
    };
  }
  return {
    description: null,
    description_status: DESCRIPTION_STATUSES.has(missingStatus) ? missingStatus : "unresolved",
    description_source: null,
  };
}

export function normalizeVideoTextMetadata(detail, { afterApi = false } = {}) {
  const output = { ...(detail ?? {}) };
  const hasOwn = (key) => Object.prototype.hasOwnProperty.call(output, key);
  const hasDescriptionValue = typeof output.description === "string";
  const keywordsObserved = typeof output.keywords_observed === "boolean"
    ? output.keywords_observed
    : hasOwn("keywords") || hasOwn("tags");
  const hashtagsObserved = typeof output.hashtags_observed === "boolean"
    ? output.hashtags_observed
    : typeof output.title === "string" && hasDescriptionValue;
  const inferred = descriptionState(output.description, {
    source: output.description_source ?? output.source ?? null,
    missingStatus: afterApi ? "unavailable" : "unresolved",
  });
  const explicitStatus = DESCRIPTION_STATUSES.has(output.description_status)
    ? output.description_status
    : null;
  output.description = inferred.description;
  output.description_status = hasDescriptionValue
    ? inferred.description_status
    : afterApi
      ? "unavailable"
      : ["unavailable", "unresolved"].includes(explicitStatus) ? explicitStatus : "unresolved";
  output.description_source = ["exact", "empty"].includes(output.description_status)
    ? output.description_source ?? inferred.description_source
    : null;
  output.keywords = normalizeVideoKeywords(output.keywords, output.tags);
  output.keywords_observed = keywordsObserved;
  output.hashtags = extractVideoHashtags(output.title, output.description);
  output.hashtags_observed = hashtagsObserved;
  return output;
}

export function hasResolvedDescription(detail) {
  return ["exact", "empty"].includes(detail?.description_status);
}
