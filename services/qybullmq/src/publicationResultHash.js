import { observationFactsHash } from "./crawlObservationStore.js";

const DOMAINS = new Set(["channel", "video", "agent"]);
const HASH_PATTERN = /^sha256:[0-9a-f]{64}$/;

function object(value, field) {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new TypeError(`${field} must be an object`);
  }
  return value;
}

function text(value, field) {
  const output = String(value ?? "").trim();
  if (!output) throw new TypeError(`${field} is required`);
  return output;
}

function positiveInteger(value, field) {
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed <= 0) {
    throw new TypeError(`${field} must be a positive integer`);
  }
  return parsed;
}

function videoResultValue(payload) {
  const windowPolicy = object(payload.window_policy, "video payload window_policy");
  if (!Array.isArray(payload.items)) throw new TypeError("video payload items must be an array");
  const positions = new Set();
  const contentIds = new Set();
  const items = payload.items.map((itemValue) => {
    const item = object(itemValue, "video payload item");
    const position = positiveInteger(item.position, "video payload item position");
    const contentId = text(item.content_id, "video payload item content_id");
    const itemHash = text(item.item_hash, "video payload item item_hash");
    if (!HASH_PATTERN.test(itemHash)) throw new TypeError("video payload item_hash must be canonical");
    if (positions.has(position)) throw new TypeError("video payload item positions must be unique");
    if (contentIds.has(contentId)) throw new TypeError("video payload content_ids must be unique");
    positions.add(position);
    contentIds.add(contentId);
    return { position, content_id: contentId, item_hash: itemHash };
  });
  if (items.some((item, index) => item.position !== index + 1)) {
    throw new TypeError("video payload item positions must be contiguous from 1");
  }
  return {
    channel_id: text(payload.channel_id, "video payload channel_id"),
    policy_version: text(windowPolicy.policy_version, "video payload policy_version"),
    max_age_days: positiveInteger(windowPolicy.max_age_days, "video payload max_age_days"),
    max_items: positiveInteger(windowPolicy.max_items, "video payload max_items"),
    items,
  };
}

export function publicationResultHash(domainValue, payloadValue) {
  const domain = text(domainValue, "Publication Domain");
  if (!DOMAINS.has(domain)) throw new TypeError(`unsupported Publication Domain: ${domain}`);
  const payload = object(payloadValue, `${domain} payload`);
  return observationFactsHash(domain === "video" ? videoResultValue(payload) : payload);
}
