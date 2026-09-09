import { AsyncLocalStorage } from "node:async_hooks";

const storage = new AsyncLocalStorage();
export const EMPTY_UPLOADS_REASON = "uploads_empty";

export function uploadsCountryCode(value) {
  const country = String(value ?? "").trim().toUpperCase();
  return /^[A-Z]{2}$/.test(country) ? country : null;
}

export class UploadsCountryRecheck extends Error {
  constructor(country) {
    super(`Uploads require a country recheck: ${country}`);
    this.name = "UploadsCountryRecheck";
    this.code = "UPLOADS_COUNTRY_RECHECK";
    this.country = country;
  }
}

export function withUploadsCountryExecution(context, callback) {
  return storage.run(context, callback);
}

// Only call for a verified empty feed or explicit missing-uploads response. Country
// comes from About (or a persisted About-based recheck), never the UI locale.
export function emptyUploadsDecision(countryValue) {
  const country = uploadsCountryCode(countryValue);
  const context = storage.getStore();
  const current = uploadsCountryCode(context?.egressCountry);
  const prior = context?.recheck;
  let reason = "no_country";
  if (country) {
    if (current === country) reason = "country_checked";
    else if (prior?.country) reason = prior.country === country && prior.status === "unavailable"
      ? "no_country_reserve" : "country_recheck_exhausted";
    else if (context) throw new UploadsCountryRecheck(country);
    else throw new Error("Country recheck requires a managed execution");
  }
  return { version: 1, outcome: "dormant", reason, country };
}

// Dormant probes with a saved country must check that country before fetching;
// a no-reserve receipt lets the caller keep the channel dormant without HTTP.
export function prepareDormantUploadsProbe(countryValue) {
  const country = uploadsCountryCode(countryValue);
  if (!country) return null;
  const context = storage.getStore();
  if (uploadsCountryCode(context?.egressCountry) === country) return null;
  return emptyUploadsDecision(country);
}

export function pendingUploadsDormancy(countryValue) {
  const context = storage.getStore();
  if (!context?.recheck?.country) return null;
  const country = uploadsCountryCode(countryValue);
  if (country && uploadsCountryCode(context.egressCountry) === country) return null;
  return emptyUploadsDecision(country);
}

export function dormantUploadsScan(channelId, anchors, decision) {
  return {
    channel_id: channelId,
    playlist_id: channelId.startsWith("UC") ? `UU${channelId.slice(2)}` : channelId,
    entries: [], pages: 0, item_count: 0, parse_gap_count: 0,
    first_page_item_count: 0, catch_up_item_count: 0,
    anchor_matched: false, matched_anchor_id: null, crossed_anchor_ids: [],
    active_anchor_id: anchors?.[0]?.id ?? null,
    stop_reason: "list_end", terminal_reason: "list_end", complete: true,
    empty_uploads: decision,
    raw: { request_count: 0, country_probe_skipped: true },
  };
}

export function uploadsResponseEvidence(value) {
  let videoIds = 0;
  const messages = [];
  const errors = [];
  const visit = node => {
    if (!node || typeof node !== "object") return;
    for (const [key, child] of Object.entries(node)) {
      if (key === "videoId" && typeof child === "string") videoIds += 1;
      if (key === "messageRenderer") {
        const text = child?.text;
        messages.push(String(text?.simpleText ?? text?.runs?.map(run => run.text ?? "").join("") ?? "").slice(0, 300));
      }
      if (key === "alertRenderer" && child?.type === "ERROR") {
        const text = child.text;
        errors.push(String(text?.simpleText ?? text?.runs?.map(run => run.text ?? "").join("") ?? "").slice(0, 300));
      }
      visit(child);
    }
  };
  visit(value);
  return { video_id_count: videoIds, messages: messages.slice(0, 5), errors: errors.slice(0, 5) };
}

export function isMissingUploadsResponse(evidence) {
  return evidence?.video_id_count === 0 && evidence.errors?.length === 1
    && evidence.errors[0] === "The playlist does not exist.";
}

export function isMissingUploadsError(error, evidence) {
  return error?.message === "The playlist does not exist."
    && error.info?.type === "Alert" && error.info?.alert_type === "ERROR"
    && isMissingUploadsResponse(evidence);
}

export function assertNormalEmptyUploadsResponse(feed, evidence) {
  if ((feed.videos || feed.items || []).length > 0) return;
  // Empty continuations can be followed normally; they are not proof of an
  // empty playlist. Unknown renderers must not silently become dormancy.
  if (feed.has_continuation) return;
  if (isMissingUploadsResponse(evidence)) return;
  if (!evidence || evidence.video_id_count > 0
      || !evidence.messages.some(message => /no videos in this playlist|this playlist is empty/i.test(message))) {
    const error = new Error("YouTube uploads returned an unverified empty response");
    error.code = "UPLOADS_EMPTY_RESPONSE_UNVERIFIED";
    error.uploads_response_evidence = evidence ?? null;
    throw error;
  }
}
