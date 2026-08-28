import {
  classifyPublicationWindow,
  publicationEvidenceFromFields,
} from "./publicationTimeEvidence.js";

export const CONTENT_WINDOW_POLICY_VERSION = "content-window-v2";

function utcDayNumber(value) {
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return null;
  return Math.floor(Date.UTC(
    parsed.getUTCFullYear(),
    parsed.getUTCMonth(),
    parsed.getUTCDate(),
  ) / 86400000);
}

function contentPublicationEvidence(detail) {
  const raw = detail?.published_at ?? detail?.published_text ?? null;
  return publicationEvidenceFromFields(detail, { publishedAt: raw });
}

export function detailAgeDays(detail, now = Date.now()) {
  const raw = detail?.published_at ?? detail?.published_text ?? null;
  if (!raw) return null;
  const publishedDay = utcDayNumber(raw);
  const referenceDay = utcDayNumber(now);
  if (publishedDay == null || referenceDay == null) return null;
  return Math.max(0, referenceDay - publishedDay);
}

export function classifyContentWindow(detail, maxAgeDays, now = Date.now()) {
  const limit = Number(maxAgeDays);
  return classifyPublicationWindow(contentPublicationEvidence(detail), {
    asOf: now,
    maxAgeDays: limit,
  });
}

export function isOutsideContentWindow(detail, maxAgeDays, now = Date.now()) {
  return classifyContentWindow(detail, maxAgeDays, now).relation === "outside";
}
