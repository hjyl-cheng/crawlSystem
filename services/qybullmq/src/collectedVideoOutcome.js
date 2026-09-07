import { resolveVideoDisposition } from "./videoDisposition.js";

function text(value) {
  return String(value ?? "").trim() || null;
}

export const ACCESS_ONLY_STATUSES = new Set([
  "unlisted",
  "members_only",
  "private",
  "unavailable",
]);
const NEW_CONTENT_ACCESS_STATUSES = new Set([
  "public",
  "unlisted",
  "members_only",
]);

export function fullVideoStorageAction({ candidate, classification, access } = {}) {
  const existingType = text(candidate?.known_content_type);
  const existingKey = text(candidate?.known_content_key);
  const accessStatus = text(access?.access_status);
  const classifiedType = classification?.authoritative === true
    && ["video", "short", "live"].includes(text(classification.content_type))
    ? text(classification.content_type)
    : null;
  const classifiedSource = classifiedType ? text(classification.source) : null;
  if (existingKey
      && ["video", "short", "live"].includes(existingType)
      && ACCESS_ONLY_STATUSES.has(accessStatus)) {
    return {
      kind: "update_access",
      content_key: existingKey,
      content_type: existingType,
      type_source: text(candidate?.known_content_type_source),
    };
  }
  if (classifiedType && NEW_CONTENT_ACCESS_STATUSES.has(accessStatus)) {
    return {
      kind: "upsert",
      content_type: classifiedType,
      type_source: classifiedSource,
    };
  }
  if (classifiedType) {
    return {
      kind: "classified_only",
      content_type: classifiedType,
      type_source: classifiedSource,
    };
  }
  return { kind: "unresolved" };
}

export function resolveCollectedVideoOutcome({ candidate = {}, classification, access, detail,
  error = null, observedAt, terminalReason = null, deferredReason = null, priorDisposition = null,
}) {
  const storageAction = terminalReason ? { kind: "unresolved" }
    : fullVideoStorageAction({ candidate, classification, access });
  const disposition = resolveVideoDisposition({
    storageAction, classification, access, detail, error, observedAt,
    terminalReason, deferredReason, priorDisposition,
  });
  return { storageAction, disposition };
}
