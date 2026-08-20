const VERIFIED_STATUSES = new Set(["verified", "not_verified", "unknown"]);

function status(value) {
  const normalized = String(value ?? "").trim();
  return VERIFIED_STATUSES.has(normalized) ? normalized : "unknown";
}

export function normalizeVerifiedCurrent(value, statusValue) {
  const normalizedValue = typeof value === "boolean" ? value : null;
  const normalizedStatus = status(statusValue);
  const valid = (normalizedStatus === "verified" && normalizedValue === true)
    || (normalizedStatus === "not_verified" && normalizedValue === false)
    || (normalizedStatus === "unknown" && normalizedValue === null);
  return {
    value: valid ? normalizedValue : null,
    status: valid ? normalizedStatus : "unknown",
    valid,
  };
}
