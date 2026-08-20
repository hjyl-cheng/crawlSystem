function utcDayNumber(value) {
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return null;
  return Math.floor(Date.UTC(
    parsed.getUTCFullYear(),
    parsed.getUTCMonth(),
    parsed.getUTCDate(),
  ) / 86400000);
}

export function detailAgeDays(detail, now = Date.now()) {
  const raw = detail?.published_at ?? detail?.published_text ?? null;
  if (!raw) return null;
  const publishedDay = utcDayNumber(raw);
  const referenceDay = utcDayNumber(now);
  if (publishedDay == null || referenceDay == null) return null;
  return Math.max(0, referenceDay - publishedDay);
}

export function isOutsideContentWindow(detail, maxAgeDays, now = Date.now()) {
  const limit = Number(maxAgeDays);
  if (!Number.isFinite(limit) || limit <= 0) return false;
  const ageDays = detailAgeDays(detail, now);
  return ageDays != null && ageDays > limit;
}
