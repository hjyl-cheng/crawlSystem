const monthCache = new Map();

function text(value) {
  const output = String(value ?? "").trim();
  return output || null;
}

function validDate(year, month, day) {
  const timestamp = Date.UTC(year, month - 1, day);
  if (!Number.isFinite(timestamp)) return null;
  const value = new Date(timestamp).toISOString().slice(0, 10);
  const expected = `${String(year).padStart(4, "0")}-${String(month).padStart(2, "0")}-${String(day).padStart(2, "0")}`;
  return value === expected ? value : null;
}

function existingDate(value) {
  const raw = text(value);
  if (!raw) return null;
  const match = raw.match(/^(\d{4})-(\d{2})-(\d{2})/);
  return match ? validDate(Number(match[1]), Number(match[2]), Number(match[3])) : null;
}

function normalizedToken(value, locale) {
  return String(value ?? "")
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/\./g, "")
    .toLocaleLowerCase(locale);
}

function monthTokens(localeValue) {
  let locale;
  try {
    locale = Intl.getCanonicalLocales(String(localeValue || "en").replaceAll("_", "-"))[0] || "en";
  } catch {
    locale = "en";
  }
  const cacheKey = `${locale}\u0000en`;
  if (monthCache.has(cacheKey)) return monthCache.get(cacheKey);
  const tokens = new Map();
  for (const candidateLocale of new Set([locale, "en"])) {
    for (let month = 1; month <= 12; month += 1) {
      for (const style of ["short", "long"]) {
        const formatted = new Intl.DateTimeFormat(candidateLocale, {
          month: style,
          timeZone: "UTC",
        }).format(new Date(Date.UTC(2020, month - 1, 1)));
        const token = normalizedToken(formatted, candidateLocale);
        if (token) tokens.set(token, month);
      }
    }
  }
  monthCache.set(cacheKey, { locale, tokens });
  return monthCache.get(cacheKey);
}

export function parseYoutubeJoinedDate(value, { locale = "en" } = {}) {
  const raw = text(value);
  if (!raw) return null;
  const iso = existingDate(raw);
  if (iso) return iso;
  const numericCjk = raw.match(/(\d{4})\D{0,3}(\d{1,2})\D{0,3}(\d{1,2})/u);
  if (numericCjk) {
    const parsed = validDate(Number(numericCjk[1]), Number(numericCjk[2]), Number(numericCjk[3]));
    if (parsed) return parsed;
  }

  const profile = monthTokens(locale);
  const tokens = normalizedToken(raw, profile.locale).split(/[^\p{L}\p{N}]+/u).filter(Boolean);
  const monthToken = tokens.find((token) => profile.tokens.has(token));
  if (!monthToken) return null;
  const yearToken = tokens.findLast((token) => /^\d{4}$/.test(token));
  const dayToken = tokens.find((token) => /^\d{1,2}$/.test(token) && Number(token) >= 1 && Number(token) <= 31);
  if (!yearToken || !dayToken) return null;
  return validDate(Number(yearToken), profile.tokens.get(monthToken), Number(dayToken));
}

export function normalizeJoinedDateCurrent({
  joinedAt,
  joinedDateText,
  precision,
  locale = "en",
} = {}) {
  const stored = existingDate(joinedAt);
  const storedPrecision = text(precision);
  const parsed = stored && ["date_only", "second", "exact"].includes(storedPrecision)
    ? stored
    : parseYoutubeJoinedDate(joinedDateText, { locale });
  return {
    value: parsed,
    raw: text(joinedDateText),
    status: parsed ? "exact" : text(joinedDateText) ? "unavailable" : "unresolved",
    precision: parsed ? "date_only" : "unknown",
  };
}
