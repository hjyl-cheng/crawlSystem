import { parseLocalizedCount } from "./localizedCount.js";
import { ParserContractError } from "./localizedParsing.js";

const DEFAULT_LOCALE = process.env.YOUTUBE_LANGUAGE || "pt-BR";
const DAY_MS = 86400000;
const profileCache = new Map();
const absoluteDateProfileCache = new Map();
const NUMBER_TYPES = new Set(["integer", "group", "decimal", "fraction"]);
const NUMBER_PATTERN = "([\\p{Decimal_Number}](?:[\\p{Decimal_Number}\\s\\u00a0\\u202f.,，．٫٬'’_]*?[\\p{Decimal_Number}])?)";
const DATE_PART_PATTERN = "[\\p{Decimal_Number}]";
const UNIT_DAYS = {
  second: 0,
  minute: 0,
  hour: 0,
  day: 1,
  week: 7,
  month: 30,
  year: 365,
};
const UNIT_MILLISECONDS = {
  second: 1000,
  minute: 60 * 1000,
  hour: 60 * 60 * 1000,
  day: DAY_MS,
  week: 7 * DAY_MS,
  month: 30 * DAY_MS,
  year: 365 * DAY_MS,
};

function canonicalLocale(value) {
  try {
    return Intl.getCanonicalLocales(String(value || DEFAULT_LOCALE).replaceAll("_", "-"))[0] || "en";
  } catch {
    return "en";
  }
}

function normalize(value, locale) {
  return String(value ?? "")
    .normalize("NFKC")
    .replace(/[\u200b-\u200f\u202a-\u202e\u2060-\u206f\ufeff]/g, "")
    .replace(/[\u00a0\u202f]/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .toLocaleLowerCase(locale);
}

function escapeRegex(value) {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function literalPattern(value, locale) {
  const normalized = normalize(value, locale);
  if (!normalized) return "";
  return normalized.split(" ").map(escapeRegex).join("\\s*");
}

function containsExactPhrase(value, phrase) {
  let offset = value.indexOf(phrase);
  while (offset >= 0) {
    const before = offset === 0 ? "" : value[offset - 1];
    const afterOffset = offset + phrase.length;
    const after = afterOffset >= value.length ? "" : value[afterOffset];
    const startsAsciiWord = /^[a-z0-9]/i.test(phrase);
    const endsAsciiWord = /[a-z0-9]$/i.test(phrase);
    if ((!startsAsciiWord || !/[a-z0-9]/i.test(before))
        && (!endsAsciiWord || !/[a-z0-9]/i.test(after))) return true;
    offset = value.indexOf(phrase, offset + 1);
  }
  return false;
}

function buildProfile(localeValue) {
  const locale = canonicalLocale(localeValue);
  const patterns = [];
  const exact = [];
  const seenPatterns = new Set();
  const seenExact = new Set();
  const samples = [1, 2, 3, 4, 5, 10, 11, 20, 21, 100];

  for (const [unit, days] of Object.entries(UNIT_DAYS)) {
    for (const style of ["long", "short", "narrow"]) {
      const always = new Intl.RelativeTimeFormat(locale, { numeric: "always", style });
      for (const amount of samples) {
        const parts = always.formatToParts(-amount, unit);
        const firstNumber = parts.findIndex((part) => NUMBER_TYPES.has(part.type));
        let lastNumber = -1;
        for (let index = parts.length - 1; index >= 0; index -= 1) {
          if (NUMBER_TYPES.has(parts[index].type)) {
            lastNumber = index;
            break;
          }
        }
        if (firstNumber < 0) {
          const phrase = normalize(parts.map((part) => part.value).join(""), locale);
          const key = `${phrase}:${amount * days}`;
          if (phrase && !seenExact.has(key)) {
            seenExact.add(key);
            exact.push({ phrase, amount, unit, ageDays: amount * days });
          }
          continue;
        }
        const prefix = parts.slice(0, firstNumber).map((part) => part.value).join("");
        const suffix = parts.slice(lastNumber + 1).map((part) => part.value).join("");
        const source = `${literalPattern(prefix, locale)}\\s*${NUMBER_PATTERN}\\s*${literalPattern(suffix, locale)}`;
        const key = `${source}:${days}`;
        if (seenPatterns.has(key)) continue;
        seenPatterns.add(key);
        patterns.push({ regex: new RegExp(source, "u"), unit, days });
      }

      const automatic = new Intl.RelativeTimeFormat(locale, { numeric: "auto", style });
      for (const amount of [0, 1, 2]) {
        const phrase = normalize(automatic.format(-amount, unit), locale);
        if (!phrase || /\p{Decimal_Number}/u.test(phrase)) continue;
        const key = `${phrase}:${amount * days}`;
        if (seenExact.has(key)) continue;
        seenExact.add(key);
        exact.push({ phrase, amount, unit, ageDays: amount * days });
      }
    }
  }

  patterns.sort((left, right) => right.regex.source.length - left.regex.source.length);
  exact.sort((left, right) => right.phrase.length - left.phrase.length);
  return { locale, patterns, exact };
}

function localeProfile(locale) {
  const canonical = canonicalLocale(locale);
  if (!profileCache.has(canonical)) profileCache.set(canonical, buildProfile(canonical));
  return profileCache.get(canonical);
}

function validUtcDay(year, month, day) {
  if (!Number.isSafeInteger(year) || year < 1900 || year > 3000
      || !Number.isSafeInteger(month) || month < 1 || month > 12
      || !Number.isSafeInteger(day) || day < 1 || day > 31) return null;
  const timestamp = Date.UTC(year, month - 1, day);
  if (!Number.isFinite(timestamp)) return null;
  const parsed = new Date(timestamp);
  if (parsed.getUTCFullYear() !== year
      || parsed.getUTCMonth() !== month - 1
      || parsed.getUTCDate() !== day) return null;
  return parsed.toISOString().slice(0, 10);
}

function buildAbsoluteDateProfile(localeValue) {
  const locale = canonicalLocale(localeValue);
  const patterns = [];
  const seen = new Set();
  for (const monthStyle of ["long", "short", "numeric", "2-digit"]) {
    const formatter = new Intl.DateTimeFormat(locale, {
      calendar: "gregory",
      timeZone: "UTC",
      year: "numeric",
      month: monthStyle,
      day: "numeric",
    });
    const monthNumbers = new Map();
    if (!["numeric", "2-digit"].includes(monthStyle)) {
      for (let month = 1; month <= 12; month += 1) {
        const monthPart = formatter.formatToParts(new Date(Date.UTC(2006, month - 1, 22)))
          .find((part) => part.type === "month")?.value;
        const token = normalize(monthPart, locale);
        if (token) monthNumbers.set(token, month);
      }
    }
    const parts = formatter.formatToParts(new Date(Date.UTC(2006, 10, 22)));
    if (!parts.some((part) => part.type === "year")
        || !parts.some((part) => part.type === "month")
        || !parts.some((part) => part.type === "day")) continue;
    const source = parts.map((part) => {
      if (part.type === "year") return `(?<year>${DATE_PART_PATTERN}{4})`;
      if (part.type === "day") return `(?<day>${DATE_PART_PATTERN}{1,2})`;
      if (part.type !== "month") return `\\s*${literalPattern(part.value, locale)}\\s*`;
      if (["numeric", "2-digit"].includes(monthStyle)) {
        return `(?<month>${DATE_PART_PATTERN}{1,2})`;
      }
      const alternatives = [...monthNumbers.keys()]
        .sort((left, right) => right.length - left.length)
        .map((token) => literalPattern(token, locale));
      return alternatives.length > 0 ? `(?<month>${alternatives.join("|")})` : "";
    }).join("");
    if (!source || seen.has(source)) continue;
    seen.add(source);
    patterns.push({ regex: new RegExp(source, "u"), monthNumbers });
  }
  return { locale, patterns };
}

function absoluteDateProfile(locale) {
  const canonical = canonicalLocale(locale);
  if (!absoluteDateProfileCache.has(canonical)) {
    absoluteDateProfileCache.set(canonical, buildAbsoluteDateProfile(canonical));
  }
  return absoluteDateProfileCache.get(canonical);
}

export function localizedAbsoluteUtcDay(value, { locale = DEFAULT_LOCALE } = {}) {
  const raw = String(value ?? "").trim();
  if (!raw) return null;
  const isoDate = raw.match(/^(\d{4})-(\d{2})-(\d{2})(?:$|[T\s])/);
  if (isoDate) {
    return validUtcDay(Number(isoDate[1]), Number(isoDate[2]), Number(isoDate[3]));
  }

  const profile = absoluteDateProfile(locale);
  const normalized = normalize(raw, profile.locale);
  for (const pattern of profile.patterns) {
    const match = pattern.regex.exec(normalized);
    if (!match?.groups) continue;
    const year = parseLocalizedCount(match.groups.year, { locale: profile.locale });
    const day = parseLocalizedCount(match.groups.day, { locale: profile.locale });
    const month = pattern.monthNumbers.size === 0
      ? parseLocalizedCount(match.groups.month, { locale: profile.locale })
      : pattern.monthNumbers.get(normalize(match.groups.month, profile.locale));
    const result = validUtcDay(year, month, day);
    if (result) return result;
  }
  return null;
}

export function parseLocalizedAgeDays(value, { locale = DEFAULT_LOCALE, now = Date.now() } = {}) {
  const raw = String(value ?? "").trim();
  if (!raw) return null;
  const isoDate = raw.match(/^(\d{4})-(\d{2})-(\d{2})/);
  if (isoDate) {
    const publishedAt = Date.UTC(Number(isoDate[1]), Number(isoDate[2]) - 1, Number(isoDate[3]));
    if (Number.isFinite(publishedAt)) return Math.max(0, Math.floor((Number(now) - publishedAt) / 86400000));
  }

  const age = parseLocalizedRelativeAge(raw, { locale });
  return age == null ? null : age.amount * UNIT_DAYS[age.unit];
}

export function parseLocalizedRelativeAge(value, { locale = DEFAULT_LOCALE } = {}) {
  const raw = String(value ?? "").trim();
  if (!raw) return null;
  const profile = localeProfile(locale);
  const normalized = normalize(raw, profile.locale);
  for (const item of profile.patterns) {
    const match = item.regex.exec(normalized);
    if (!match) continue;
    const amount = parseLocalizedCount(match[1], { locale: profile.locale });
    if (amount == null) continue;
    return { amount, unit: item.unit };
  }
  for (const item of profile.exact) {
    if (containsExactPhrase(normalized, item.phrase)) {
      return { amount: item.amount, unit: item.unit };
    }
  }
  return null;
}

export function localizedEstimatedUtcTimestamp(value, {
  locale = DEFAULT_LOCALE,
  now = Date.now(),
} = {}) {
  const observedAt = Number(now);
  if (!Number.isFinite(observedAt)) return null;
  const age = parseLocalizedRelativeAge(value, { locale });
  if (!age) return null;
  const timestamp = observedAt - (age.amount * UNIT_MILLISECONDS[age.unit]);
  const result = new Date(timestamp);
  return Number.isNaN(result.getTime()) ? null : result.toISOString();
}

export function localizedPublishedUtcDay(value, {
  locale = DEFAULT_LOCALE,
  now = Date.now(),
} = {}) {
  const raw = String(value ?? "").trim();
  if (!raw) return null;
  const absoluteDay = localizedAbsoluteUtcDay(raw, { locale });
  if (absoluteDay) return absoluteDay;
  if (/^\d{4}-\d{2}-\d{2}(?:$|[T\s])/.test(raw)) return null;

  const ageDays = parseLocalizedAgeDays(raw, { locale, now });
  if (ageDays == null || !Number.isFinite(ageDays)) return null;
  const observed = new Date(Number(now));
  if (Number.isNaN(observed.getTime())) return null;
  const observedDay = Date.UTC(
    observed.getUTCFullYear(),
    observed.getUTCMonth(),
    observed.getUTCDate(),
  );
  return new Date(observedDay - (Math.floor(ageDays) * DAY_MS)).toISOString().slice(0, 10);
}

export function parseRequiredLocalizedAgeDays(value, {
  locale = DEFAULT_LOCALE,
  field = "published_age",
  source = null,
  now = Date.now(),
} = {}) {
  const ageDays = parseLocalizedAgeDays(value, { locale, now });
  if (ageDays != null) return ageDays;
  throw new ParserContractError({
    field,
    value,
    locale: canonicalLocale(locale),
    source,
    reason: "unsupported_localized_relative_time",
  });
}
