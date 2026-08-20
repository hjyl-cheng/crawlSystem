import { ParserContractError } from "./localizedParsing.js";

const DEFAULT_LOCALE = process.env.YOUTUBE_LANGUAGE || "pt-BR";

const profileCache = new Map();

// Unicode decimal digit blocks. NFKC handles compatibility digits; these
// ranges cover native decimal systems that JavaScript Number() does not parse.
const DECIMAL_ZERO_CODE_POINTS = [
  0x0030, 0x0660, 0x06f0, 0x07c0, 0x0966, 0x09e6, 0x0a66, 0x0ae6,
  0x0b66, 0x0be6, 0x0c66, 0x0ce6, 0x0d66, 0x0de6, 0x0e50, 0x0ed0,
  0x0f20, 0x1040, 0x1090, 0x17e0, 0x1810, 0x1946, 0x19d0, 0x1a80,
  0x1a90, 0x1b50, 0x1bb0, 0x1c40, 0x1c50, 0xa620, 0xa8d0, 0xa900,
  0xa9d0, 0xa9f0, 0xaa50, 0xabf0, 0xff10, 0x104a0, 0x10d30, 0x11066,
  0x110f0, 0x11136, 0x111d0, 0x112f0, 0x11450, 0x114d0, 0x11650,
  0x116c0, 0x11730, 0x118e0, 0x11950, 0x11c50, 0x11d50, 0x11da0,
  0x11f50, 0x16a60, 0x16ac0, 0x16b50, 0x1d7ce, 0x1d7d8, 0x1d7e2,
  0x1d7ec, 0x1d7f6, 0x1e140, 0x1e2f0, 0x1e4f0, 0x1e950, 0x1fbf0,
];

const COMMON_COMPACT_UNITS = [
  ["thousand", 1_000], ["thousands", 1_000], ["mil", 1_000],
  ["mille", 1_000], ["mila", 1_000], ["тыс", 1_000], ["тис", 1_000],
  ["हजार", 1_000], ["हज़ार", 1_000], ["ألف", 1_000], ["الف", 1_000],
  ["هزار", 1_000], ["천", 1_000], ["千", 1_000], ["rb", 1_000],
  ["million", 1_000_000], ["millions", 1_000_000],
  ["milhao", 1_000_000], ["milhoes", 1_000_000], ["milhão", 1_000_000],
  ["milhões", 1_000_000], ["mio", 1_000_000], ["mln", 1_000_000],
  ["млн", 1_000_000], ["مليون", 1_000_000],
  ["میلیون", 1_000_000], ["ล้าน", 1_000_000], ["jt", 1_000_000],
  ["trieu", 1_000_000], ["triệu", 1_000_000],
  ["billion", 1_000_000_000], ["billions", 1_000_000_000],
  ["bilhao", 1_000_000_000], ["bilhoes", 1_000_000_000],
  ["bilhão", 1_000_000_000], ["bilhões", 1_000_000_000],
  ["mrd", 1_000_000_000], ["млрд", 1_000_000_000],
  ["مليار", 1_000_000_000], ["میلیارد", 1_000_000_000],
  ["lakh", 100_000], ["lakhs", 100_000], ["लाख", 100_000],
  ["লাখ", 100_000], ["لاکھ", 100_000],
  ["crore", 10_000_000], ["crores", 10_000_000], ["करोड़", 10_000_000],
  ["करोड", 10_000_000], ["কোটি", 10_000_000], ["کروڑ", 10_000_000],
  ["万", 10_000], ["萬", 10_000], ["만", 10_000],
  ["亿", 100_000_000], ["億", 100_000_000], ["억", 100_000_000],
  ["k", 1_000], ["m", 1_000_000], ["b", 1_000_000_000],
  ["mi", 1_000_000], ["bi", 1_000_000_000],
];

const SUBSCRIBER_LABEL_PATTERN = new RegExp([
  "subscriber", "subscritor", "suscriptor", "inscrit", "abonn", "iscritt",
  "abonnee", "abone", "subskryb", "odberat", "tilaaj", "prenumerant",
  "feliratkoz", "abonat", "подпис", "підпис", "구독", "登録者", "订阅", "訂閱",
  "مشترك", "مشترک", "सब्सक्राइबर", "सदस्य", "সাবস্ক্রাইবার", "গ্রাহক",
  "سبسکرائبر", "ผู้ติดตาม", "người đăng ký", "pelanggan", "מנוי", "מנויים",
  "συνδρομητ", "tagasubaybay", "wanaofuatilia",
].join("|"), "iu");

function canonicalLocale(value) {
  try {
    const requested = String(value || DEFAULT_LOCALE).replaceAll("_", "-");
    return Intl.getCanonicalLocales(requested)[0] || DEFAULT_LOCALE;
  } catch {
    return "en";
  }
}

function nativeDigitValue(char) {
  const codePoint = char.codePointAt(0);
  for (const zero of DECIMAL_ZERO_CODE_POINTS) {
    const value = codePoint - zero;
    if (value >= 0 && value <= 9) return value;
  }
  return null;
}

function normalizeDigits(value, digitMap = new Map()) {
  let output = "";
  for (const char of String(value ?? "").normalize("NFKC")) {
    if (digitMap.has(char)) {
      output += digitMap.get(char);
      continue;
    }
    const numeric = nativeDigitValue(char);
    output += numeric == null ? char : String(numeric);
  }
  return output;
}

function normalizeUnitText(value) {
  return String(value ?? "")
    .normalize("NFKC")
    .replace(/[\u200b-\u200d\u2060\ufeff]/g, "")
    .replace(/[\u00a0\u202f]/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .toLowerCase();
}

function numberFromCompactParts(parts, digitMap) {
  const integer = normalizeDigits(
    parts.filter((part) => part.type === "integer").map((part) => part.value).join(""),
    digitMap,
  );
  const fraction = normalizeDigits(
    parts.filter((part) => part.type === "fraction").map((part) => part.value).join(""),
    digitMap,
  );
  const value = Number(fraction ? `${integer}.${fraction}` : integer);
  return Number.isFinite(value) && value > 0 ? value : null;
}

function addUnit(units, token, multiplier, locale, { overwrite = false } = {}) {
  const normalized = normalizeUnitText(token, locale);
  if (!normalized) return;
  const variants = new Set([normalized, normalized.replace(/[.。]+$/u, "")]);
  for (const variant of variants) {
    if (!variant) continue;
    if (overwrite || !units.has(variant)) units.set(variant, multiplier);
  }
}

function buildLocaleProfile(localeValue) {
  const locale = canonicalLocale(localeValue);
  const digitMap = new Map(Array.from({ length: 10 }, (_, value) => [String(value), String(value)]));
  const digitFormatter = new Intl.NumberFormat(locale, { useGrouping: false });
  for (let value = 0; value <= 9; value += 1) {
    for (const char of digitFormatter.format(value)) {
      if (/\p{Decimal_Number}/u.test(char)) digitMap.set(char, String(value));
    }
  }

  const decimal = new Intl.NumberFormat(locale).formatToParts(1.1)
    .find((part) => part.type === "decimal")?.value ?? ".";
  const units = new Map();
  for (const [token, multiplier] of COMMON_COMPACT_UNITS) addUnit(units, token, multiplier, locale);
  const localeUnitTokens = new Set();

  const compactFormatter = new Intl.NumberFormat(locale, {
    notation: "compact",
    compactDisplay: "short",
    maximumFractionDigits: 2,
  });
  for (const magnitude of [
    1_000, 10_000, 100_000, 1_000_000, 10_000_000, 100_000_000,
    1_000_000_000, 10_000_000_000, 100_000_000_000, 1_000_000_000_000,
  ]) {
    const parts = compactFormatter.formatToParts(magnitude);
    const token = parts.filter((part) => part.type === "compact").map((part) => part.value).join("");
    const coefficient = numberFromCompactParts(parts, digitMap);
    if (!token || !coefficient) continue;
    const normalizedToken = normalizeUnitText(token, locale).replace(/[.。]+$/u, "");
    addUnit(units, token, Math.round(magnitude / coefficient), locale, {
      overwrite: !localeUnitTokens.has(normalizedToken),
    });
    localeUnitTokens.add(normalizedToken);
  }

  return {
    locale,
    decimal,
    digitMap,
    units: [...units.entries()].sort((left, right) => right[0].length - left[0].length),
  };
}

function localeProfile(locale) {
  const canonical = canonicalLocale(locale);
  if (!profileCache.has(canonical)) profileCache.set(canonical, buildLocaleProfile(canonical));
  return profileCache.get(canonical);
}

function unitFromSuffix(suffixValue, profile) {
  const suffix = normalizeUnitText(suffixValue, profile.locale)
    .replace(/^[\s:：;；,，.。~≈≃\-–—]+/u, "");
  for (const [token, multiplier] of profile.units) {
    if (!suffix.startsWith(token)) continue;
    const next = suffix.slice(token.length, token.length + 1);
    const asciiToken = /^[a-z]+$/i.test(token);
    if (asciiToken && next && /[\p{Letter}\p{Mark}]/u.test(next)) continue;
    return { token, multiplier };
  }
  return { token: null, multiplier: 1 };
}

function unitFromPrefix(prefixValue, profile) {
  const prefix = normalizeUnitText(prefixValue, profile.locale)
    .replace(/[\s:：;；,，.。~≈≃\-–—]+$/u, "");
  for (const [token, multiplier] of profile.units) {
    if (!prefix.endsWith(token)) continue;
    const previous = prefix.slice(Math.max(0, prefix.length - token.length - 1), -token.length);
    const asciiToken = /^[a-z]+$/i.test(token);
    if (asciiToken && previous && /[\p{Letter}\p{Mark}]/u.test(previous)) continue;
    return { token, multiplier };
  }
  return { token: null, multiplier: 1 };
}

function parseNumberToken(numberToken, profile, compact) {
  const normalized = normalizeDigits(numberToken, profile.digitMap);
  if (!compact) {
    const digits = normalized.replace(/\D/g, "");
    return digits ? Number(digits) : null;
  }

  const decimalCandidates = new Set([profile.decimal, ".", ",", "٫"]);
  let decimalIndex = -1;
  let decimalSymbol = null;
  for (const symbol of decimalCandidates) {
    const index = normalized.lastIndexOf(symbol);
    if (index > decimalIndex) {
      decimalIndex = index;
      decimalSymbol = symbol;
    }
  }
  let output = "";
  for (let index = 0; index < normalized.length; index += 1) {
    const char = normalized[index];
    if (/\d/.test(char)) output += char;
    else if (decimalSymbol && char === decimalSymbol && index === decimalIndex) output += ".";
  }
  const value = Number(output);
  return Number.isFinite(value) ? value : null;
}

export function parseLocalizedCountDetails(value, { locale = DEFAULT_LOCALE } = {}) {
  if (typeof value === "number" && Number.isFinite(value)) {
    return { value: Math.round(value), multiplier: 1, unit: null, locale: canonicalLocale(locale) };
  }
  if (value === null || value === undefined || value === "") return null;
  const raw = String(value).normalize("NFKC");
  const match = raw.match(/[\p{Decimal_Number}]+(?:[\s\u00a0\u202f.,，．٫٬'’_]*[\p{Decimal_Number}]+)*/u);
  if (!match) return null;
  const profile = localeProfile(locale);
  const prefix = raw.slice(0, match.index ?? 0);
  const suffix = raw.slice((match.index ?? 0) + match[0].length);
  const suffixUnit = unitFromSuffix(suffix, profile);
  const unit = suffixUnit.multiplier > 1 ? suffixUnit : unitFromPrefix(prefix, profile);
  const number = parseNumberToken(match[0], profile, unit.multiplier > 1);
  if (!Number.isFinite(number)) return null;
  const count = Math.round(number * unit.multiplier);
  if (!Number.isSafeInteger(count) || count < 0) return null;
  return {
    value: count,
    multiplier: unit.multiplier,
    unit: unit.token,
    locale: profile.locale,
  };
}

export function parseLocalizedCount(value, options = {}) {
  return parseLocalizedCountDetails(value, options)?.value ?? null;
}

export function parseRequiredLocalizedCount(value, {
  locale = DEFAULT_LOCALE,
  field = "localized_count",
  source = null,
  context = null,
} = {}) {
  const details = parseLocalizedCountDetails(value, { locale });
  if (details) return details.value;
  throw new ParserContractError({
    field,
    value,
    locale: canonicalLocale(locale),
    source,
    reason: "unsupported_localized_count",
    context,
  });
}

export function looksLikeSubscriberCountText(value) {
  return SUBSCRIBER_LABEL_PATTERN.test(String(value ?? "").normalize("NFKC"));
}

export function findSubscriberCountText(values, { locale = DEFAULT_LOCALE } = {}) {
  const texts = (Array.isArray(values) ? values : [values])
    .map((value) => String(value ?? "").trim())
    .filter((value) => value && !value.startsWith("@") && !/(?:^|\/)youtube\.com\/@/i.test(value));
  const labelled = texts.find(looksLikeSubscriberCountText);
  if (labelled) return labelled;
  const compact = texts.filter((text) => {
    const parsed = parseLocalizedCountDetails(text, { locale });
    return parsed?.multiplier > 1;
  });
  return compact.length === 1 ? compact[0] : null;
}
