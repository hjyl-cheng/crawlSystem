import { ParserContractError } from "./localizedParsing.js";
import { YOUTUBE_UI_LANGUAGE_CODES } from "./youtubeLanguages.js";

const COUNTRY_ALIASES = Object.freeze({
  "bolivia (plurinational state of)": "BO",
  "brunei darussalam": "BN",
  "cabo verde": "CV",
  "czech republic": "CZ",
  "democratic republic of the congo": "CD",
  "iran (islamic republic of)": "IR",
  "ivory coast": "CI",
  "lao people's democratic republic": "LA",
  "micronesia (federated states of)": "FM",
  "moldova (the republic of)": "MD",
  "republic of korea": "KR",
  "republic of the congo": "CG",
  "russian federation": "RU",
  "south korea": "KR",
  "syrian arab republic": "SY",
  "taiwan, province of china": "TW",
  "tanzania, united republic of": "TZ",
  "the netherlands": "NL",
  "turkiye": "TR",
  "türkiye": "TR",
  "united kingdom of great britain and northern ireland": "GB",
  "united states of america": "US",
  "venezuela (bolivarian republic of)": "VE",
  "viet nam": "VN",
});

let countryProfile = null;

function normalizedLabel(value, { fold = false } = {}) {
  let output = String(value ?? "")
    .normalize("NFKC")
    .replace(/[\u200b-\u200f\u202a-\u202e\u2060-\u206f\ufeff]/g, "")
    .replace(/[\u2018\u2019\u02bc]/g, "'")
    .replace(/\s+/g, " ")
    .trim()
    .toLowerCase();
  if (fold) {
    output = output.normalize("NFKD").replace(/\p{Mark}+/gu, "").normalize("NFC");
  }
  return output;
}

function addCountryLabel(index, label, code) {
  const key = normalizedLabel(label);
  if (!key) return;
  if (!index.has(key)) index.set(key, new Set());
  index.get(key).add(code);
}

function canonicalRegionCode(code) {
  try {
    const region = new Intl.Locale(`und-${code}`).region;
    return /^[A-Z]{2}$/.test(region ?? "") ? region : code;
  } catch {
    return code;
  }
}

function buildCountryProfile() {
  const english = new Intl.DisplayNames(["en"], { type: "region" });
  const codes = [];
  const codeAliases = new Map();
  for (let first = 65; first <= 90; first += 1) {
    for (let second = 65; second <= 90; second += 1) {
      const code = String.fromCharCode(first, second);
      const name = english.of(code);
      if (!name || name === code) continue;
      const canonicalCode = canonicalRegionCode(code);
      if (canonicalCode !== code) {
        codeAliases.set(code, canonicalCode);
        continue;
      }
      codes.push(code);
    }
  }
  const names = new Map(codes.map((code) => [code, english.of(code)]));
  const exact = new Map();
  const folded = new Map();
  for (const [code, name] of names) {
    addCountryLabel(exact, name, code);
    addCountryLabel(folded, normalizedLabel(name, { fold: true }), code);
  }
  for (const [label, code] of Object.entries(COUNTRY_ALIASES)) {
    addCountryLabel(exact, label, code);
    addCountryLabel(folded, normalizedLabel(label, { fold: true }), code);
  }
  return { codes: new Set(codes), codeAliases, names, exact, folded, localized: false };
}

function profile() {
  if (!countryProfile) countryProfile = buildCountryProfile();
  return countryProfile;
}

function uniqueCode(index, key) {
  const matches = index.get(key);
  return matches?.size === 1 ? [...matches][0] : null;
}

function addLocalizedCountryLabels(current) {
  if (current.localized) return;
  for (const locale of YOUTUBE_UI_LANGUAGE_CODES) {
    const displayNames = new Intl.DisplayNames([locale], { type: "region" });
    for (const code of current.codes) {
      const name = displayNames.of(code);
      if (!name || name === code) continue;
      addCountryLabel(current.exact, name, code);
      addCountryLabel(current.folded, normalizedLabel(name, { fold: true }), code);
    }
  }
  current.localized = true;
}

export function canonicalizeCrawlerCountry(value) {
  const raw = String(value ?? "").trim();
  if (!raw) return { raw: null, code: null, name: null };
  const current = profile();
  const upper = raw.toUpperCase();
  const codeAlias = current.codeAliases.get(upper);
  let code = current.codes.has(upper)
    ? upper
    : codeAlias && current.codes.has(codeAlias)
      ? codeAlias
    : uniqueCode(current.exact, normalizedLabel(raw))
      ?? uniqueCode(current.folded, normalizedLabel(raw, { fold: true }));
  if (!code) {
    addLocalizedCountryLabels(current);
    code = uniqueCode(current.exact, normalizedLabel(raw))
      ?? uniqueCode(current.folded, normalizedLabel(raw, { fold: true }));
  }
  return {
    raw,
    code: code ?? null,
    name: code ? current.names.get(code) ?? raw : null,
  };
}

export function normalizeCrawlerCountry(value) {
  const country = String(value ?? "").trim();
  return country || null;
}

export function parseYoutubeAboutCountry(value, context = null) {
  const country = canonicalizeCrawlerCountry(value);
  if (!country.raw || country.code) return country;
  throw new ParserContractError({
    field: "country",
    value: country.raw,
    source: "youtube_about",
    reason: "unsupported_localized_country",
    context,
  });
}

export function hasResolvedCrawlerCountry(channel) {
  const storedCode = String(channel?.country_code ?? "").trim().toUpperCase();
  const countryCode = /^[A-Z]{2}$/.test(storedCode)
    ? storedCode
    : canonicalizeCrawlerCountry(channel?.country).code;
  return countryCode !== null
    && String(channel?.country_source ?? "").trim() === "youtube_about";
}

export function countryRequiresAgent(channel) {
  return !hasResolvedCrawlerCountry(channel);
}
