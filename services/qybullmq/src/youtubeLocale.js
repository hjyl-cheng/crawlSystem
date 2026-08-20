import { ParserContractError } from "./localizedParsing.js";
import {
  YOUTUBE_UI_LANGUAGE_ALIASES,
  YOUTUBE_UI_LANGUAGE_CODES,
} from "./youtubeLanguages.js";

const supportedLanguages = new Map(
  YOUTUBE_UI_LANGUAGE_CODES.map((code) => [code.toLowerCase(), code]),
);

function canonicalLanguage(value) {
  const candidate = String(value ?? "").trim().replaceAll("_", "-");
  if (!candidate) return null;
  const direct = supportedLanguages.get(candidate.toLowerCase());
  if (direct) return direct;
  try {
    const canonical = Intl.getCanonicalLocales(candidate)[0] ?? null;
    const alias = YOUTUBE_UI_LANGUAGE_ALIASES[canonical];
    if (alias) return alias;
  } catch {
    // The stable parser error below also covers malformed BCP 47 input.
  }
  throw new ParserContractError({
    field: "youtube_language",
    value,
    source: "query_configuration",
    reason: "unsupported_youtube_language",
    context: { supported_count: YOUTUBE_UI_LANGUAGE_CODES.length },
  });
}

function canonicalCountry(value) {
  const candidate = String(value ?? "").trim().toUpperCase();
  if (!candidate) return null;
  if (!/^[A-Z]{2}$/.test(candidate)) {
    throw new ParserContractError({
      field: "youtube_country",
      value,
      source: "query_configuration",
      reason: "invalid_youtube_country",
    });
  }
  return candidate;
}

export function resolveYoutubeLocale(values = {}, defaults = {}) {
  const language = canonicalLanguage(values.language)
    ?? canonicalLanguage(defaults.language)
    ?? "en";
  const country = canonicalCountry(values.country)
    ?? canonicalCountry(defaults.country)
    ?? "US";
  return { language, country };
}
