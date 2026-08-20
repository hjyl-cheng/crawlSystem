import assert from "node:assert/strict";
import test from "node:test";

import {
  localizedPublishedUtcDay,
  parseLocalizedAgeDays,
  parseRequiredLocalizedAgeDays,
} from "../src/localizedTime.js";
import { isParserContractError } from "../src/localizedParsing.js";
import { YOUTUBE_UI_LANGUAGE_CODES } from "../src/youtubeLanguages.js";

test("localized relative ages round-trip across YouTube locales", () => {
  const cases = [
    ["hour", 3, 0],
    ["day", 2, 2],
    ["week", 3, 21],
    ["month", 5, 150],
    ["year", 2, 730],
  ];
  for (const locale of YOUTUBE_UI_LANGUAGE_CODES) {
    const formatter = new Intl.RelativeTimeFormat(locale, { numeric: "always" });
    for (const [unit, amount, expected] of cases) {
      const text = formatter.format(-amount, unit);
      assert.equal(parseLocalizedAgeDays(text, { locale }), expected, `${locale}: ${text}`);
    }
  }
});

test("localized special relative dates and YouTube prefixes are parsed", () => {
  assert.equal(parseLocalizedAgeDays("Streamed yesterday", { locale: "en" }), 1);
  assert.equal(parseLocalizedAgeDays("Transmitido há 3 dias", { locale: "pt-BR" }), 3);
  assert.equal(parseLocalizedAgeDays("2日前に配信", { locale: "ja" }), 2);
  assert.equal(parseLocalizedAgeDays("قبل يومين", { locale: "ar" }), 2);
});

test("localized publication text resolves only to a UTC calendar day", () => {
  const now = Date.parse("2026-07-21T23:30:00.000Z");
  assert.equal(localizedPublishedUtcDay("2 days ago", { locale: "en", now }), "2026-07-19");
  assert.equal(localizedPublishedUtcDay("2026-07-10", { locale: "en", now }), "2026-07-10");
  assert.equal(localizedPublishedUtcDay("2026-02-30", { locale: "en", now }), null);
  assert.equal(localizedPublishedUtcDay("unknown", { locale: "en", now }), null);
});

test("unsupported nonempty publication text is a parser contract error", () => {
  assert.throws(
    () => parseRequiredLocalizedAgeDays("未知の新しい日時形式", {
      locale: "ja",
      field: "published_age",
      source: "youtube_search",
    }),
    (error) => isParserContractError(error),
  );
});
