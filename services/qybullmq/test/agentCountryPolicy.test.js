import assert from "node:assert/strict";
import test from "node:test";
import {
  canonicalizeCrawlerCountry,
  countryRequiresAgent,
  hasResolvedCrawlerCountry,
  normalizeCrawlerCountry,
  parseYoutubeAboutCountry,
} from "../src/agentCountryPolicy.js";
import { isParserContractError } from "../src/localizedParsing.js";
import { YOUTUBE_UI_LANGUAGE_CODES } from "../src/youtubeLanguages.js";

test("crawler country normalization treats blank text as missing", () => {
  assert.equal(normalizeCrawlerCountry(null), null);
  assert.equal(normalizeCrawlerCountry(""), null);
  assert.equal(normalizeCrawlerCountry("   "), null);
  assert.equal(normalizeCrawlerCountry(" Brazil "), "Brazil");
  assert.deepEqual(canonicalizeCrawlerCountry("Estados Unidos"), {
    raw: "Estados Unidos",
    code: "US",
    name: "United States",
  });
  assert.equal(normalizeCrawlerCountry("Brasil"), "Brasil");
  assert.equal(normalizeCrawlerCountry("México"), "México");
});

test("YouTube locale country labels round-trip to one canonical country", () => {
  const expectedNames = new Intl.DisplayNames(["en"], { type: "region" });
  for (const locale of YOUTUBE_UI_LANGUAGE_CODES) {
    const localizedNames = new Intl.DisplayNames([locale], { type: "region" });
    for (const code of ["BR", "US", "MX", "KR", "IN", "SA"]) {
      const localized = localizedNames.of(code);
      const parsed = canonicalizeCrawlerCountry(localized);
      assert.equal(parsed.code, code, `${locale}: ${localized}`);
      assert.equal(parsed.name, expectedNames.of(code), `${locale}: ${localized}`);
    }
  }
});

test("deprecated region aliases do not make canonical country names ambiguous", () => {
  assert.deepEqual(canonicalizeCrawlerCountry("United Kingdom"), {
    raw: "United Kingdom",
    code: "GB",
    name: "United Kingdom",
  });
  assert.deepEqual(canonicalizeCrawlerCountry("Germany"), {
    raw: "Germany",
    code: "DE",
    name: "Germany",
  });
  assert.equal(canonicalizeCrawlerCountry("UK").code, "GB");
  assert.equal(canonicalizeCrawlerCountry("DD").code, "DE");
});

test("an observed but unknown About country fails instead of bypassing Agent", () => {
  assert.throws(
    () => parseYoutubeAboutCountry("新的未知国家标签", { channel_id: "UCglobal" }),
    (error) => isParserContractError(error) && error.field === "country",
  );
});

test("only a nonblank YouTube About country bypasses Agent country lookup", () => {
  assert.equal(hasResolvedCrawlerCountry({ country: "Brazil", country_source: "youtube_about" }), true);
  assert.equal(countryRequiresAgent({ country: "Brazil", country_source: "youtube_about" }), false);
  assert.equal(countryRequiresAgent({
    country: "原始国家文本",
    country_code: "BR",
    country_source: "youtube_about",
  }), false);
  assert.equal(countryRequiresAgent({ country: "", country_source: "youtube_about" }), true);
  assert.equal(countryRequiresAgent({ country: null, country_source: "youtube_about" }), true);
  assert.equal(countryRequiresAgent({ country: "Brazil", country_source: "agent" }), true);
});
