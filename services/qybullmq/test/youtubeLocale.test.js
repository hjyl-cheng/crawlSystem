import assert from "node:assert/strict";
import test from "node:test";

import { resolveYoutubeLocale } from "../src/youtubeLocale.js";
import { isParserContractError } from "../src/localizedParsing.js";
import { YOUTUBE_UI_LANGUAGE_CODES } from "../src/youtubeLanguages.js";

test("job locale overrides process defaults and is canonicalized", () => {
  assert.deepEqual(
    resolveYoutubeLocale(
      { language: "ko", country: "kr" },
      { language: "pt-BR", country: "BR" },
    ),
    { language: "ko", country: "KR" },
  );
});

test("the parser matrix follows YouTube's 83 current display locales", () => {
  assert.equal(YOUTUBE_UI_LANGUAGE_CODES.length, 83);
  assert.equal(new Set(YOUTUBE_UI_LANGUAGE_CODES).size, 83);
  assert.equal(resolveYoutubeLocale({ language: "pt-BR", country: "BR" }).language, "pt");
  assert.equal(resolveYoutubeLocale({ language: "he", country: "IL" }).language, "iw");
  for (const language of YOUTUBE_UI_LANGUAGE_CODES) {
    assert.equal(resolveYoutubeLocale({ language, country: "US" }).language, language);
  }
});

test("invalid YouTube locales fail before making a mismatched request", () => {
  assert.throws(
    () => resolveYoutubeLocale(
      { language: "not_a_locale", country: "BRA" },
      { language: "pt-BR", country: "BR" },
    ),
    (error) => isParserContractError(error),
  );
});
