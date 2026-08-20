import assert from "node:assert/strict";
import test from "node:test";

import {
  buildYoutubeSearchUrl,
  VIDEO_POPULARITY_THIS_YEAR_FILTER_PARAM,
} from "../src/youtube.js";

test("discover search filter is video popularity plus this year", () => {
  const url = new URL(buildYoutubeSearchUrl(
    "crianças",
    "pt-BR",
    "BR",
    VIDEO_POPULARITY_THIS_YEAR_FILTER_PARAM,
  ));

  assert.equal(url.searchParams.get("search_query"), "crianças");
  assert.equal(url.searchParams.get("hl"), "pt-BR");
  assert.equal(url.searchParams.get("gl"), "BR");
  assert.equal(url.searchParams.get("sp"), "CAMSBAgFEAE=");
});
