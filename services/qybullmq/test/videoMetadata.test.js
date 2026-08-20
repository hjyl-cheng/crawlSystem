import assert from "node:assert/strict";
import test from "node:test";

import {
  descriptionState,
  extractVideoHashtags,
  hasResolvedDescription,
  normalizeVideoKeywords,
  normalizeVideoTextMetadata,
} from "../src/videoMetadata.js";

test("extractVideoHashtags reads title and description with Unicode support", () => {
  assert.deepEqual(
    extractVideoHashtags(
      "Short #Roblox #\u8bdd\u9898",
      "More #roblox #maquillaje_2026 #\ud55c\uae00",
    ),
    ["#Roblox", "#\u8bdd\u9898", "#maquillaje_2026", "#\ud55c\uae00"],
  );
});

test("extractVideoHashtags preserves Unicode join controls", () => {
  assert.deepEqual(
    extractVideoHashtags("#می‌خواهم #क्‍षेत्र"),
    ["#می‌خواهم", "#क्‍षेत्र"],
  );
});

test("descriptionState distinguishes a legitimate empty description", () => {
  assert.deepEqual(descriptionState("", { source: "youtubejs_player" }), {
    description: "",
    description_status: "empty",
    description_source: "youtubejs_player",
  });
  assert.equal(hasResolvedDescription({ description_status: "empty" }), true);
  assert.equal(hasResolvedDescription({ description_status: "unresolved" }), false);
});

test("normalizeVideoTextMetadata keeps keywords separate from visible hashtags", () => {
  const result = normalizeVideoTextMetadata({
    title: "Example #PublicTag",
    description: "Body #SecondTag",
    description_source: "yt_dlp",
    keywords: ["Creator Keyword", "creator keyword"],
    tags: ["Hidden Tag"],
  });
  assert.equal(result.description_status, "exact");
  assert.deepEqual(result.hashtags, ["#PublicTag", "#SecondTag"]);
  assert.equal(result.hashtags_observed, true);
  assert.deepEqual(result.keywords, ["Creator Keyword", "Hidden Tag"]);
  assert.equal(result.keywords_observed, true);
});

test("resolved empty metadata arrays remain distinguishable from unavailable fields", () => {
  const observedEmpty = normalizeVideoTextMetadata({
    title: "No tags",
    description: "",
    keywords: [],
    keywords_observed: true,
  });
  assert.deepEqual(observedEmpty.hashtags, []);
  assert.equal(observedEmpty.hashtags_observed, true);
  assert.deepEqual(observedEmpty.keywords, []);
  assert.equal(observedEmpty.keywords_observed, true);

  const unavailable = normalizeVideoTextMetadata({ title: "No detail response" });
  assert.deepEqual(unavailable.hashtags, []);
  assert.equal(unavailable.hashtags_observed, false);
  assert.deepEqual(unavailable.keywords, []);
  assert.equal(unavailable.keywords_observed, false);
});

test("missing description becomes unavailable only after the terminal API pass", () => {
  assert.equal(normalizeVideoTextMetadata({}).description_status, "unresolved");
  assert.equal(normalizeVideoTextMetadata({}, { afterApi: true }).description_status, "unavailable");
});

test("normalizeVideoKeywords preserves source order and removes case duplicates", () => {
  assert.deepEqual(
    normalizeVideoKeywords(["Roblox", "Brookhaven"], ["roblox", "Shorts"]),
    ["Roblox", "Brookhaven", "Shorts"],
  );
});
