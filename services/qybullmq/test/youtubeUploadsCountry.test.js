import assert from "node:assert/strict";
import test from "node:test";
import { collectYoutubeJsUploadBundle, scanYoutubeJsFeed } from "../src/youtubeJs.js";
import { emptyUploadsDecision, prepareDormantUploadsProbe, withUploadsCountryExecution } from "../src/youtubeUploadsCountry.js";

const emptyClient = { getPlaylist: async () => ({ videos: [], has_continuation: false }) };

test("empty Full Crawl uploads request a country handoff, not a network retry", async () => {
  await assert.rejects(withUploadsCountryExecution({ egressCountry: "US" }, () =>
    collectYoutubeJsUploadBundle(emptyClient, "UCtest", 30, { country: "BR" })),
  { code: "UPLOADS_COUNTRY_RECHECK", country: "BR" });
});

for (const [context, country, reason] of [
  [{ egressCountry: "US" }, null, "no_country"],
  [{ egressCountry: "BR" }, "BR", "country_checked"],
  [{ egressCountry: "US", recheck: { country: "BR", status: "unavailable" } }, "BR", "no_country_reserve"],
  [{ egressCountry: "US", recheck: { country: "BR", status: "requested" } }, "BR", "country_recheck_exhausted"],
]) {
  test(`normal empty uploads settle dormant: ${reason}`, async () => {
    const result = await withUploadsCountryExecution(context, () =>
      collectYoutubeJsUploadBundle(emptyClient, "UCtest", 30, { country }));
    assert.equal(result.uploads.empty_uploads.reason, reason);
    assert.equal(result.uploads.empty_uploads.country, country);
    assert.equal(result.uploads.complete, true);
  });
}

test("an invalid item or a failed continuation never becomes normal empty uploads", async () => {
  const malformed = { getPlaylist: async () => ({ videos: [{ type: "Video", title: "missing id" }], has_continuation: false }) };
  await assert.rejects(collectYoutubeJsUploadBundle(malformed, "UCtest", 30), /incomplete empty-list evidence/);
  const error = new Error("network timeout");
  const failed = { getPlaylist: async () => { throw error; } };
  await assert.rejects(collectYoutubeJsUploadBundle(failed, "UCtest", 30), error);
  const scan = await scanYoutubeJsFeed({ videos: [], has_continuation: true, getContinuation: async () => { throw error; } });
  assert.equal(scan.complete, false);
  assert.equal(scan.stop_reason, "pagination_error");
});

test("dormant country probes request a route before scanning, or stay dormant without reserve", () => {
  assert.throws(() => withUploadsCountryExecution({ egressCountry: "US" }, () => prepareDormantUploadsProbe("BR")), { code: "UPLOADS_COUNTRY_RECHECK" });
  assert.equal(withUploadsCountryExecution({ egressCountry: "BR" }, () => prepareDormantUploadsProbe("BR")), null);
  const decision = withUploadsCountryExecution({ egressCountry: "US", recheck: { country: "BR", status: "unavailable" } }, () => prepareDormantUploadsProbe("BR"));
  assert.equal(decision.outcome, "dormant");
});

test("nonempty uploads preserve Full Crawl truncation even on another country", async () => {
  const client = { getPlaylist: async () => ({ videos: [{ id: "one" }, { id: "two" }], has_continuation: true }) };
  const result = await withUploadsCountryExecution({ egressCountry: "US" }, () => collectYoutubeJsUploadBundle(client, "UCtest", 1, { country: "BR" }));
  assert.equal(result.entries.length, 1);
  assert.equal(result.uploads.stop_reason, "max_items");
  assert.equal(result.uploads.empty_uploads, undefined);
});

test("HTTP 200 without video nodes needs an explicit empty-playlist message", async () => {
  const { uploadsResponseEvidence, assertNormalEmptyUploadsResponse } = await import("../src/youtubeUploadsCountry.js");
  const feed = { videos: [], has_continuation: false };
  const raw = { contents: { messageRenderer: { text: { runs: [{ text: "No videos in this playlist yet" }] } } } };
  assert.doesNotThrow(() => assertNormalEmptyUploadsResponse(feed, uploadsResponseEvidence(raw)));
  assert.throws(() => assertNormalEmptyUploadsResponse(feed, uploadsResponseEvidence({})), { code: "UPLOADS_EMPTY_RESPONSE_UNVERIFIED" });
  assert.throws(() => assertNormalEmptyUploadsResponse(feed, uploadsResponseEvidence({ ...raw, unknownRenderer: { videoId: "lost-video" } })), { code: "UPLOADS_EMPTY_RESPONSE_UNVERIFIED" });
});

test("a no-country-reserve receipt finishes Full Crawl without another request on the old IP", async () => {
  const client = { getPlaylist: async () => assert.fail("must not retry old-country HTTP after no reserve") };
  const result = await withUploadsCountryExecution({ egressCountry: "US", recheck: { country: "BR", status: "unavailable" } }, () =>
    collectYoutubeJsUploadBundle(client, "UCtest", 30, { country: "BR" }));
  assert.equal(result.uploads.empty_uploads.reason, "no_country_reserve");
  assert.equal(result.uploads.pages, 0);
});
