import assert from "node:assert/strict";
import test from "node:test";
import {
  normalizeAboutCurrentIdentity,
  normalizeAboutObservationCurrent,
} from "../src/aboutCurrent.js";

function identity(values = {}) {
  return normalizeAboutCurrentIdentity({
    title: "Channel",
    handle: "@channel",
    avatar_url: "https://example.test/avatar.jpg",
    keywords: ["Build", "Code"],
    available_tabs: ["videos", "shorts"],
    description: "Summary",
    ...values,
  });
}

test("About Current identity normalization is stable across list order and duplicates", () => {
  assert.deepEqual(
    identity({ keywords: ["Code", "Build", "Code"] }).keywords,
    ["Build", "Code"],
  );
  assert.deepEqual(
    identity({ available_tabs: ["shorts", "videos"] }).available_tabs,
    ["shorts", "videos"],
  );
});

test("About Current identity splits YouTube's quoted keyword string", () => {
  assert.deepEqual(
    identity({
      keywords: '"musica infantil" "canciones infantiles" kids "musica infantil"',
    }).keywords,
    ["canciones infantiles", "kids", "musica infantil"],
  );
});

test("About observation normalization produces persistable Channel fields", () => {
  const current = normalizeAboutObservationCurrent({
    title: "Channel",
    avatar_url: "https://yt3.googleusercontent.com/avatar?token=temporary",
    keywords: null,
    available_tabs: [],
    description: "Description",
    joined_date_text: "Joined Apr 2, 2020",
    external_links: [{
      title: "Website",
      display_url: "example.com",
      target_url: "https://example.com/?utm_source=youtube",
    }],
    is_verified: false,
    is_verified_status: "not_verified",
    youtube_business_email_available: true,
    youtube_business_email_status: "available",
    is_family_safe: true,
    rss_url: "https://www.youtube.com/feeds/videos.xml?channel_id=UCcurrent",
  }, { aboutObserved: true });

  assert.equal(current.joinedAt, "2020-04-02");
  assert.equal(current.joinedAtPrecision, "date_only");
  assert.equal(current.externalLinksStatus, "observed");
  assert.equal(current.externalLinks[0].target_url, "https://example.com/");
  assert.equal(current.isVerified, false);
  assert.equal(current.isVerifiedStatus, "not_verified");
  assert.equal(current.youtubeBusinessEmailAvailable, true);
  assert.equal(current.youtubeBusinessEmailStatus, "available");
  assert.equal(current.keywordsStatus, "observed");
  assert.equal(current.availableTabsStatus, "observed");
});

test("a failed About request cannot turn an unobserved Link surface into empty", () => {
  const current = normalizeAboutObservationCurrent({
    external_links: [],
    available_tabs: ["videos"],
  }, { aboutObserved: false });
  assert.equal(current.externalLinks, null);
  assert.equal(current.externalLinksStatus, "unresolved");
  assert.equal(current.normalization.links_ready, false);
  assert.equal(current.youtubeBusinessEmailAvailable, null);
  assert.equal(current.youtubeBusinessEmailStatus, "unknown");
});

test("a successful About observation can explicitly remove a previous business email entry", () => {
  const current = normalizeAboutObservationCurrent({
    youtube_business_email_available: false,
    youtube_business_email_status: "not_available",
  }, { aboutObserved: true });

  assert.equal(current.youtubeBusinessEmailAvailable, false);
  assert.equal(current.youtubeBusinessEmailStatus, "not_available");
});

test("a successful About response confirms an empty initial Link collection", () => {
  const observed = normalizeAboutObservationCurrent({
    external_links: [],
  }, { aboutObserved: true });
  const unknown = normalizeAboutObservationCurrent({
    external_links: [],
  }, { aboutObserved: false });

  assert.deepEqual(observed.externalLinks, []);
  assert.equal(observed.externalLinksStatus, "observed");
  assert.equal(unknown.externalLinks, null);
  assert.equal(unknown.externalLinksStatus, "unresolved");
});
