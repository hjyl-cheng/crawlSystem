import assert from "node:assert/strict";
import test from "node:test";
import {
  combinedAboutObservationMetrics,
  normalizeAboutMetrics,
} from "../src/aboutMetrics.js";
import { recordAboutObservation } from "../src/aboutObservationStore.js";

function clientFixture() {
  const calls = [];
  return {
    calls,
    client: {
      async query(sql, params = []) {
        calls.push({ sql: String(sql), params });
        if (String(sql).includes("INSERT INTO crawler.crawl_observation_keys")) {
          return { rowCount: 1, rows: [{ observation_id: params[1] }] };
        }
        if (String(sql).includes("FROM crawler.channel_domain_cursors")
            && String(sql).includes("FOR UPDATE")) {
          return { rowCount: 1, rows: [{ latest_sequence: 0 }] };
        }
        return { rowCount: 1, rows: [] };
      },
    },
  };
}

function command(about, current = {}) {
  return {
    idempotencyKey: "about:run:test:attempt:1",
    channelId: "UCaboutWriter",
    runId: null,
    observedAt: "2026-07-25T02:03:04.000Z",
    triggerReason: "manual",
    crawlerVersion: "test",
    about,
    current: {
      aboutDescription: "About description",
      identity: {
        title: "Current title",
        handle: "@current",
        avatar_url: "https://example.test/avatar.jpg",
        keywords: ["z", "a", "z"],
        available_tabs: ["videos", "shorts"],
        summary: "Current summary",
      },
      ...current,
    },
  };
}

test("About writer updates identity Current and snapshots only the three About metrics", async () => {
  const fixture = clientFixture();
  const about = normalizeAboutMetrics({
    aboutObserved: true,
    locale: "en",
    metadata: {
      subscriber_count_text: "123 subscribers",
      subscriber_count_source: "youtube_about",
      view_count_text: "4,567 views",
      view_count_source: "youtube_about",
      video_count_text: "89 videos",
      video_count_source: "youtube_about",
    },
  });

  const result = await recordAboutObservation(fixture.client, command(about));

  const identityUpdate = fixture.calls.find((call) => (
    call.sql.includes("about_identity_last_observed_at=$2")
  ));
  assert.ok(identityUpdate);
  assert.deepEqual(identityUpdate.params.slice(2, 8), [
    "Current title",
    "@current",
    "https://example.test/avatar.jpg",
    ["a", "z"],
    ["shorts", "videos"],
    "Current summary",
  ]);
  const snapshots = fixture.calls.filter((call) => (
    call.sql.includes("INSERT INTO crawler.channel_about_metric_snapshots")
  ));
  assert.equal(snapshots.length, 1);
  assert.deepEqual(snapshots[0].params.slice(3, 6), [123, 4567, 89]);
  assert.equal(result.snapshot_written, true);
  const crawlerOutbox = fixture.calls.findIndex((call) => (
    call.sql.includes("INSERT INTO crawler.crawler_outbox")
  ));
  const publication = fixture.calls.findIndex((call) => (
    call.sql.includes("publication-reconciler:find-owner")
  ));
  assert.ok(crawlerOutbox >= 0 && publication > crawlerOutbox);
});

test("combined About Partial updates identity Current without writing a metric snapshot", async () => {
  const fixture = clientFixture();
  const about = combinedAboutObservationMetrics(normalizeAboutMetrics({
    aboutObserved: false,
    metadata: {
      subscriber_count_text: "1.2K subscribers",
      subscriber_count_source: "youtube_channel_header",
    },
  }));

  const result = await recordAboutObservation(fixture.client, command(about));

  assert.equal(result.outcome, "partial");
  assert.equal(result.snapshot_written, false);
  assert.equal(fixture.calls.some((call) => (
    call.sql.includes("about_identity_last_observed_at=$2")
  )), true);
  assert.equal(fixture.calls.some((call) => (
    call.sql.includes("INSERT INTO crawler.channel_about_metric_snapshots")
  )), false);
});

test("About writer carries explicit business email availability into the Current update", async () => {
  const fixture = clientFixture();
  const about = normalizeAboutMetrics({
    aboutObserved: true,
    metadata: {
      subscriber_count_text: "123 subscribers",
      subscriber_count_source: "youtube_about",
      view_count_text: "4,567 views",
      view_count_source: "youtube_about",
      video_count_text: "89 videos",
      video_count_source: "youtube_about",
    },
  });

  await recordAboutObservation(fixture.client, command(about, {
    youtubeBusinessEmailAvailable: false,
    youtubeBusinessEmailStatus: "not_available",
  }));

  const currentUpdate = fixture.calls.find((call) => (
    call.sql.includes("youtube_business_email_available=CASE")
  ));
  assert.ok(currentUpdate);
  assert.equal(currentUpdate.params[23], false);
  assert.equal(currentUpdate.params[24], "not_available");
});

test("About writer rejects mismatched business email evidence", async () => {
  const fixture = clientFixture();
  const about = normalizeAboutMetrics({ aboutObserved: false });
  await assert.rejects(
    recordAboutObservation(fixture.client, command(about, {
      youtubeBusinessEmailAvailable: false,
      youtubeBusinessEmailStatus: "available",
    })),
    /youtubeBusinessEmailAvailable.*disagree/,
  );
});

test("About writer persists only normalized observed Channel fields", async () => {
  const fixture = clientFixture();
  const about = normalizeAboutMetrics({
    aboutObserved: true,
    metadata: {
      subscriber_count_text: "123 subscribers",
      subscriber_count_source: "youtube_about",
      view_count_text: "4,567 views",
      view_count_source: "youtube_about",
      video_count_text: "89 videos",
      video_count_source: "youtube_about",
    },
  });
  await recordAboutObservation(fixture.client, command(about, {
    descriptionStatus: "exact",
    joinedDateText: "Joined Jan 2, 2020",
    joinedAt: "2020-01-02",
    joinedAtPrecision: "date_only",
    externalLinks: [{
      title: "Website",
      target_url: "https://example.test/?utm_source=youtube",
    }],
    externalLinksStatus: "observed",
    rssUrl: "https://www.youtube.com/feeds/videos.xml?channel_id=UCaboutWriter",
    vanityChannelUrl: "https://www.youtube.com/@current",
    isFamilySafe: true,
    isVerified: false,
    isVerifiedStatus: "not_verified",
    keywordsStatus: "observed",
    availableTabsStatus: "observed",
  }));

  const aboutUpdate = fixture.calls.find((call) => call.sql.includes("description_status=CASE"));
  assert.ok(aboutUpdate);
  assert.equal(aboutUpdate.params[17], "2020-01-02");
  assert.deepEqual(JSON.parse(aboutUpdate.params[19]), [{
    title: "Website",
    display_url: null,
    target_url: "https://example.test/",
    favicon_url: null,
    position: 0,
    link_type: "website",
    purpose: "public_reference",
  }]);
  assert.equal(aboutUpdate.params[20], "observed");

  const identityUpdate = fixture.calls.find((call) => call.sql.includes("is_verified_status=CASE"));
  assert.ok(identityUpdate);
  assert.deepEqual(identityUpdate.params.slice(8, 15), [
    "https://www.youtube.com/feeds/videos.xml?channel_id=UCaboutWriter",
    "https://www.youtube.com/@current",
    true,
    false,
    "not_verified",
    "observed",
    "observed",
  ]);
});

test("an invalid observed Link set cannot overwrite the previous trusted Current", async () => {
  const fixture = clientFixture();
  const about = combinedAboutObservationMetrics(normalizeAboutMetrics({
    aboutObserved: false,
    metadata: {},
  }));
  await recordAboutObservation(fixture.client, command(about, {
    externalLinks: [{ title: "Display only", display_url: "example.test" }],
    externalLinksStatus: "observed",
  }));
  const aboutUpdate = fixture.calls.find((call) => call.sql.includes("external_links_status=CASE"));
  assert.equal(aboutUpdate.params[19], null);
  assert.equal(aboutUpdate.params[20], "unresolved");
});

test("an empty About description only initializes an empty projection", async () => {
  const fixture = clientFixture();
  const about = normalizeAboutMetrics({
    aboutObserved: true,
    metadata: {
      subscriber_count_text: "123 subscribers",
      subscriber_count_source: "youtube_about",
      view_count_text: "4,567 views",
      view_count_source: "youtube_about",
      video_count_text: "89 videos",
      video_count_source: "youtube_about",
    },
  });

  await recordAboutObservation(fixture.client, command(about, {
    aboutDescription: null,
    descriptionStatus: "empty",
    identity: {
      title: "Current title",
      handle: "@current",
      avatar_url: "https://example.test/avatar.jpg",
      keywords: [],
      available_tabs: [],
      summary: null,
    },
  }));

  const identityUpdate = fixture.calls.find((call) => call.sql.includes("about_identity_last_observed_at=$2"));
  assert.match(
    identityUpdate.sql,
    /summary=CASE[\s\S]*\$16::text='empty'[\s\S]*NULLIF\(btrim\(about_description\),''\)[\s\S]*NULLIF\(btrim\(summary\),''\)/,
  );
  assert.equal(identityUpdate.params[7], null);
  assert.equal(identityUpdate.params.includes("empty"), true);
});
