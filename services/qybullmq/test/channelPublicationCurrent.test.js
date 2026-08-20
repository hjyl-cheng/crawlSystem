import assert from "node:assert/strict";
import test from "node:test";
import {
  buildChannelPublicationCurrent,
} from "../src/channelPublicationCurrent.js";
import {
  normalizeJoinedDateCurrent,
  parseYoutubeJoinedDate,
} from "../src/youtubeJoinedDate.js";

const CHANNEL_ID = "UCchannel-current";

function completeRow(overrides = {}) {
  return {
    channel_id: CHANNEL_ID,
    channel_url: `https://www.youtube.com/channel/${CHANNEL_ID}`,
    title: "Channel Current",
    handle: "@channelcurrent",
    avatar_url: "https://yt3.googleusercontent.com/avatar?temporary=one",
    keywords: ["Publication", "Channel"],
    keywords_status: "observed",
    available_tabs: ["videos", "shorts"],
    available_tabs_status: "observed",
    about_description: "Complete About description",
    joined_date_text: "Joined Apr 2, 2020",
    joined_at: null,
    joined_at_precision: "unknown",
    subscriber_count: 1000,
    subscriber_count_status: "exact",
    subscriber_count_source: "youtube_about",
    total_view_count: 2000,
    total_view_count_status: "exact",
    total_view_count_source: "youtube_about",
    total_video_count: 30,
    total_video_count_status: "exact",
    total_video_count_source: "youtube_about",
    is_verified: false,
    is_verified_status: "not_verified",
    is_family_safe: true,
    external_links: [],
    external_links_status: "observed",
    youtube_business_email_available: true,
    youtube_business_email_observed_at: "2026-07-31T10:00:00.000Z",
    status: "active",
    country: "Brazil",
    country_code: "BR",
    country_canonical_name: "Brazil",
    source_json: { channel_header: {} },
    ...overrides,
  };
}

test("Joined Date normalization parses the observed YouTube format deterministically", () => {
  assert.equal(parseYoutubeJoinedDate("Joined Apr 2, 2020"), "2020-04-02");
  assert.equal(
    parseYoutubeJoinedDate("Inscreveu-se em 2 de abril de 2020", { locale: "pt-BR" }),
    "2020-04-02",
  );
  assert.equal(parseYoutubeJoinedDate("Joined Feb 31, 2020"), null);
  assert.deepEqual(normalizeJoinedDateCurrent({
    joinedAt: null,
    joinedDateText: "Joined Apr 2, 2020",
    precision: "unknown",
  }), {
    value: "2020-04-02",
    raw: "Joined Apr 2, 2020",
    status: "exact",
    precision: "date_only",
  });
});

test("a complete normalized Channel Current has the full V2 contract and stable hash", () => {
  const first = buildChannelPublicationCurrent(completeRow());
  const second = buildChannelPublicationCurrent(completeRow({
    avatar_url: "https://yt3.googleusercontent.com/avatar?temporary=two",
    keywords: ["Channel", "Publication", "Channel"],
    available_tabs: ["shorts", "videos"],
  }));

  assert.equal(first.ready, true);
  assert.equal(first.payload.joined_date, "2020-04-02");
  assert.equal(first.payload.rss_url, `https://www.youtube.com/feeds/videos.xml?channel_id=${CHANNEL_ID}`);
  assert.equal(first.contract_version, 2);
  assert.equal(first.payload.youtube_business_email_available, true);
  assert.equal(first.payload.youtube_business_email_observed_at, "2026-07-31T10:00:00.000Z");
  assert.deepEqual(first.payload.avatar, [{
    url: "https://yt3.googleusercontent.com/avatar",
    position: 0,
  }]);
  assert.equal(first.result_hash, second.result_hash);
  assert.match(first.result_hash, /^sha256:[0-9a-f]{64}$/);
});

test("Header fallback fills old operational rows while optional unknown fields remain publishable", () => {
  const result = buildChannelPublicationCurrent(completeRow({
    avatar_url: null,
    keywords: [],
    keywords_status: "unresolved",
    available_tabs: [],
    available_tabs_status: "unresolved",
    about_description: null,
    is_verified: null,
    is_verified_status: "unknown",
    source_json: {
      channel_header: {
        avatar_url: "https://example.test/avatar.jpg",
        keywords: "\"one phrase\" second",
        available_tabs: ["videos", "live"],
        description: "Header fallback description",
      },
    },
  }));

  assert.equal(result.ready, true);
  assert.deepEqual(result.payload.keywords, ["one phrase", "second"]);
  assert.equal(result.payload.has_live_streams, true);
  assert.equal(result.payload.is_verified, null);
  assert.equal(result.payload.is_verified_status, "unknown");
  assert.equal(result.warnings.some((item) => item.code === "channel_verified_state_unknown"), true);
});

test("Header fallback keeps the observed Verified value and status as one pair", () => {
  const result = buildChannelPublicationCurrent(completeRow({
    is_verified: null,
    is_verified_status: "unknown",
    source_json: {
      channel_header: {
        is_verified: false,
        is_verified_status: "not_verified",
      },
    },
  }));

  assert.equal(result.payload.is_verified, false);
  assert.equal(result.payload.is_verified_status, "not_verified");
  assert.equal(result.issues.some((item) => item.code === "channel_verified_state_unknown"), false);
  assert.equal(result.ready, true);
});

test("an unobserved empty Link list is absent without blocking the Channel Domain", () => {
  const result = buildChannelPublicationCurrent(completeRow({
    external_links: [],
    external_links_status: "unresolved",
    source_json: {
      channel_header: {
        external_links: [],
      },
    },
  }));
  assert.equal(result.ready, true);
  assert.equal(result.links.explicit_empty, false);
  assert.equal(result.warnings.some((item) => item.code === "channel_links_state_unknown"), true);
});

test("missing optional Channel fields do not weaken identity and lifecycle gates", () => {
  const optional = buildChannelPublicationCurrent(completeRow({
    avatar_url: null,
    keywords: [],
    keywords_status: "unresolved",
    available_tabs: [],
    available_tabs_status: "unresolved",
    about_description: null,
    description_status: "unresolved",
    joined_date_text: null,
    is_family_safe: null,
    is_verified: null,
    is_verified_status: "unknown",
    external_links: [],
    external_links_status: "unresolved",
    youtube_business_email_available: null,
    youtube_business_email_observed_at: null,
  }));
  const invalidIdentity = buildChannelPublicationCurrent(completeRow({
    channel_id: null,
    channel_url: null,
    status: "removed",
  }));

  assert.equal(optional.ready, true);
  assert.equal(optional.payload.description, null);
  assert.deepEqual(optional.payload.links, []);
  assert.equal(optional.payload.youtube_business_email_available, null);
  assert.equal(optional.warnings.some((item) => (
    item.field === "youtube_business_email_available"
  )), true);
  assert.equal(optional.warnings.length > 0, true);
  assert.equal(invalidIdentity.ready, false);
  assert.equal(invalidIdentity.issues.some((item) => item.field === "channel_id"), true);
  assert.equal(invalidIdentity.issues.some((item) => item.field === "lifecycle_status"), true);
});

test("a confirmed removed business email entry is published as false", () => {
  const result = buildChannelPublicationCurrent(completeRow({
    youtube_business_email_available: false,
    youtube_business_email_observed_at: new Date("2026-07-31T11:00:00.000Z"),
  }));

  assert.equal(result.ready, true);
  assert.equal(result.payload.youtube_business_email_available, false);
  assert.equal(result.payload.youtube_business_email_observed_at, "2026-07-31T11:00:00.000Z");
});

test("an explicitly empty About description never falls back to stale Header text", () => {
  const result = buildChannelPublicationCurrent(completeRow({
    about_description: null,
    summary: null,
    description_status: "empty",
    source_json: {
      channel_header: { description: "STALE HEADER DESCRIPTION" },
    },
  }));

  assert.equal(result.ready, true);
  assert.equal(result.payload.description, null);
  assert.equal(result.checks.find((item) => item.field === "description").complete, true);
});
