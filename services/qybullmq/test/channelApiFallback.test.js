import assert from "node:assert/strict";
import test from "node:test";
import {
  channelApiFallbackMissingFields,
  resolveChannelApiFallback,
  resolveChannelQualificationAfterApi,
} from "../src/channelApiFallback.js";

test("missing required subscriber count invokes the channel data API", async () => {
  const calls = [];
  const result = await resolveChannelApiFallback({
    channelId: "UCmissing",
    metadata: { title: "Missing", subscriber_count: null },
    enforceMinSubscribers: true,
    fallbackMode: "emergency",
    apiKeys: ["key-a"],
    timeoutMs: 12000,
    dailyRequestLimit: 5000,
    reserveRequest: async (limit, count) => {
      calls.push(["reserve", limit, count]);
      return { request_count: 1 };
    },
    fetchDetails: async (ids, key, options) => {
      calls.push(["fetch", ids, key, options]);
      return {
        detailsById: new Map([["UCmissing", {
          channel_id: "UCmissing",
          subscriber_count: 2500,
          subscriber_count_source: "youtube_data_api_channels_list",
        }]]),
        raw: { items: [{ id: "UCmissing" }] },
        returnedCount: 1,
      };
    },
  });

  assert.deepEqual(channelApiFallbackMissingFields(
    { title: "Missing", subscriber_count: null },
    { enforceMinSubscribers: true },
  ), ["subscriber_count"]);
  assert.deepEqual(channelApiFallbackMissingFields(
    { title: "Legacy", subscriber_count: 2500, subscriber_count_source: null },
    { enforceMinSubscribers: true },
  ), ["subscriber_count"]);
  assert.equal(result.attempted, true);
  assert.equal(result.detail.subscriber_count, 2500);
  assert.deepEqual(calls, [
    ["reserve", 5000, 1],
    ["fetch", ["UCmissing"], "key-a", { timeoutMs: 12000 }],
  ]);
});

test("complete channel metadata skips the channel data API", async () => {
  let called = false;
  const result = await resolveChannelApiFallback({
    channelId: "UCcomplete",
    metadata: {
      title: "Complete",
      subscriber_count: 2500,
      subscriber_count_source: "youtube_channel_header",
    },
    enforceMinSubscribers: true,
    fallbackMode: "emergency",
    apiKeys: ["key-a"],
    reserveRequest: async () => {
      called = true;
      return { request_count: 1 };
    },
    fetchDetails: async () => {
      called = true;
      return null;
    },
  });

  assert.equal(result.attempted, false);
  assert.equal(result.reason, "not_needed");
  assert.equal(called, false);
});

test("channel data API transport failures remain retryable", async () => {
  await assert.rejects(
    resolveChannelApiFallback({
      channelId: "UCretry",
      metadata: { title: "Retry", subscriber_count: null },
      enforceMinSubscribers: true,
      fallbackMode: "emergency",
      apiKeys: ["key-a", "key-b"],
      dailyRequestLimit: 5000,
      reserveRequest: async () => ({ request_count: 1 }),
      fetchDetails: async () => {
        throw new Error("temporary channels.list failure");
      },
    }),
    (error) => {
      assert.equal(error.name, "Error");
      assert.match(error.message, /temporary channels\.list failure/);
      return true;
    },
  );
});

test("an empty channels.list response is a terminal unavailable outcome", async () => {
  const fallback = await resolveChannelApiFallback({
    channelId: "UCmissing",
    metadata: { title: "Missing", subscriber_count: null },
    enforceMinSubscribers: true,
    fallbackMode: "emergency",
    apiKeys: ["key-a"],
    dailyRequestLimit: 5000,
    reserveRequest: async () => ({ request_count: 1 }),
    fetchDetails: async () => ({
      detailsById: new Map(),
      raw: { items: [] },
      returnedCount: 0,
    }),
  });
  const qualification = resolveChannelQualificationAfterApi({
    qualified: false,
    status: "failed",
    reason: "subscriber_count_unknown",
    subscriberCount: null,
    minSubscriberCount: 1000,
  }, fallback);

  assert.equal(fallback.attempted, true);
  assert.equal(fallback.reason, "not_found");
  assert.deepEqual(fallback.raw, { items: [] });
  assert.equal(qualification.reason, "channel_unavailable");
  assert.deepEqual(resolveChannelQualificationAfterApi({
    qualified: true,
    status: "passed",
    reason: null,
    subscriberCount: 2500,
    minSubscriberCount: 1000,
  }, fallback), {
    qualified: false,
    status: "failed",
    reason: "channel_unavailable",
    subscriberCount: 2500,
    minSubscriberCount: 1000,
  });
});

test("an API-confirmed hidden subscriber count is a business outcome", () => {
  const qualification = resolveChannelQualificationAfterApi({
    qualified: false,
    status: "failed",
    reason: "subscriber_count_unknown",
    subscriberCount: null,
    minSubscriberCount: 1000,
  }, {
    attempted: true,
    detail: { hidden_subscriber_count: true },
  });

  assert.equal(qualification.qualified, false);
  assert.equal(qualification.reason, "subscriber_count_hidden");
});
