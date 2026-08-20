import assert from "node:assert/strict";
import test from "node:test";
import {
  evaluateChannelQualification,
  evaluateDiscoveryChannelQualification,
} from "../src/channelQualification.js";

test("channel qualification passes crawls that do not enforce a subscriber threshold", () => {
  assert.deepEqual(evaluateChannelQualification({
    subscriberCount: null,
    minSubscriberCount: 1000,
    required: false,
  }), {
    qualified: true,
    status: "not_required",
    reason: null,
    subscriberCount: null,
    minSubscriberCount: 1000,
  });
});

test("channel qualification rejects unknown and below-threshold counts", () => {
  assert.equal(evaluateChannelQualification({
    subscriberCount: null,
    minSubscriberCount: 1000,
    required: true,
  }).reason, "subscriber_count_unknown");
  assert.equal(evaluateChannelQualification({
    subscriberCount: 999,
    minSubscriberCount: 1000,
    required: true,
  }).reason, "subscriber_count_below_minimum");
});

test("channel qualification accepts a count at the configured threshold", () => {
  const result = evaluateChannelQualification({
    subscriberCount: "1000",
    minSubscriberCount: 1000,
    required: true,
  });
  assert.equal(result.qualified, true);
  assert.equal(result.status, "passed");
  assert.equal(result.subscriberCount, 1000);
});

test("discovery sends a missing subscriber row to snapshot validation", () => {
  const result = evaluateDiscoveryChannelQualification({
    subscriberCount: null,
    minSubscriberCount: 1000,
  });
  assert.equal(result.qualified, true);
  assert.equal(result.status, "needs_snapshot");
  assert.equal(result.reason, null);
  assert.equal(result.subscriberCount, null);
  assert.equal(result.subscriberCountMissing, true);
});
