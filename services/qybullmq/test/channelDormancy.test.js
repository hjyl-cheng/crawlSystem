import assert from "node:assert/strict";
import test from "node:test";
import {
  addUtcCalendarDays,
  buildDormantLifecycle,
  DORMANT_RECHECK_MAX_DAYS,
  DORMANT_RECHECK_MIN_DAYS,
  dormantRecheckDelayDays,
  evaluateVideoActivity,
} from "../src/channelDormancy.js";

test("dormant recheck delay is deterministic and remains inside 30-90 days", () => {
  const first = dormantRecheckDelayDays("UCtest", 1);
  assert.equal(first, dormantRecheckDelayDays("UCtest", 1));
  assert.ok(first >= DORMANT_RECHECK_MIN_DAYS);
  assert.ok(first <= DORMANT_RECHECK_MAX_DAYS);
  const state = buildDormantLifecycle({
    channelId: "UCtest",
    observedAt: "2026-07-23T23:59:59.000Z",
  });
  assert.equal(
    state.dormant_recheck_day,
    addUtcCalendarDays("2026-07-23", state.dormant_recheck_delay_days),
  );
  assert.equal(state.dormant_cycle, 1);
});

test("a later dormant cycle preserves dormant_since and derives a new UTC recheck", () => {
  const state = buildDormantLifecycle({
    channelId: "UCtest",
    observedAt: "2026-09-01T00:00:01.000Z",
    dormantSince: "2026-07-23T20:00:00.000Z",
    dormantCycle: 1,
  });
  assert.equal(state.dormant_since, "2026-07-23T20:00:00.000Z");
  assert.equal(state.dormant_cycle, 2);
});

test("only a complete zero-content scan can enter dormancy", () => {
  assert.equal(evaluateVideoActivity({
    recentPublishedContentCount: 0,
    discoveryComplete: false,
  }).decision, "inconclusive");
  assert.equal(evaluateVideoActivity({
    recentPublishedContentCount: 0,
    uncertainContentCount: 1,
    discoveryComplete: true,
  }).decision, "inconclusive");
  assert.equal(evaluateVideoActivity({
    recentPublishedContentCount: 0,
    discoveryComplete: true,
  }).decision, "dormant");
  assert.equal(evaluateVideoActivity({
    recentPublishedContentCount: 1,
    discoveryComplete: false,
  }).decision, "active");
});

