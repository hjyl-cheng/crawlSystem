import assert from "node:assert/strict";
import test from "node:test";
import { detailAgeDays, isOutsideContentWindow } from "../src/contentWindow.js";

const now = Date.parse("2026-07-11T12:00:00Z");

test("detailAgeDays supports second and date-only timestamps", () => {
  assert.equal(detailAgeDays({ published_at: "2026-07-10T12:00:00Z" }, now), 1);
  assert.equal(detailAgeDays({ published_at: "2026-04-01" }, now), 101);
  assert.equal(detailAgeDays({ published_text: "unknown" }, now), null);
});

test("detailAgeDays uses UTC calendar days at the 90-day boundary", () => {
  const reference = "2026-07-21T00:01:00Z";
  assert.equal(detailAgeDays({ published_at: "2026-04-22T23:59:00Z" }, reference), 90);
  assert.equal(detailAgeDays({ published_at: "2026-04-21T23:59:00Z" }, reference), 91);
  assert.equal(isOutsideContentWindow({ published_at: "2026-04-22T23:59:00Z" }, 90, reference), false);
  assert.equal(isOutsideContentWindow({ published_at: "2026-04-21T23:59:00Z" }, 90, reference), true);
});

test("isOutsideContentWindow excludes only known out-of-window content", () => {
  assert.equal(isOutsideContentWindow({ published_at: "2026-04-01" }, 90, now), true);
  assert.equal(isOutsideContentWindow({ published_at: "2026-06-01" }, 90, now), false);
  assert.equal(isOutsideContentWindow({}, 90, now), false);
  assert.equal(isOutsideContentWindow({ published_at: "2020-01-01" }, 0, now), false);
});
