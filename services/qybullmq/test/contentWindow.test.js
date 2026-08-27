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
  assert.equal(isOutsideContentWindow({
    published_at: "2026-04-22T23:59:00Z",
    source: "test_detail",
  }, 90, reference), false);
  assert.equal(isOutsideContentWindow({
    published_at: "2026-04-21T23:59:00Z",
    source: "test_detail",
  }, 90, reference), true);
});

test("isOutsideContentWindow excludes only known out-of-window content", () => {
  assert.equal(isOutsideContentWindow({ published_at: "2026-04-01", source: "test_detail" }, 90, now), true);
  assert.equal(isOutsideContentWindow({ published_at: "2026-06-01", source: "test_detail" }, 90, now), false);
  assert.equal(isOutsideContentWindow({ published_at: "2026-04-01" }, 90, now), false);
  assert.equal(isOutsideContentWindow({}, 90, now), false);
  assert.equal(isOutsideContentWindow({ published_at: "2020-01-01" }, 0, now), false);
});

test("isOutsideContentWindow uses exact instants and keeps ambiguous evidence", () => {
  const reference = "2026-08-26T05:36:44Z";
  assert.equal(isOutsideContentWindow({
    published_at: "2026-05-28T17:20:53Z",
    published_at_status: "exact",
    published_at_precision: "second",
    published_at_source: "yt_dlp_timestamp",
  }, 90, reference), false, "ITDP is inside the exact window");
  assert.equal(isOutsideContentWindow({
    published_at: "2026-05-28T01:33:12Z",
    published_at_status: "exact",
    published_at_precision: "second",
    published_at_source: "yt_dlp_timestamp",
  }, 90, reference), true, "Boxe is outside the exact window");
  assert.equal(isOutsideContentWindow({
    published_at: "2026-05-28",
    published_at_status: "exact",
    published_at_precision: "date_only",
    published_at_source: "yt_dlp_upload_date",
  }, 90, reference), false, "a cutoff-overlap date must continue to detail");
  assert.equal(isOutsideContentWindow({
    published_at: "2026-01-01",
    published_at_status: "relative",
    published_at_precision: "date_only",
    published_at_source: "youtube_uploads_relative_time",
  }, 90, reference), false, "relative evidence cannot prove an item is old");
  assert.equal(isOutsideContentWindow({
    published_at: "2026-01-01",
    published_at_precision: "date_only",
    published_at_source: "youtube_uploads",
  }, 90, reference), false, "a flattened historical Uploads source cannot prove an item is old");
});
