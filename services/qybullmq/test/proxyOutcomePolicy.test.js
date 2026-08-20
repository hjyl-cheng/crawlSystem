import assert from "node:assert/strict";
import test from "node:test";
import { shouldReportProxyJobSuccess } from "../src/proxyOutcomePolicy.js";

test("proxy success requires complete business output", () => {
  assert.equal(shouldReportProxyJobSuccess("channel-crawl", {
    phase_timings_ms: { total: 1000 },
    detail_partial: 0,
  }), true);
  assert.equal(shouldReportProxyJobSuccess("channel-crawl", {
    phase_timings_ms: { total: 1000 },
    detail_partial: 1,
  }), false);
  assert.equal(shouldReportProxyJobSuccess("channel-detail-repair", {
    status: "done",
    failed: 0,
    partial: 0,
  }), true);
  assert.equal(shouldReportProxyJobSuccess("channel-detail-repair", {
    status: "done",
    failed: 0,
    partial: 1,
  }), false);
});
