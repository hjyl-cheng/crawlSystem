import assert from "node:assert/strict";
import test from "node:test";
import { projectVideoDetail, videoDetailFieldStatus } from "../src/videoDetailEvidence.js";

test("evidence keeps estimated values, exact zero, policy zero and missing counts distinct", () => {
  for (const [input, expected] of [
    [{ view_count: 1200, view_count_status: "estimated" }, [1200, "estimated"]],
    [{ view_count_text: "1.2K views" }, [1200, "estimated"]],
    [{ view_count: 0 }, [0, "exact"]],
    [{ view_count: null }, [null, "unresolved"]],
  ]) {
    const facts = projectVideoDetail(input, { locale: "en" });
    assert.deepEqual([facts.view_count, facts.view_count_status], expected);
  }
  for (const status of ["exact", "zero_from_empty", "zero_from_surface", "zero_from_upcoming"]) {
    const facts = projectVideoDetail({ comment_count: 0, comment_count_status: status });
    assert.equal(facts.comment_count, 0);
    assert.equal(facts.comment_count_status, status);
  }
  assert.equal(projectVideoDetail({ comments_disabled: true }).comment_count_status, "disabled");
  assert.equal(projectVideoDetail({}).comments_disabled, null);
  assert.equal(projectVideoDetail({}).comment_count, null);
  const partial = projectVideoDetail({ comment_count: 17, comment_count_status: "unresolved",
    description: "Unverified text", description_status: "unavailable" });
  assert.equal(partial.comment_count, 17);
  assert.equal(partial.comment_count_status, "unresolved");
  assert.equal(partial.description_status, "unavailable");
  assert.equal(partial.description_observed, false);
  assert.equal(videoDetailFieldStatus({ comment_count: 17, comment_count_status: "unresolved" }).comment_count, "unobserved");
  assert.equal(videoDetailFieldStatus({ comment_count: 0 }, { required_surface: "comments" }).comment_count, "parser_gap");
});
