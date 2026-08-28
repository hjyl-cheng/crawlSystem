import assert from "node:assert/strict";
import test from "node:test";
import {
  PUBLICATION_TIME_CLASSIFIER_VERSION,
  classifyPublicationWindow,
  normalizePublicationEvidence,
  publicationEvidenceConflictRecord,
  publicationEvidenceFromFields,
  selectPublicationEvidence,
} from "../src/publicationTimeEvidence.js";

const asOf = "2026-08-26T05:36:44.000Z";

test("normalizes the three Uploads publication evidence variants", () => {
  assert.deepEqual(normalizePublicationEvidence({
    published_at: "2026-05-28T17:20:53Z",
    published_at_status: "exact",
    published_at_precision: "second",
    published_at_source: "yt_dlp_flat_timestamp",
  }), {
    published_at: "2026-05-28T17:20:53.000Z",
    published_at_status: "exact",
    published_at_precision: "second",
    published_at_source: "yt_dlp_flat_timestamp",
  });

  assert.deepEqual(normalizePublicationEvidence({
    published_at: "2026-05-28",
    published_at_status: "exact",
    published_at_precision: "date_only",
    published_at_source: "yt_dlp_flat_upload_date",
  }), {
    published_at: "2026-05-28T00:00:00.000Z",
    published_at_status: "exact",
    published_at_precision: "date_only",
    published_at_source: "yt_dlp_flat_upload_date",
  });

  assert.deepEqual(normalizePublicationEvidence({
    published_at: new Date("2026-05-28T00:00:00.000Z"),
    published_at_status: "exact",
    published_at_precision: "date_only",
    published_at_source: "crawler_contents",
  }), {
    published_at: "2026-05-28T00:00:00.000Z",
    published_at_status: "exact",
    published_at_precision: "date_only",
    published_at_source: "crawler_contents",
  });

  assert.deepEqual(normalizePublicationEvidence({
    published_at: "2026-05-28",
    published_at_status: "relative",
    published_at_precision: "date_only",
    published_at_source: "youtube_uploads_relative_time",
  }), {
    published_at: "2026-05-28T00:00:00.000Z",
    published_at_status: "relative",
    published_at_precision: "date_only",
    published_at_source: "youtube_uploads_relative_time",
  });
});

test("invalid or incomplete publication evidence normalizes to unresolved", () => {
  for (const input of [
    {},
    { published_at: null, published_at_status: "exact", published_at_precision: "second", published_at_source: "test" },
    { published_at: null, published_at_status: "exact", published_at_precision: "date_only", published_at_source: "test" },
    { published_at: "not-a-date", published_at_status: "exact", published_at_precision: "second", published_at_source: "test" },
    { published_at: "2026-05-28", published_at_status: "exact", published_at_precision: "date_only" },
    { published_at: "2026-05-28", published_at_status: "relative", published_at_precision: "minute", published_at_source: "test" },
  ]) {
    assert.deepEqual(normalizePublicationEvidence(input), {
      published_at: null,
      published_at_status: "unresolved",
      published_at_precision: "unknown",
      published_at_source: null,
    });
  }
});

test("compatibility inference is centralized and requires usable provenance", () => {
  assert.deepEqual(publicationEvidenceFromFields({
    published_at: "2026-05-28T17:20:53Z",
    source: "yt_dlp",
  }), {
    published_at: "2026-05-28T17:20:53.000Z",
    published_at_status: "exact",
    published_at_precision: "second",
    published_at_source: "yt_dlp",
  });
  assert.equal(publicationEvidenceFromFields({
    published_at: "2026-05-28",
    published_at_source: "youtube_uploads_relative_time",
  }).published_at_status, "relative");
  for (const input of [
    { published_at: "2026-05-28" },
    { published_at: "2026-05-28", published_at_source: "youtube_uploads" },
  ]) {
    assert.deepEqual(publicationEvidenceFromFields(input), {
      published_at: null,
      published_at_status: "unresolved",
      published_at_precision: "unknown",
      published_at_source: null,
    });
  }
});

test("selects publication evidence by status and precision as one quartet", () => {
  const exactDate = {
    published_at: "2026-05-28",
    published_at_status: "exact",
    published_at_precision: "date_only",
    published_at_source: "yt_dlp_flat_upload_date",
  };
  const relativeDate = {
    published_at: "2026-05-01",
    published_at_status: "relative",
    published_at_precision: "date_only",
    published_at_source: "youtube_uploads_relative_time",
  };
  const exactSecond = {
    published_at: "2026-05-28T17:20:53Z",
    published_at_status: "exact",
    published_at_precision: "second",
    published_at_source: "yt_dlp_flat_timestamp",
  };

  assert.deepEqual(selectPublicationEvidence(relativeDate, exactDate).evidence,
    normalizePublicationEvidence(exactDate));
  assert.deepEqual(selectPublicationEvidence(exactDate, relativeDate).evidence,
    normalizePublicationEvidence(exactDate));
  assert.deepEqual(selectPublicationEvidence(exactDate, exactSecond).evidence,
    normalizePublicationEvidence(exactSecond));
});

test("equal-quality conflicts retain current evidence and expose the conflict", () => {
  const current = {
    published_at: "2026-05-29",
    published_at_status: "exact",
    published_at_precision: "date_only",
    published_at_source: "same_source",
  };
  const candidate = {
    published_at: "2026-05-28",
    published_at_status: "exact",
    published_at_precision: "date_only",
    published_at_source: "same_source",
  };
  const selected = selectPublicationEvidence(current, candidate);

  assert.deepEqual(selected.evidence, normalizePublicationEvidence(current));
  assert.equal(selected.selected, "current");
  assert.equal(selected.reason_code, "equal_quality_conflict_current_retained");
  assert.deepEqual(selected.conflict, {
    reason_code: "equal_quality_publication_conflict",
    current: normalizePublicationEvidence(current),
    candidate: normalizePublicationEvidence(candidate),
  });
  assert.deepEqual(publicationEvidenceConflictRecord(selected), {
    ...selected.conflict,
    resolution: {
      selected: "current",
      reason_code: "equal_quality_conflict_current_retained",
    },
  });

  const reverse = selectPublicationEvidence(candidate, current);
  assert.deepEqual(reverse.evidence, normalizePublicationEvidence(candidate));
  assert.equal(reverse.reason_code, "equal_quality_conflict_current_retained");
});

test("equal publication values from different sources are idempotent", () => {
  const current = {
    published_at: "2026-05-29T12:34:56.000Z",
    published_at_status: "exact",
    published_at_precision: "second",
    published_at_source: "youtubejs_player_microformat",
  };
  const candidate = {
    ...current,
    published_at_source: "yt_dlp_timestamp",
  };
  const selected = selectPublicationEvidence(current, candidate);

  assert.deepEqual(selected.evidence, normalizePublicationEvidence(current));
  assert.equal(selected.selected, "equal");
  assert.equal(selected.reason_code, "equivalent");
  assert.equal(selected.conflict, undefined);
});

test("classifies the real ITDP and Boxe timestamps at the exact 90-day cutoff", () => {
  const itdp = classifyPublicationWindow({
    published_at: "2026-05-28T17:20:53Z",
    published_at_status: "exact",
    published_at_precision: "second",
    published_at_source: "yt_dlp_timestamp",
  }, { asOf, maxAgeDays: 90 });
  const boxe = classifyPublicationWindow({
    published_at: "2026-05-28T01:33:12Z",
    published_at_status: "exact",
    published_at_precision: "second",
    published_at_source: "yt_dlp_timestamp",
  }, { asOf, maxAgeDays: 90 });

  assert.equal(itdp.relation, "inside");
  assert.equal(boxe.relation, "outside");
  assert.equal(itdp.classifier_version, PUBLICATION_TIME_CLASSIFIER_VERSION);
});

test("exact date-only evidence on the cutoff day overlaps the cutoff instant", () => {
  const result = classifyPublicationWindow({
    published_at: "2026-05-28",
    published_at_status: "exact",
    published_at_precision: "date_only",
    published_at_source: "yt_dlp_upload_date",
  }, { asOf, maxAgeDays: 90 });

  assert.deepEqual({
    relation: result.relation,
    reason_code: result.reason_code,
    basis: result.basis,
  }, {
    relation: "cutoff_overlap",
    reason_code: "date_only_spans_cutoff",
    basis: "utc_civil_date",
  });
});

test("only exact evidence can produce a resolved window relation", () => {
  for (const publishedAtStatus of ["relative", "estimated", "unavailable", "unresolved"]) {
    const result = classifyPublicationWindow({
      published_at: "2026-08-25",
      published_at_status: publishedAtStatus,
      published_at_precision: publishedAtStatus === "unavailable" || publishedAtStatus === "unresolved"
        ? "unknown"
        : "date_only",
      published_at_source: publishedAtStatus === "unavailable" || publishedAtStatus === "unresolved"
        ? null
        : "test_inference",
    }, { asOf, maxAgeDays: 90 });
    assert.equal(result.relation, "unresolved", publishedAtStatus);
  }
});

test("classifies exact publication evidence after the observation instant", () => {
  const result = classifyPublicationWindow({
    published_at: "2026-08-26T05:36:45Z",
    published_at_status: "exact",
    published_at_precision: "second",
    published_at_source: "test",
  }, { asOf, maxAgeDays: 90 });

  assert.equal(result.relation, "after_as_of");
});
