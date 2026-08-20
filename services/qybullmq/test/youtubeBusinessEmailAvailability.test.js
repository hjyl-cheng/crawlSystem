import assert from "node:assert/strict";
import test from "node:test";
import {
  normalizeYoutubeBusinessEmailCurrent,
  observeYoutubeBusinessEmail,
} from "../src/youtubeBusinessEmailAvailability.js";

test("a non-empty YouTube business email marker is available", () => {
  assert.deepEqual(observeYoutubeBusinessEmail({
    metadata: { sign_in_for_business_email: "Sign in to see email address" },
  }, { aboutObserved: true }), {
    available: true,
    status: "available",
  });
});

test("a recognized About response without the marker is not available", () => {
  assert.deepEqual(observeYoutubeBusinessEmail({ metadata: {} }, { aboutObserved: true }), {
    available: false,
    status: "not_available",
  });
});

test("failed or unrecognized About responses remain unknown", () => {
  assert.deepEqual(observeYoutubeBusinessEmail(null, { aboutObserved: false }), {
    available: null,
    status: "unknown",
  });
  assert.deepEqual(observeYoutubeBusinessEmail({}, { aboutObserved: true }), {
    available: null,
    status: "unknown",
  });
});

test("stored availability pairs cannot turn malformed or failed evidence into false", () => {
  assert.deepEqual(normalizeYoutubeBusinessEmailCurrent(true, "available"), {
    available: true,
    status: "available",
  });
  assert.deepEqual(normalizeYoutubeBusinessEmailCurrent(false, "not_available"), {
    available: false,
    status: "not_available",
  });
  assert.deepEqual(normalizeYoutubeBusinessEmailCurrent(true, "available", {
    aboutObserved: false,
  }), {
    available: null,
    status: "unknown",
  });
  assert.deepEqual(normalizeYoutubeBusinessEmailCurrent(false, "available"), {
    available: null,
    status: "unknown",
  });
});
