import assert from "node:assert/strict";
import test from "node:test";
import { UnrecoverableError } from "bullmq";

import {
  isParserContractError,
  parserContractDetails,
  ParserContractError,
} from "../src/localizedParsing.js";

test("parser contract failures are unrecoverable BullMQ errors", () => {
  const error = new ParserContractError({
    field: "subscriber_count",
    value: "new localized subscriber format",
    locale: "ko",
    source: "youtube_about",
    reason: "unsupported_localized_count",
    context: { channel_id: "UCglobal" },
  });

  assert.equal(error instanceof UnrecoverableError, true);
  assert.equal(isParserContractError(error), true);
  assert.deepEqual(parserContractDetails(new Error(error.message)), error.toJSON());
});
