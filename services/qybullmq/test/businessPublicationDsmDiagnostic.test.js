import assert from "node:assert/strict";
import test from "node:test";
import {
  DSM_DIAGNOSTIC_HELP,
} from "../scripts/diagnoseBusinessPublicationDsm.mjs";

test("Business Publication DSM diagnostic documents its repeatable public modes", () => {
  assert.match(DSM_DIAGNOSTIC_HELP, /audit-plans.*read-only/s);
  assert.match(DSM_DIAGNOSTIC_HELP, /reproduce.*Parallel Hash/s);
  assert.match(DSM_DIAGNOSTIC_HELP, /INC009_DSM_DATABASE_URL/);
  assert.match(DSM_DIAGNOSTIC_HELP, /INC009_DSM_CONTAINER/);
  assert.match(DSM_DIAGNOSTIC_HELP, /64 MiB.*DSM failure/s);
  assert.match(DSM_DIAGNOSTIC_HELP, /256 MiB.*succeed/s);
});
