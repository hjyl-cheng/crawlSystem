import assert from "node:assert/strict";
import test from "node:test";
import { freshPublicationBootstrapCommand } from "../scripts/bootstrapFreshPublication.mjs";

test("fresh Publication bootstrap CLI is a read-only plan unless --apply is explicit", () => {
  assert.deepEqual(freshPublicationBootstrapCommand([]), { apply: false, help: false });
  assert.deepEqual(freshPublicationBootstrapCommand(["--apply"]), { apply: true, help: false });
  assert.deepEqual(freshPublicationBootstrapCommand(["--help"]), { apply: false, help: true });
  assert.throws(() => freshPublicationBootstrapCommand(["--apply", "--apply"]), /only be provided once/);
  assert.throws(() => freshPublicationBootstrapCommand(["--force"]), /unknown option/);
});
