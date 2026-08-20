import assert from "node:assert/strict";
import test from "node:test";
import {
  PUBLICATION_WRITER_VERSION,
  publicationWriterVersionSatisfies,
} from "../src/publicationWriterVersion.js";

test("Publication Writer version is fixed in code and ordered within one version family", () => {
  assert.equal(PUBLICATION_WRITER_VERSION, "publication-reconciler-v1");
  assert.equal(publicationWriterVersionSatisfies("publication-reconciler-v1", "publication-reconciler-v1"), true);
  assert.equal(publicationWriterVersionSatisfies("publication-reconciler-v2", "publication-reconciler-v1"), true);
  assert.equal(publicationWriterVersionSatisfies("publication-reconciler-v1", "publication-reconciler-v2"), false);
  assert.equal(publicationWriterVersionSatisfies("other-writer-v2", "publication-reconciler-v1"), false);
  assert.equal(publicationWriterVersionSatisfies("", "publication-reconciler-v1"), false);
  assert.equal(publicationWriterVersionSatisfies("unversioned", "unversioned"), true);
});
