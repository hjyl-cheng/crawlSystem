import assert from "node:assert/strict";
import test from "node:test";
import {
  assertSameFullCrawlFetchContract,
  defaultFullCrawlFetchContract,
  FullCrawlFetchContractError,
  fullCrawlFetchContractId,
  LEGACY_FULL_CRAWL_FETCH_CONTRACT,
  LEGACY_FULL_CRAWL_FETCH_CONTRACT_ID,
  newFullCrawlFetchContractForJob,
  readFullCrawlFetchContractFromIntent,
  YOUTUBEJS_FULL_CRAWL_FETCH_CONTRACT,
  YOUTUBEJS_FULL_CRAWL_FETCH_CONTRACT_ID,
  YOUTUBEJS_FULL_CRAWL_V1_FETCH_CONTRACT,
  isYoutubeJsFullCrawlFetchContract,
} from "../src/fullCrawlFetchContract.js";

test("optional comments use v2 without changing the deployed v1 hash or accepting a version switch", () => {
  assert.equal(YOUTUBEJS_FULL_CRAWL_V1_FETCH_CONTRACT.contract_hash,
    "sha256:a80e2f3c6e94042b4c12df5ff1dd94a06afd36d395e238904f4d4ca8f575dd80");
  assert.equal(YOUTUBEJS_FULL_CRAWL_FETCH_CONTRACT.executor_version, 2);
  assert.equal(isYoutubeJsFullCrawlFetchContract(YOUTUBEJS_FULL_CRAWL_V1_FETCH_CONTRACT), true);
  assert.equal(isYoutubeJsFullCrawlFetchContract(YOUTUBEJS_FULL_CRAWL_FETCH_CONTRACT), true);
  assert.throws(() => assertSameFullCrawlFetchContract(
    YOUTUBEJS_FULL_CRAWL_V1_FETCH_CONTRACT, YOUTUBEJS_FULL_CRAWL_FETCH_CONTRACT,
  ), FullCrawlFetchContractError);
});

test("historical intent without a fetch contract remains legacy", () => {
  const resolved = readFullCrawlFetchContractFromIntent({
    intent: { job_name: "channel-snapshot", crawl_mode: "full" },
  });

  assert.equal(resolved.explicit, false);
  assert.equal(resolved.id, LEGACY_FULL_CRAWL_FETCH_CONTRACT_ID);
  assert.deepEqual(resolved.contract, LEGACY_FULL_CRAWL_FETCH_CONTRACT);
});

test("new ordinary Full Crawl uses the configured default", () => {
  const resolved = newFullCrawlFetchContractForJob(
    { name: "channel-snapshot" },
    { FULL_CRAWL_FETCH_CONTRACT_DEFAULT: YOUTUBEJS_FULL_CRAWL_FETCH_CONTRACT_ID },
  );

  assert.equal(fullCrawlFetchContractId(resolved), YOUTUBEJS_FULL_CRAWL_FETCH_CONTRACT_ID);
});

test("repairs and controlled recovery stay legacy despite the configured default", () => {
  const environment = {
    FULL_CRAWL_FETCH_CONTRACT_DEFAULT: YOUTUBEJS_FULL_CRAWL_FETCH_CONTRACT_ID,
  };
  for (const name of [
    "channel-snapshot-recovery",
    "channel-crawl-repair",
    "channel-full-repair",
    "channel-checkpoint-repair",
    "channel-detail-repair",
  ]) {
    assert.equal(
      fullCrawlFetchContractId(newFullCrawlFetchContractForJob({ name }, environment)),
      LEGACY_FULL_CRAWL_FETCH_CONTRACT_ID,
    );
  }
});

test("unknown default and altered manifest hash fail closed", () => {
  assert.throws(
    () => defaultFullCrawlFetchContract({ FULL_CRAWL_FETCH_CONTRACT_DEFAULT: "future_v9" }),
    FullCrawlFetchContractError,
  );
  assert.throws(
    () => fullCrawlFetchContractId({
      ...YOUTUBEJS_FULL_CRAWL_FETCH_CONTRACT,
      contract_hash: "sha256:altered",
    }),
    FullCrawlFetchContractError,
  );
});

test("contract comparison accepts exact replay and rejects executor switching", () => {
  assert.deepEqual(
    assertSameFullCrawlFetchContract(
      YOUTUBEJS_FULL_CRAWL_FETCH_CONTRACT,
      YOUTUBEJS_FULL_CRAWL_FETCH_CONTRACT,
    ),
    YOUTUBEJS_FULL_CRAWL_FETCH_CONTRACT,
  );
  assert.throws(
    () => assertSameFullCrawlFetchContract(
      YOUTUBEJS_FULL_CRAWL_FETCH_CONTRACT,
      LEGACY_FULL_CRAWL_FETCH_CONTRACT,
    ),
    FullCrawlFetchContractError,
  );
});
