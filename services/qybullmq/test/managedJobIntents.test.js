import assert from "node:assert/strict";
import test from "node:test";
import {
  buildDiscoverPageIntent,
  buildQueryQualityChunkIntents,
  ManagedPolicyUnavailableError,
} from "../src/managedJobIntents.js";

const policies = [
  {
    id: "qy-br-discover-anonymous-v1",
    version: 1,
    hash: "sha256:discover-br",
    role: "discover",
    youtube_language: "pt-BR",
    youtube_country: "BR",
  },
  {
    id: "qy-br-query-quality-anonymous-v1",
    version: 1,
    hash: "sha256:quality-br",
    role: "query_quality",
    youtube_language: "pt-BR",
    youtube_country: "BR",
  },
];

test("Discover Page Intent is stable and its BullMQ payload contains only the persistent identity", () => {
  const input = {
    pageId: "query:42:run:stable:page:1",
    queryId: 42,
    queryText: "anime brasil",
    pageNo: 1,
    discoveryRunId: "query:42:run:stable",
    pipelineCycleId: "pipeline:1",
    dispatchBatchId: "pipeline:1",
    language: "pt-BR",
    country: "BR",
    searchFilter: "video",
    sort: "popularity",
    timeWindow: "this_year",
  };

  const first = buildDiscoverPageIntent(input, { policies });
  const replay = buildDiscoverPageIntent({ ...input, priority: 999 }, { policies });

  assert.equal(first.intentHash, replay.intentHash);
  assert.equal(first.policy.id, "qy-br-discover-anonymous-v1");
  assert.deepEqual(first.jobPayload, {
    page_id: "query:42:run:stable:page:1",
    intent_schema_version: 1,
  });
  assert.equal(first.managedIntent.continuation_token, null);
});

test("Query Quality freezes ordered members only after grouping by effective Locale and Policy", () => {
  const chunks = buildQueryQualityChunkIntents({
    qualityBatchId: "batch-1",
    batchOptions: {
      language: "pt-BR",
      country: "BR",
      min_subscriber_count: 1000,
      top_videos: 20,
      include_video_search: true,
    },
    tasks: [
      { quality_task_id: 3, query_id: 103, language: null, country: null },
      { quality_task_id: 1, query_id: 101, language: "pt-BR", country: "BR" },
      { quality_task_id: 2, query_id: 102, language: "pt-BR", country: "BR" },
    ],
    chunkSize: 2,
  }, { policies });

  assert.deepEqual(chunks.map((chunk) => chunk.qualityTaskIds), [[1, 2], [3]]);
  assert.ok(chunks.every((chunk) => chunk.policy.id === "qy-br-query-quality-anonymous-v1"));
  assert.ok(chunks.every((chunk) => Object.keys(chunk.jobPayload).sort().join(",")
    === "intent_schema_version,quality_chunk_id"));
  assert.notEqual(chunks[0].chunkIntentHash, chunks[1].chunkIntentHash);
});

test("an unsupported Query Quality Locale is deferred instead of silently using the BR Policy", () => {
  assert.throws(() => buildQueryQualityChunkIntents({
    qualityBatchId: "batch-us",
    batchOptions: {},
    tasks: [
      { quality_task_id: 1, query_id: 201, language: "en-US", country: "US" },
    ],
  }, { policies }), (error) => (
    error instanceof ManagedPolicyUnavailableError
      && error.role === "query_quality"
      && error.language === "en-US"
      && error.country === "US"
  ));
});
