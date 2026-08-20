import assert from "node:assert/strict";
import test from "node:test";
import {
  InMemoryManagedJobIntentRepository,
  ManagedJobIntentConflictError,
  ManagedJobIntentStore,
} from "../src/managedJobIntentStore.js";

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

function discoverInput(overrides = {}) {
  return {
    pageId: "query:7:run:stable:page:1",
    queryId: 7,
    queryText: "roblox brasil",
    pageNo: 1,
    discoveryRunId: "query:7:run:stable",
    pipelineCycleId: "pipeline:7",
    dispatchBatchId: "pipeline:7",
    language: "pt-BR",
    country: "BR",
    ...overrides,
  };
}

test("a Discover producer transaction creates one immutable Page and one dispatch record", async () => {
  const repository = new InMemoryManagedJobIntentRepository();
  const store = new ManagedJobIntentStore({ repository, policies });

  const first = await store.prepareDiscoverPage(discoverInput());
  const replay = await store.prepareDiscoverPage(discoverInput());

  assert.equal(first.created, true);
  assert.equal(replay.created, false);
  assert.equal(repository.pages.size, 1);
  assert.equal(repository.outbox.size, 1);
  assert.equal([...repository.outbox.values()][0].payload_json.page_id, first.page.page_id);
  assert.deepEqual(Object.keys([...repository.outbox.values()][0].payload_json).sort(), [
    "intent_schema_version",
    "page_id",
  ]);
});

test("a reused Discover page_id cannot overwrite a different immutable Intent", async () => {
  const repository = new InMemoryManagedJobIntentRepository();
  const store = new ManagedJobIntentStore({ repository, policies });
  await store.prepareDiscoverPage(discoverInput());

  await assert.rejects(
    store.prepareDiscoverPage(discoverInput({ queryText: "different query" })),
    (error) => error instanceof ManagedJobIntentConflictError
      && error.aggregateKind === "discover_page",
  );
  assert.equal(repository.pages.size, 1);
  assert.equal(repository.outbox.size, 1);
});

test("a Query Quality producer freezes each Task into exactly one persistent Chunk", async () => {
  const repository = new InMemoryManagedJobIntentRepository({
    qualityBatches: [{
      quality_batch_id: "batch-1",
      status: "queued",
      options_json: {
        language: "pt-BR",
        country: "BR",
        min_subscriber_count: 1000,
        include_video_search: true,
      },
      tasks: [1, 2, 3, 4].map((id) => ({
        quality_task_id: id,
        query_id: 100 + id,
        language: "pt-BR",
        country: "BR",
        status: "queued",
      })),
    }],
  });
  const store = new ManagedJobIntentStore({ repository, policies, qualityChunkSize: 3 });

  const first = await store.prepareQueryQualityBatch("batch-1");
  const replay = await store.prepareQueryQualityBatch("batch-1");

  assert.deepEqual(first.chunks.map((chunk) => chunk.quality_task_ids), [[1, 2, 3], [4]]);
  assert.equal(replay.chunks.length, 0);
  assert.equal(repository.qualityChunks.size, 2);
  assert.deepEqual(
    [...repository.qualityChunks.values()].map((chunk) => chunk.status),
    ["pending", "pending"],
  );
  assert.equal(repository.qualityMembers.size, 4);
  assert.equal(repository.outbox.size, 2);
});
