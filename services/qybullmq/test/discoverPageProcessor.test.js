import assert from "node:assert/strict";
import test from "node:test";

import { createDiscoverPageProcessor } from "../src/discoverPageProcessor.js";
import {
  discoverSearchPageRequest,
  parseDiscoverSearchPage,
  VIDEO_POPULARITY_THIS_YEAR_FILTER_PARAM,
} from "../src/youtube.js";

const NEW_CHANNEL = "UCnewchannel00000000000a";
const EXISTING_CHANNEL = "UCexistingchannel000000b";

function videoRenderer(videoId, channelId, owner) {
  return {
    videoRenderer: {
      videoId,
      title: { runs: [{ text: `video ${videoId}` }] },
      ownerText: {
        runs: [{
          text: owner,
          navigationEndpoint: { browseEndpoint: { browseId: channelId, canonicalBaseUrl: `/@${owner}` } },
        }],
      },
      viewCountText: { simpleText: "1,234 views" },
    },
  };
}

function searchData(channels = [[NEW_CHANNEL, "newowner"], [EXISTING_CHANNEL, "oldowner"]]) {
  return {
    contents: {
      items: [
        ...channels.map(([channelId, owner], index) => videoRenderer(`vid${index}`, channelId, owner)),
        {
          continuationItemRenderer: {
            continuationEndpoint: { continuationCommand: { token: "next-token" } },
          },
        },
      ],
    },
  };
}

function searchHtml(data = searchData()) {
  return `<html><script>var ytInitialData = ${JSON.stringify(data)};</script>`
    + `<script>ytcfg.set({"INNERTUBE_API_KEY":"api-key-1","INNERTUBE_CONTEXT":{"client":{"clientName":"WEB","clientVersion":"2.1"}}});</script></html>`;
}

function preparedPage({ pageId = "page-1", managedIntent = {} } = {}) {
  return {
    page_id: pageId,
    page_intent_hash: "hash-1",
    query_text: "receitas",
    query_id: 42,
    page_no: 1,
    dispatch_batch_id: "batch-1",
    request_language: "pt-BR",
    request_country: "BR",
    priority: 100,
    result_json: { managed_intent: { pipeline_cycle_id: "cycle-1", ...managedIntent } },
  };
}

function harness({ claimRows = 1, currentStatus = "running", requestPage } = {}) {
  const calls = [];
  const jobs = { bulk: [], single: [] };
  const raws = [];
  const signals = [];
  const requests = [];
  let nextCandidateId = 100;
  const query = async (sql, params = []) => {
    const text = sql.replace(/\s+/g, " ").trim();
    calls.push({ text, params });
    if (text.startsWith("UPDATE crawler.query_pages SET status='running'")) return { rowCount: claimRows, rows: [] };
    if (text.startsWith("SELECT managed_fetch_status")) return { rowCount: 1, rows: [{ managed_fetch_status: currentStatus }] };
    if (text.startsWith("SELECT channel_id FROM crawler.channels")) {
      return { rows: params[0].filter((id) => id === EXISTING_CHANNEL).map((channel_id) => ({ channel_id })) };
    }
    if (text.includes("INSERT INTO crawler.channel_candidates")) {
      const specs = JSON.parse(params[0]);
      return {
        rows: specs.map((spec) => ({
          candidate_id: String(nextCandidateId++),
          channel_id: spec.channel_id,
          channel_url: spec.channel_url,
          status: spec.candidate_status,
          priority: spec.priority,
        })),
      };
    }
    if (text.startsWith("UPDATE crawler.channel_candidates SET status='queued'")) {
      return { rows: params[0].map((candidate_id) => ({ candidate_id, snapshot_dispatch_generation: 1 })) };
    }
    if (text.startsWith("SELECT quality_score")) return { rows: [{ quality_score: 80 }] };
    return { rowCount: 0, rows: [] };
  };
  const processor = createDiscoverPageProcessor({
    query,
    queues: {
      "youtube-channel-crawl": {
        addBulk: async (items) => { jobs.bulk.push(...items); },
        add: async (name, data, options) => { jobs.single.push({ name, data, options }); },
      },
    },
    putRawObject: async (value) => { raws.push(value); },
    getCrawlSettingsV2: async () => ({ minSubscriberCount: 1000, discoverStopMinQualifiedRatio: 0.2 }),
    getQueryScheduler: async () => ({ status: "running" }),
    signalReadyDiscoveryPageQualifications: async (value) => { signals.push(value); },
    requestPage: async (input) => {
      requests.push(input);
      return requestPage(input);
    },
    language: "en",
    country: "US",
    demoChannelId: () => "demo0001",
  });
  const find = (prefix) => calls.filter(({ text }) => text.startsWith(prefix) || text.includes(prefix));
  return { processor, calls, jobs, raws, signals, requests, find };
}

const initialPage = (html = searchHtml()) => async (input) => ({
  kind: input.kind,
  url: "https://www.youtube.com/results?search_query=receitas",
  status: 200,
  rawText: html,
  rawContentType: "text/html; charset=utf-8",
});

test("initial page requests one search, stores candidates and queues snapshots", async () => {
  const h = harness({ requestPage: initialPage() });
  const result = await h.processor({ data: { page_id: "page-1" } }, preparedPage());

  assert.deepEqual(h.requests, [{
    kind: "initial", queryText: "receitas", language: "pt", country: "BR", ytConfig: null, continuation: null,
  }]);
  assert.equal(h.raws.length, 1);
  assert.equal(h.raws[0].objectType, "youtube_search_html");
  assert.equal(h.raws[0].metadata.url, "https://www.youtube.com/results?search_query=receitas");

  const events = h.find("INSERT INTO crawler.task_events");
  assert.equal(events.length, 1);
  assert.equal(events[0].params[0], "discover_initial");
  assert.equal(events[0].params[2], "completed");

  const inserted = JSON.parse(h.find("INSERT INTO crawler.channel_candidates")[0].params[0]);
  assert.deepEqual(inserted.map((spec) => [spec.channel_id, spec.candidate_status]).sort(), [
    [EXISTING_CHANNEL, "existing"],
    [NEW_CHANNEL, "discovered"],
  ]);
  assert.equal(h.jobs.bulk.length, 1);
  assert.equal(h.jobs.bulk[0].data.channel_id ?? h.jobs.bulk[0].data.channel?.channel_id, NEW_CHANNEL);

  const pageUpdate = h.find("UPDATE crawler.query_pages SET status = $2")[0];
  assert.equal(pageUpdate.params[1], "running");
  const pageResult = JSON.parse(pageUpdate.params[7]);
  assert.equal(pageResult.next_continuation_token, "next-token");
  assert.equal(pageResult.yt_config.apiKey, "api-key-1");
  assert.deepEqual(pageResult.pending_qualification_channel_ids, [NEW_CHANNEL]);
  assert.deepEqual(h.signals, [{ pageId: "page-1" }]);
  assert.equal(result.candidate_count, 2);
  assert.equal(result.rejected_existing, 1);
  assert.equal(result.snapshot_queued, 1);
});

test("continuation page reuses the stored ytConfig and parses JSON", async () => {
  const ytConfig = { apiKey: "api-key-1", context: { client: { clientName: "WEB" } } };
  const h = harness({
    requestPage: async (input) => ({
      kind: input.kind,
      url: "https://www.youtube.com/youtubei/v1/search?key=api-key-1",
      status: 200,
      rawText: JSON.stringify(searchData([[NEW_CHANNEL, "newowner"]])),
      rawContentType: "application/json; charset=utf-8",
    }),
  });
  await h.processor({ data: { page_id: "page-2" } }, preparedPage({
    pageId: "page-2",
    managedIntent: { continuation_token: "token-1", yt_config: ytConfig },
  }));

  assert.deepEqual(h.requests, [{
    kind: "continuation", queryText: "receitas", language: "pt", country: "BR", ytConfig, continuation: "token-1",
  }]);
  assert.equal(h.raws[0].objectType, "youtube_search_continuation_json");
  // The continuation result keeps its previous shape: no URL is recorded.
  assert.equal(h.raws[0].metadata.url, null);
  const event = h.find("INSERT INTO crawler.task_events")[0];
  assert.equal(event.params[0], "discover_continuation");
  assert.equal(event.params[1], "https://www.youtube.com/results");
  const pageResult = JSON.parse(h.find("UPDATE crawler.query_pages SET status = $2")[0].params[7]);
  assert.deepEqual(pageResult.yt_config, ytConfig);
});

test("request failure marks the page failed, records the network event and rethrows", async () => {
  const failure = new Error("Sign in to confirm you're not a bot");
  const h = harness({ requestPage: async () => { throw failure; } });
  await assert.rejects(h.processor({ data: { page_id: "page-1" } }, preparedPage()), failure);

  const failed = h.find("UPDATE crawler.query_pages SET status = 'failed'");
  assert.equal(failed.length, 1);
  assert.deepEqual(failed[0].params, ["page-1", "Sign in to confirm you're not a bot", "ip_blocked_or_rate_limited"]);
  const event = h.find("INSERT INTO crawler.task_events")[0];
  assert.equal(event.params[0], "discover_fetch");
  assert.equal(event.params[1], "receitas");
  assert.equal(event.params[2], "failed");
  assert.equal(h.find("INSERT INTO crawler.channel_candidates").length, 0);
  assert.equal(h.raws.length, 0);
});

test("parse failure is handled like a failed fetch", async () => {
  const h = harness({ requestPage: initialPage("<html>no initial data</html>") });
  await assert.rejects(h.processor({ data: { page_id: "page-1" } }, preparedPage()), /ytInitialData not found/);
  const failed = h.find("UPDATE crawler.query_pages SET status = 'failed'")[0];
  assert.equal(failed.params[2], "parse");
  assert.equal(h.raws.length, 0);
});

test("a completed page is not fetched again", async () => {
  const h = harness({ claimRows: 0, currentStatus: "done", requestPage: initialPage() });
  const result = await h.processor({ data: { page_id: "page-1" } }, preparedPage());
  assert.deepEqual(result, { ok: true, page_id: "page-1", managed_fetch_complete: true });
  assert.equal(h.requests.length, 0);
});

test("a page that is neither claimable nor done is rejected", async () => {
  const h = harness({ claimRows: 0, currentStatus: "failed", requestPage: initialPage() });
  await assert.rejects(
    h.processor({ data: { page_id: "page-1" } }, preparedPage()),
    /Discover Page Intent is not executable: page-1/,
  );
  assert.equal(h.requests.length, 0);
});

test("mismatched page ids are rejected before any database work", async () => {
  const h = harness({ requestPage: initialPage() });
  await assert.rejects(
    h.processor({ data: { page_id: "other" } }, preparedPage()),
    /conflicts with Page Intent/,
  );
  assert.equal(h.calls.length, 0);
});

test("demo pages dispatch a demo channel without a network request", async () => {
  const h = harness({ requestPage: initialPage() });
  const result = await h.processor({ data: { page_id: "page-1" } }, preparedPage({ managedIntent: { demo: true } }));
  assert.deepEqual(result, { ok: true, page_id: "page-1", demo: true });
  assert.equal(h.requests.length, 0);
  assert.equal(h.find("INSERT INTO crawler.channels")[0].params[0], "UCdemodemo0001");
  assert.equal(h.jobs.single.length, 1);
  assert.deepEqual(h.find("UPDATE crawler.query_pages SET status = 'done'")[0].params, ["page-1", 1, 1, 0]);
});

test("a page without candidates finishes and schedules the next query crawl", async () => {
  const h = harness({ requestPage: initialPage(searchHtml(searchData([]))) });
  const result = await h.processor({ data: { page_id: "page-1" } }, preparedPage());
  const pageUpdate = h.find("UPDATE crawler.query_pages SET status = $2")[0];
  assert.equal(pageUpdate.params[1], "done");
  assert.equal(pageUpdate.params[6], "no_candidates");
  assert.equal(h.find("UPDATE crawler.query_terms").length, 1);
  assert.equal(result.qualification_pending, false);
  assert.equal(h.jobs.bulk.length, 0);
});

test("search request builder matches the local Discover request", () => {
  const initial = discoverSearchPageRequest({ kind: "initial", queryText: "receitas", language: "pt-BR", country: "BR" });
  const url = new URL(initial.url);
  assert.equal(url.searchParams.get("sp"), VIDEO_POPULARITY_THIS_YEAR_FILTER_PARAM);
  assert.equal(url.searchParams.get("hl"), "pt-BR");
  assert.equal(url.searchParams.get("gl"), "BR");
  assert.deepEqual(initial.init, { language: "pt-BR" });

  const ytConfig = { apiKey: "a b", clientName: 1, clientVersion: "2.1", context: { client: { hl: "pt" } } };
  const continuation = discoverSearchPageRequest({ kind: "continuation", ytConfig, continuation: "tok", language: "pt-BR" });
  assert.equal(continuation.url, "https://www.youtube.com/youtubei/v1/search?key=a%20b");
  assert.equal(continuation.init.method, "POST");
  assert.deepEqual(continuation.init.headers, {
    "content-type": "application/json",
    "x-youtube-client-name": "1",
    "x-youtube-client-version": "2.1",
  });
  assert.deepEqual(JSON.parse(continuation.init.body), { context: ytConfig.context, continuation: "tok" });

  assert.throws(() => discoverSearchPageRequest({ kind: "continuation", ytConfig: {}, continuation: "tok" }), /api key missing/);
  assert.throws(() => discoverSearchPageRequest({ kind: "channel" }), /unknown Discover page kind/);
});

test("search page parser returns the previous fetch result shape", () => {
  const initial = parseDiscoverSearchPage({
    kind: "initial", url: "https://www.youtube.com/results", rawText: searchHtml(), rawContentType: "text/html; charset=utf-8",
  });
  assert.deepEqual(Object.keys(initial), ["url", "rawText", "rawContentType", "initialData", "ytConfig"]);
  assert.equal(initial.ytConfig.apiKey, "api-key-1");

  const ytConfig = { apiKey: "k" };
  const continuation = parseDiscoverSearchPage({
    kind: "continuation", rawText: "{\"a\":1}", rawContentType: "application/json; charset=utf-8",
  }, { ytConfig });
  assert.deepEqual(continuation, {
    rawText: "{\"a\":1}", rawContentType: "application/json; charset=utf-8", initialData: { a: 1 }, ytConfig,
  });
  assert.throws(() => parseDiscoverSearchPage({ kind: "other" }), /unknown Discover page kind/);
});
