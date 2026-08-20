import assert from "node:assert/strict";
import test from "node:test";
import { ParserContractError } from "../src/localizedParsing.js";
import {
  institutionalPenalty,
  queryScoringErrorResult,
  queryTokens,
  scoreQueryBatch,
} from "../src/queryQuality.js";

test("fallback scoring returns one numeric score for every query", async () => {
  const queries = [
    "maquiagem para pele madura",
    "skincare brasil",
    "tecnologia",
  ];
  const results = await scoreQueryBatch(queries, { fallbackOnly: true, concurrency: 3 });

  assert.equal(results.length, queries.length);
  assert.deepEqual(results.map((result) => result.query), queries);
  for (const result of results) {
    assert.equal(Number.isFinite(result.quality_score), true);
    assert.equal(result.quality_score >= 0 && result.quality_score <= 100, true);
    assert.equal(result.quality_status, "scored_fallback");
    assert.equal(result.signals.fallback, true);
  }
});

test("query tokenization uses locale-aware word boundaries", () => {
  assert.deepEqual(queryTokens("한국 화장품 리뷰", "ko"), ["한국", "화장품", "리뷰"]);
  assert.ok(queryTokens("韩国美妆教程", "zh-CN").length >= 3);
  assert.ok(queryTokens("รีวิวเครื่องสำอางเกาหลี", "th").length >= 3);
});

test("non-space language fallback scores are not treated as one broad token", async () => {
  const [result] = await scoreQueryBatch(["韩国美妆教程"], {
    fallbackOnly: true,
    language: "zh-CN",
  });
  assert.ok(result.signals.specificity_tokens >= 3);
  assert.ok(result.component_scores.specificity >= 99);
});

test("fallback scoring does not award one language a private vocabulary bonus", async () => {
  const [portuguese] = await scoreQueryBatch(["maquiagem pele madura"], {
    fallbackOnly: true,
    language: "pt-BR",
  });
  const [korean] = await scoreQueryBatch(["한국 성숙 피부"], {
    fallbackOnly: true,
    language: "ko",
  });
  assert.equal(portuguese.signals.specificity_tokens, korean.signals.specificity_tokens);
  assert.equal(portuguese.quality_score, korean.quality_score);
});

test("live scoring does not apply a market-specific institution vocabulary", () => {
  assert.equal(institutionalPenalty(
    [{ title: "Notícias Globo" }],
    { samples: [{ title: "Jornal da manhã" }] },
    "pt-BR",
  ), 0);
  assert.equal(institutionalPenalty(
    [{ title: "뉴스 채널" }],
    { samples: [{ title: "오늘의 뉴스" }] },
    "ko",
  ), 0);
});

test("a parser contract error can never receive a fallback quality score", () => {
  const error = new ParserContractError({
    field: "published_age",
    value: "new localized format",
    locale: "en",
    source: "youtube_query_quality_video_renderer",
    reason: "unsupported_localized_relative_time",
  });
  assert.throws(
    () => queryScoringErrorResult("test query", { language: "en" }, error),
    (thrown) => thrown === error,
  );
  assert.equal(
    queryScoringErrorResult("test query", { language: "en" }, new Error("HTTP 429"))
      .quality_status,
    "scored_fallback",
  );
});
