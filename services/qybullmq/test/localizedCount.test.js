import assert from "node:assert/strict";
import test from "node:test";

import {
  findSubscriberCountText,
  parseLocalizedCount,
  parseRequiredLocalizedCount,
} from "../src/localizedCount.js";
import { isParserContractError } from "../src/localizedParsing.js";
import { extractChannelCandidates, parseChannelHeader, parseCountText } from "../src/youtube.js";
import { parseYoutubeJsCount } from "../src/youtubeJs.js";
import { YOUTUBE_UI_LANGUAGE_CODES } from "../src/youtubeLanguages.js";

const CASES = [
  ["pt-BR", "1,2 mil inscritos", 1_200],
  ["en", "1.2K subscribers", 1_200],
  ["de", "1,2 Mio. Abonnenten", 1_200_000],
  ["ru", "1,2 млн подписчиков", 1_200_000],
  ["ko", "1.2만 명의 구독자", 12_000],
  ["ja", "1.2万人のチャンネル登録者", 12_000],
  ["hi", "1.2 लाख सदस्य", 120_000],
  ["ar", "١٫٢ مليون مشترك", 1_200_000],
  ["fa", "۱٫۲ میلیون مشترک", 1_200_000],
  ["bn", "১.২ লা সাবস্ক্রাইবার", 120_000],
  ["id", "1,2 jt subscriber", 1_200_000],
  ["tr", "1,2 B abone", 1_200],
  ["vi", "1,2 Tr người đăng ký", 1_200_000],
  ["th", "๑.๒M ผู้ติดตาม", 1_200_000],
];

test("localized count parsing follows ICU compact notation across scripts", () => {
  for (const [locale, input, expected] of CASES) {
    assert.equal(parseLocalizedCount(input, { locale }), expected, `${locale}: ${input}`);
    assert.equal(parseCountText(input, locale), expected, `legacy ${locale}: ${input}`);
    assert.equal(parseYoutubeJsCount(input, locale), expected, `youtubejs ${locale}: ${input}`);
  }
});

test("localized count parsing round-trips YouTube market locales", () => {
  const values = [1_234, 12_000, 120_000, 1_200_000, 120_000_000, 1_200_000_000];

  for (const locale of YOUTUBE_UI_LANGUAGE_CODES) {
    const formatter = new Intl.NumberFormat(locale, {
      notation: "compact",
      compactDisplay: "short",
      maximumFractionDigits: 2,
    });
    for (const value of values) {
      const formatted = formatter.format(value);
      const parsed = parseLocalizedCount(formatted, { locale });
      const relativeError = parsed == null ? Infinity : Math.abs(parsed - value) / value;
      assert.ok(relativeError <= 0.011, `${locale}: ${formatted} -> ${parsed}, expected ${value}`);
    }
  }
});

test("localized count parsing distinguishes locale-specific compact symbols", () => {
  assert.equal(parseLocalizedCount("1.2B subscribers", { locale: "en" }), 1_200_000_000);
  assert.equal(parseLocalizedCount("1,2 B abone", { locale: "tr" }), 1_200);
  assert.equal(parseLocalizedCount("1.2M subscribers", { locale: "en" }), 1_200_000);
  assert.equal(parseLocalizedCount("1,2 M subscriber", { locale: "id" }), 1_200_000_000);
});

test("unabbreviated localized counts treat separators as grouping", () => {
  assert.equal(parseLocalizedCount("1,234 subscribers", { locale: "en" }), 1_234);
  assert.equal(parseLocalizedCount("1.234 inscritos", { locale: "pt-BR" }), 1_234);
  assert.equal(parseLocalizedCount("١٬٢٣٤ مشترك", { locale: "ar" }), 1_234);
});

function channelRoot(metadataParts, about = {}) {
  return {
    metadata: {
      channelMetadataRenderer: {
        externalId: "UCglobal",
        title: "Global Channel",
      },
    },
    header: {
      pageHeaderRenderer: {
        content: {
          pageHeaderViewModel: {
            metadata: {
              contentMetadataViewModel: {
                metadataRows: metadataParts.map((content) => ({
                  metadataParts: [{ text: { content } }],
                })),
              },
            },
          },
        },
      },
    },
    about: {
      aboutChannelRenderer: {
        metadata: {
          aboutChannelViewModel: {
            channelId: "UCglobal",
            videoCountText: "8 videos",
            ...about,
          },
        },
      },
    },
  };
}

test("a localized subscriber row is parsed instead of being mistaken for absence", () => {
  const korean = parseChannelHeader(
    channelRoot(["@global", "1.2만 명의 구독자", "8개 동영상"]),
    "ko",
  );
  assert.equal(korean.subscriber_count, 12_000);
  assert.equal(korean.subscriber_count_source, "youtube_channel_header");

  const arabic = parseChannelHeader(
    channelRoot(["@global", "١٬٢٣٤ مشترك", "٨ فيديوهات"]),
    "ar",
  );
  assert.equal(arabic.subscriber_count, 1_234);
});

test("search renderers distinguish legacy video counts from subscriber counts structurally", () => {
  const renderer = (subscriberCountText, videoCountText) => ({
    channelRenderer: {
      channelId: "UCglobal",
      title: { simpleText: "Global Channel" },
      subscriberCountText: { simpleText: subscriberCountText },
      videoCountText: { simpleText: videoCountText },
    },
  });
  const [legacy] = extractChannelCandidates(
    renderer("1.2만 명의 구독자", "8개 동영상"),
    "global",
    null,
    "ko",
  );
  const [current] = extractChannelCandidates(
    renderer("@global", "1.2만 명의 구독자"),
    "global",
    null,
    "ko",
  );
  assert.equal(legacy.subscriber_count, 12_000);
  assert.equal(current.subscriber_count, 12_000);
});

test("a channel video count is never stored as its subscriber count", () => {
  const [candidate] = extractChannelCandidates({
    channelRenderer: {
      channelId: "UCvideosOnly",
      title: { simpleText: "Canal" },
      subscriberCountText: { simpleText: "@canal" },
      videoCountText: { simpleText: "341 vídeos" },
    },
  }, "canal", null, "pt-BR");

  assert.equal(candidate.subscriber_count_text, null);
  assert.equal(candidate.subscriber_count, null);
});

test("only a genuinely absent subscriber row becomes zero after About loads", () => {
  assert.equal(findSubscriberCountText(["@global", "8개 동영상"], { locale: "ko" }), null);
  const parsed = parseChannelHeader(channelRoot(["@global", "8개 동영상"]), "ko");
  assert.equal(parsed.subscriber_count, 0);
  assert.equal(parsed.subscriber_count_source, "youtube_about_missing_subscriber_row");
});

test("a Handle containing subscriber-label text cannot mask the real subscriber count", () => {
  assert.equal(
    findSubscriberCountText(
      ["@Olkabone", "1.35M subscribers", "2K videos"],
      { locale: "en" },
    ),
    "1.35M subscribers",
  );
});

test("a present but unsupported subscriber value fails loudly", () => {
  assert.throws(
    () => parseRequiredLocalizedCount("구독자 수 비공개 형식", {
      locale: "ko",
      field: "subscriber_count",
      source: "youtube_about",
    }),
    (error) => {
      assert.equal(isParserContractError(error), true);
      assert.equal(error.code, "PARSER_CONTRACT_ERROR");
      assert.equal(error.field, "subscriber_count");
      assert.equal(error.locale, "ko");
      assert.equal(error.source, "youtube_about");
      return true;
    },
  );
});

test("a structured About subscriber value cannot silently become zero", () => {
  const root = channelRoot(["@global", "8개 동영상"], {
    subscriberCountText: "구독자 수 비공개 형식",
  });
  assert.throws(
    () => parseChannelHeader(root, "ko"),
    (error) => isParserContractError(error),
  );
});
