import assert from "node:assert/strict";
import test from "node:test";
import { channelDetailFromDataApiItem, parseChannelHeader } from "../src/youtube.js";

function channelRoot(metadataParts) {
  return {
    metadata: {
      channelMetadataRenderer: {
        externalId: "UCzero",
        title: "Zero Channel",
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
  };
}

test("a channel header without a subscriber row remains unknown until About loads", () => {
  const parsed = parseChannelHeader(channelRoot(["@zero", "1 video"]));
  assert.equal(parsed.subscriber_count, null);
  assert.equal(parsed.subscriber_count_text, null);
  assert.equal(parsed.subscriber_count_source, null);
});

test("Verified is false only when the PageHeader verification surface was observed", () => {
  const unknown = parseChannelHeader(channelRoot(["@unknown"]));
  assert.equal(unknown.is_verified, null);
  assert.equal(unknown.is_verified_status, "unknown");

  const observedRoot = channelRoot(["@ordinary"]);
  observedRoot.header.pageHeaderRenderer.content.pageHeaderViewModel.title = {
    dynamicTextViewModel: { text: { content: "Ordinary" } },
  };
  const ordinary = parseChannelHeader(observedRoot);
  assert.equal(ordinary.is_verified, false);
  assert.equal(ordinary.is_verified_status, "not_verified");

  observedRoot.header.pageHeaderRenderer.content.pageHeaderViewModel.title
    .dynamicTextViewModel.text.attachmentRuns = [{ startIndex: 8, length: 0 }];
  const verified = parseChannelHeader(observedRoot);
  assert.equal(verified.is_verified, true);
  assert.equal(verified.is_verified_status, "verified");
});

test("a loaded About surface without a subscriber row means zero subscribers", () => {
  const root = channelRoot(["@zero", "1 video"]);
  root.about = {
    aboutChannelRenderer: {
      metadata: {
        aboutChannelViewModel: {
          channelId: "UCzero",
          videoCountText: "1 video",
        },
      },
    },
  };
  const parsed = parseChannelHeader(root);
  assert.equal(parsed.subscriber_count, 0);
  assert.equal(parsed.subscriber_count_text, "0 subscribers");
  assert.equal(parsed.subscriber_count_source, "youtube_about_missing_subscriber_row");
});

test("an unavailable channel header does not turn a network failure into zero", () => {
  const parsed = parseChannelHeader({
    metadata: { channelMetadataRenderer: { externalId: "UCunknown", title: "Unknown" } },
  });
  assert.equal(parsed.subscriber_count, null);
  assert.equal(parsed.subscriber_count_text, null);
  assert.equal(parsed.subscriber_count_source, null);
});

test("an explicit subscriber row keeps its parsed value", () => {
  const parsed = parseChannelHeader(channelRoot(["@known", "1234 inscritos", "8 videos"]));
  assert.equal(parsed.subscriber_count, 1234);
  assert.equal(parsed.subscriber_count_source, "youtube_channel_header");
});

test("channel data API statistics carry an observed subscriber source", () => {
  const parsed = channelDetailFromDataApiItem({
    id: "UCapi",
    snippet: {
      title: "API Channel",
      customUrl: "@api-channel",
      description: "API description",
      thumbnails: { high: { url: "https://example.test/avatar.jpg", width: 800 } },
    },
    statistics: {
      subscriberCount: "2500",
      hiddenSubscriberCount: false,
    },
  });

  assert.equal(parsed.subscriber_count, 2500);
  assert.equal(parsed.subscriber_count_source, "youtube_data_api_channels_list");
  assert.equal(parsed.hidden_subscriber_count, false);
});

test("channel data API preserves an explicit hidden subscriber state", () => {
  const parsed = channelDetailFromDataApiItem({
    id: "UChidden",
    snippet: { title: "Hidden Channel" },
    statistics: { hiddenSubscriberCount: true },
  });

  assert.equal(parsed.subscriber_count, null);
  assert.equal(parsed.subscriber_count_source, null);
  assert.equal(parsed.hidden_subscriber_count, true);
});
