import assert from "node:assert/strict";
import test from "node:test";
import {
  classifyYoutubeCommentPage,
  commentFirstPageNeedsResolution,
  commentPageHasFirstPage,
  commentPageFromDataApiThreads,
  commentPageResolutionStatus,
  commentPageTotalCount,
  confirmedNoVisibleThreadsPage,
  emptyYoutubeCommentPage,
  isYoutubeJsCommentsResult,
  normalizeYoutubeCommentPage,
  youtubeCommentPageFromGetComments,
  youtubeCommentsDisabled,
} from "../src/youtubeCommentPage.js";
import { fetchYoutubeJsCommentsSection } from "../src/youtubeJs.js";

function commentPageFixture({
  countText = "123 Comments",
  comments = [{
    commentId: "Ugxd1",
    commentKey: "entity-1",
    displayName: "Viewer",
    channelId: "UCauthor",
    content: "First comment",
  }],
} = {}) {
  const entities = comments.map((comment) => ({
    key: comment.commentKey,
    properties: {
      commentId: comment.commentId,
      replyLevel: 0,
      publishedTime: "2 days ago",
      content: { content: comment.content },
    },
    author: {
      displayName: comment.displayName,
      channelId: comment.channelId,
      isCreator: false,
    },
    toolbar: {
      likeCountNotliked: "3",
      replyCount: "0",
    },
  }));
  return {
    onResponseReceivedEndpoints: [{
      reloadContinuationItemsCommand: {
        targetId: "comments-section",
        continuationItems: [{
          commentsHeaderRenderer: {
            countText: { simpleText: countText },
          },
        }],
      },
    }, {
      appendContinuationItemsAction: {
        targetId: "comments-section",
        continuationItems: comments.map((comment) => ({
          commentThreadRenderer: {
            commentViewModel: {
              commentViewModel: {
                commentId: comment.commentId,
                commentKey: comment.commentKey,
              },
            },
          },
        })),
      },
    }],
    frameworkUpdates: {
      entityBatchUpdate: {
        mutations: entities.map((entity) => ({
          payload: { commentEntityPayload: entity },
        })),
      },
    },
  };
}

test("an observed comment total is independent of optional first-page rows", () => {
  const page = normalizeYoutubeCommentPage(commentPageFixture({ countText: "17 Comments", comments: [] }));
  assert.equal(page.returned_count, 0);
  const result = classifyYoutubeCommentPage(page);
  assert.equal(result.comment_count, 17);
  assert.equal(result.comment_count_status, "exact");
});

test("optional comments fall back once to the server's newest endpoint when top has no rows", async () => {
  const top = commentPageFixture({ countText: "3 Comments", comments: [] });
  top.onResponseReceivedEndpoints[0].reloadContinuationItemsCommand.continuationItems[0]
    .commentsHeaderRenderer.sortMenu = { sortFilterSubMenuRenderer: { subMenuItems: [
      { selected: true, serviceEndpoint: { continuationCommand: { token: "top-token", request: "CONTINUATION_REQUEST_TYPE_WATCH_NEXT" } } },
      { selected: false, serviceEndpoint: { continuationCommand: { token: "newest-token", request: "CONTINUATION_REQUEST_TYPE_WATCH_NEXT" } } },
    ] } };
  for (const scenario of ["collected", "empty", "failed"]) {
    let calls = 0;
    const client = { actions: { execute: async (path, request) => {
      calls += 1;
      assert.equal(path, "next");
      if (calls === 1) return { success: true, data: top };
      assert.equal(calls, 2);
      assert.equal(request.continuation, "newest-token");
      if (scenario === "failed") throw new Error("comments timeout");
      return { success: true, data: scenario === "empty" ? top : commentPageFixture({ countText: "3 Comments" }) };
    } } };
    const raw = await fetchYoutubeJsCommentsSection(client, "video-id", { fallbackToNewest: true });
    const page = normalizeYoutubeCommentPage(raw);
    assert.equal(calls, 2);
    assert.equal(page.sort, scenario === "failed" ? "TOP_COMMENTS" : "NEWEST_FIRST");
    assert.equal(commentPageHasFirstPage(page), true);
    assert.equal(page.total_count, 3);
    assert.equal(page.returned_count, scenario === "collected" ? 1 : 0);
    assert.equal(classifyYoutubeCommentPage(page).comment_count_status, "exact");
    assert.equal(page.fetch_diagnostics.fallback_status, scenario);
  }
});

test("normalizeYoutubeCommentPage reads the header count and first-page comments from raw JSON", () => {
  const page = normalizeYoutubeCommentPage(commentPageFixture());
  assert.equal(page.version, 1);
  assert.equal(page.sort, "TOP_COMMENTS");
  assert.equal(page.total_count, 123);
  assert.equal(page.returned_count, 1);
  assert.equal(page.comments[0].comment_id, "Ugxd1");
  assert.equal(page.comments[0].text, "First comment");
  assert.equal(commentPageTotalCount(page), 123);
  assert.equal(commentPageHasFirstPage(page), true);
});

test("comment collection bypasses youtubei.js parsing for avatar-less comment entities", async () => {
  const raw = commentPageFixture();
  let getCommentsCalls = 0;
  let executeCalls = 0;
  const client = {
    getComments() {
      getCommentsCalls += 1;
      throw new TypeError("Cannot read properties of undefined (reading 'endpoint')");
    },
    actions: {
      async execute(path, request) {
        executeCalls += 1;
        assert.equal(path, "next");
        assert.equal(typeof request.continuation, "string");
        assert.equal(request.parse, undefined);
        return { success: true, status_code: 200, data: raw };
      },
    },
  };

  const response = await fetchYoutubeJsCommentsSection(client, "video-id");
  const page = normalizeYoutubeCommentPage(response);

  assert.equal(getCommentsCalls, 0);
  assert.equal(executeCalls, 1);
  assert.equal(page.returned_count, 1);
  assert.equal(page.comments[0].comment_id, "Ugxd1");
  assert.equal(page.comments[0].author_avatar_url, null);
});

test("normalizeYoutubeCommentPage keeps a zero first page without youtubei.js parsing", () => {
  const page = normalizeYoutubeCommentPage(commentPageFixture({
    countText: "0 Comments",
    comments: [],
  }));
  assert.equal(page.total_count, 0);
  assert.equal(page.returned_count, 0);
  assert.deepEqual(page.comments, []);
});

test("youtubeCommentPageFromGetComments keeps the stored first-page JSONB shape", () => {
  const page = youtubeCommentPageFromGetComments({
    header: { count: { text: "18 Comments" }, comments_count: { text: "18" } },
    contents: [{
      type: "CommentThread",
      comment: {
        comment_id: "Ugxd1",
        content: { text: "First comment" },
        published_time: "2 days ago",
        like_count: "3",
        reply_count: "0",
        is_pinned: false,
        is_hearted: false,
        author_is_channel_owner: false,
        author: {
          id: "UCauthor",
          name: "Viewer",
          url: "https://www.youtube.com/channel/UCauthor",
          is_verified: false,
          thumbnails: [{ url: "https://yt3.ggpht.com/a", width: 48, height: 48 }],
          best_thumbnail: { url: "https://yt3.ggpht.com/a", width: 48, height: 48 },
        },
      },
    }],
  }, { collectedAt: "2026-08-17T12:00:00.000Z" });
  assert.equal(isYoutubeJsCommentsResult({
    header: { count: { text: "18 Comments" } },
    contents: [{ type: "CommentThread", comment: { comment_id: "Ugxd1" } }],
  }), true);
  assert.equal(page.version, 1);
  assert.equal(page.sort, "TOP_COMMENTS");
  assert.equal(page.total_count, 18);
  assert.equal(page.returned_count, 1);
  assert.equal(page.comments[0].comment_id, "Ugxd1");
  assert.equal(page.comments[0].text, "First comment");
  assert.equal(page.comments[0].author_name, "Viewer");
  assert.equal(page.comments[0].author_channel_id, "UCauthor");
  assert.equal(page.comments[0].author_url, "https://www.youtube.com/channel/UCauthor");
  assert.equal(page.comments[0].author_avatar_url, "https://yt3.ggpht.com/a");
  assert.equal(page.comments[0].like_count, 3);
  assert.equal(page.comments[0].reply_count, 0);
  assert.equal(commentPageHasFirstPage(page), true);
});

test("youtubeCommentsDisabled recognizes an explicit disabled surface", () => {
  assert.equal(youtubeCommentsDisabled({
    contents: { message: "Comments are turned off." },
  }), true);
  assert.equal(youtubeCommentsDisabled({
    contents: { message: "Os comentários estão desativados." },
  }), true);
  assert.equal(youtubeCommentsDisabled(commentPageFixture()), false);
  assert.equal(commentPageHasFirstPage(emptyYoutubeCommentPage({ totalCount: 0 })), true);
});

test("an inactive comment composer disabledText is not a disabled comment surface", () => {
  const raw = commentPageFixture({ countText: "38 Comments" });
  raw.onResponseReceivedEndpoints[0]
    .reloadContinuationItemsCommand
    .continuationItems[0]
    .commentsHeaderRenderer
    .createRenderer = {
      commentSimpleboxRenderer: {
        disabledText: "Comments are turned off.",
      },
    };

  assert.equal(youtubeCommentsDisabled(raw), false);
  const page = normalizeYoutubeCommentPage(raw);
  assert.equal(page.total_count, 38);
  assert.equal(page.returned_count, 1);
  assert.equal(page.comments_disabled, false);
  assert.deepEqual(classifyYoutubeCommentPage(page), {
    comments_disabled: false,
    comment_count: 38,
    comment_count_status: "exact",
    comment_count_source: "youtubejs_comments",
  });

  const zeroCommentRaw = commentPageFixture({ countText: "0 Comments", comments: [] });
  zeroCommentRaw.onResponseReceivedEndpoints[0]
    .reloadContinuationItemsCommand
    .continuationItems[0]
    .commentsHeaderRenderer
    .createRenderer = raw.onResponseReceivedEndpoints[0]
      .reloadContinuationItemsCommand
      .continuationItems[0]
      .commentsHeaderRenderer
      .createRenderer;
  assert.equal(youtubeCommentsDisabled(zeroCommentRaw), false);
  assert.equal(
    classifyYoutubeCommentPage(normalizeYoutubeCommentPage(zeroCommentRaw)).comment_count_status,
    "zero_from_surface",
  );
});

test("classifyYoutubeCommentPage keeps closed, zero and positive totals distinct from body collection", () => {
  assert.deepEqual(classifyYoutubeCommentPage(emptyYoutubeCommentPage({ totalCount: 0 }), {
    disabled: true,
  }), {
    comments_disabled: true,
    comment_count: 0,
    comment_count_status: "disabled",
    comment_count_source: "youtubejs_comments",
  });
  assert.equal(classifyYoutubeCommentPage(normalizeYoutubeCommentPage(commentPageFixture({
    countText: "0 Comments",
    comments: [],
  }))).comment_count_status, "zero_from_surface");
  const failed = classifyYoutubeCommentPage({
    version: 1,
    sort: "TOP_COMMENTS",
    total_count: 12,
    returned_count: 0,
    comments: [],
  });
  assert.equal(failed.comment_count, 12);
  assert.equal(failed.comment_count_status, "exact");
});

test("two-source empty evidence remains unresolved until the official API confirms it", () => {
  const unresolved = emptyYoutubeCommentPage({ totalCount: 23 });
  assert.equal(commentFirstPageNeedsResolution({
    comment_count: 23,
    comments_disabled: false,
    comments_first_page: unresolved,
  }), true);

  const confirmed = confirmedNoVisibleThreadsPage({
    totalCount: 23,
    checkedAt: "2026-08-18T06:00:00.000Z",
    retryAt: "2026-08-25T06:00:00.000Z",
    sources: ["yt_dlp_top_comments", "youtubejs_comments", "youtube_data_api_comment_threads"],
  });
  assert.equal(commentPageResolutionStatus(confirmed), "confirmed_no_visible_threads");
  assert.equal(commentFirstPageNeedsResolution({
    comment_count: 23,
    comments_disabled: false,
    comments_first_page: confirmed,
  }, { now: "2026-08-19T00:00:00.000Z" }), false);
  assert.equal(commentFirstPageNeedsResolution({
    comment_count: 24,
    comments_disabled: false,
    comments_first_page: confirmed,
  }, { now: "2026-08-19T00:00:00.000Z" }), true);
  assert.equal(commentFirstPageNeedsResolution({
    comment_count: 23,
    comments_disabled: false,
    comments_first_page: confirmed,
  }, { now: "2026-08-26T00:00:00.000Z" }), true);
  assert.equal(commentFirstPageNeedsResolution({
    comment_count: 23,
    comments_disabled: false,
    comments_first_page: {
      ...confirmed,
      resolution: {
        ...confirmed.resolution,
        next_retry_at: "not-a-timestamp",
      },
    },
  }, { now: "2026-08-19T00:00:00.000Z" }), true);
});

test("official CommentThreads rows normalize to the first-page contract", () => {
  const page = commentPageFromDataApiThreads({
    items: [{
      id: "thread-1",
      snippet: {
        totalReplyCount: 3,
        topLevelComment: {
          id: "Ugw-comment-1",
          snippet: {
            textOriginal: "First public comment",
            authorDisplayName: "Viewer",
            authorChannelId: { value: "UCviewer" },
            authorChannelUrl: "https://www.youtube.com/channel/UCviewer",
            authorProfileImageUrl: "https://yt3.ggpht.com/avatar",
            publishedAt: "2026-08-17T10:00:00Z",
            updatedAt: "2026-08-17T10:05:00Z",
            likeCount: 18,
          },
        },
      },
    }],
  }, {
    totalCount: 23,
    collectedAt: "2026-08-18T06:00:00.000Z",
  });
  assert.equal(page.returned_count, 1);
  assert.equal(page.total_count, 23);
  assert.equal(page.comments[0].comment_id, "Ugw-comment-1");
  assert.equal(page.comments[0].reply_count, 3);
  assert.equal(page.comments[0].like_count, 18);
  assert.equal(page.comments[0].published_at_status, "exact");
});

test("normalizeYoutubeCommentPage reads commentViewModel threads without commentThreadRenderer", () => {
  const page = normalizeYoutubeCommentPage({
    onResponseReceivedEndpoints: [{
      reloadContinuationItemsCommand: {
        continuationItems: [{
          commentsHeaderRenderer: { countText: { simpleText: "2 Comments" } },
        }, {
          commentViewModel: {
            commentId: "Ugxd1",
            commentKey: "entity-1",
          },
        }],
      },
    }],
    frameworkUpdates: {
      entityBatchUpdate: {
        mutations: [{
          payload: {
            commentEntityPayload: {
              key: "entity-1",
              properties: {
                commentId: "Ugxd1",
                replyLevel: 0,
                publishedTime: "2 days ago",
                content: { content: "First comment" },
              },
              author: { displayName: "Viewer", channelId: "UCauthor" },
              toolbar: { likeCountNotliked: "3", replyCount: "0" },
            },
          },
        }],
      },
    },
  });
  assert.equal(page.total_count, 2);
  assert.equal(page.returned_count, 1);
  assert.equal(page.comments[0].text, "First comment");
});
