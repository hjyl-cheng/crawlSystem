import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import test from "node:test";

function runPython(source) {
  const result = spawnSync("python3", ["-c", source], {
    cwd: new URL("..", import.meta.url),
    encoding: "utf8",
  });
  assert.equal(result.status, 0, result.stderr || result.stdout);
  return JSON.parse(result.stdout);
}

test("yt-dlp comment options cap extraction to top-level Top Comments", () => {
  const value = runPython(`
import json
from scripts.ytdlp_comments import bounded_youtube_extractor_args

print(json.dumps(bounded_youtube_extractor_args({"player_client": ["web_safari"]}, 20)))
`);

  assert.deepEqual(value, {
    player_client: ["web_safari"],
    comment_sort: ["top"],
    max_comments: ["20", "20", "0", "0", "1"],
  });
});

test("bounded extraction preserves the original total comment count", () => {
  const value = runPython(`
import json
from scripts.ytdlp_comments import install_original_comment_count_capture, original_comment_count

class FakeYtDlp:
    @staticmethod
    def post_extract(info):
        info["comments"] = [{"id": "Ugw-parent", "parent": "root"}]
        info["comment_count"] = 1

ydl = FakeYtDlp()
install_original_comment_count_capture(ydl)
info = {"id": "video-id", "comment_count": 9000}
ydl.post_extract(info)
print(json.dumps({"total": original_comment_count(info), "extracted": info["comment_count"]}))
`);

  assert.deepEqual(value, { total: 9000, extracted: 1 });
});

test("yt-dlp comments normalize into the first-page contract without requiring an avatar", () => {
  const value = runPython(`
import json
from scripts.ytdlp_comments import normalize_ytdlp_comment_page

comments = [
    {
        "id": "Ugw-parent",
        "parent": "root",
        "text": "First comment",
        "author": "Viewer",
        "author_id": "UCviewer",
        "author_url": "https://www.youtube.com/@viewer",
        "author_thumbnail": None,
        "timestamp": 1787011200,
        "_time_text": "2 days ago",
        "like_count": 18,
        "is_pinned": True,
        "author_is_uploader": False,
        "author_is_verified": True,
        "is_favorited": True,
    },
    {
        "id": "Ugw-reply",
        "parent": "Ugw-parent",
        "text": "Reply must not be stored",
    },
]
page = normalize_ytdlp_comment_page(
    comments,
    total_count=9000,
    collected_at="2026-08-18T12:00:00Z",
)
print(json.dumps(page))
`);

  assert.equal(value.total_count, 9000);
  assert.equal(value.returned_count, 1);
  assert.equal(value.sort, "TOP_COMMENTS");
  assert.equal(value.comments[0].comment_id, "Ugw-parent");
  assert.equal(value.comments[0].author_avatar_url, null);
  assert.equal(value.comments[0].reply_count, null);
  assert.equal(value.comments[0].is_verified, true);
  assert.equal(value.comments[0].is_hearted, true);
});
