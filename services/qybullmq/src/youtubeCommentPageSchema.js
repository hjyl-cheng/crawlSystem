const START = "-- youtube-comment-first-page-schema:start";
const END = "-- youtube-comment-first-page-schema:end";

export function youtubeCommentFirstPageSchemaBlock(schema) {
  const source = String(schema ?? "");
  const start = source.indexOf(START);
  const end = source.indexOf(END);
  if (start < 0 || end < 0 || end <= start) {
    throw new Error("youtube comment first-page schema block is missing");
  }
  return source.slice(start + START.length, end).trim();
}
