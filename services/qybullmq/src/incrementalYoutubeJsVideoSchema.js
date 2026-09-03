const START = "-- incremental-youtubejs-video-checkpoint-schema:start";
const END = "-- incremental-youtubejs-video-checkpoint-schema:end";

export function incrementalYoutubeJsVideoCheckpointSchemaBlock(schema) {
  const source = String(schema ?? "");
  const start = source.indexOf(START);
  const end = source.indexOf(END);
  if (
    start < 0
    || end <= start
    || source.indexOf(START, start + START.length) >= 0
    || source.indexOf(END, end + END.length) >= 0
  ) {
    throw new Error("Incremental YouTubeJS Video checkpoint schema block is missing or duplicated");
  }
  return source.slice(start + START.length, end).trim();
}
