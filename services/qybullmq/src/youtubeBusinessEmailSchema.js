const START_MARKER = "-- youtube-business-email-crawler-schema:start";
const END_MARKER = "-- youtube-business-email-crawler-schema:end";

export function youtubeBusinessEmailCrawlerSchemaBlock(schemaValue) {
  const schema = String(schemaValue ?? "");
  const start = schema.indexOf(START_MARKER);
  const end = schema.indexOf(END_MARKER);
  if (start < 0 || end <= start
      || schema.indexOf(START_MARKER, start + START_MARKER.length) >= 0
      || schema.indexOf(END_MARKER, end + END_MARKER.length) >= 0) {
    throw new Error("YouTube business email crawler schema markers are missing or duplicated");
  }
  return schema.slice(start + START_MARKER.length, end).trim();
}
