const START_MARKER = "-- publication-current-schema:start";
const END_MARKER = "-- publication-current-schema:end";
const CAPTURE_START_MARKER = "-- publication-capture-schema:start";
const CAPTURE_END_MARKER = "-- publication-capture-schema:end";

function blockBounds(schemaValue, startMarker, endMarker, label) {
  const schema = String(schemaValue ?? "");
  const start = schema.indexOf(startMarker);
  const end = schema.indexOf(endMarker);
  const duplicateStart = schema.indexOf(startMarker, start + startMarker.length);
  const duplicateEnd = schema.indexOf(endMarker, end + endMarker.length);
  if (start < 0 || end <= start || duplicateStart >= 0 || duplicateEnd >= 0) {
    throw new Error(`${label} schema markers are missing, duplicated, or invalid`);
  }
  return { schema, start, end, startMarker, endMarker };
}

export function publicationCurrentSchemaBlock(schemaValue) {
  const { schema, start, end } = blockBounds(
    schemaValue,
    START_MARKER,
    END_MARKER,
    "Publication Current",
  );
  return schema.slice(start + START_MARKER.length, end).trim();
}

export function publicationCaptureSchemaBlock(schemaValue) {
  const { schema, start, end } = blockBounds(
    schemaValue,
    CAPTURE_START_MARKER,
    CAPTURE_END_MARKER,
    "Publication Capture",
  );
  return schema.slice(start + CAPTURE_START_MARKER.length, end).trim();
}

export function crawlerRuntimeSchema(schemaValue) {
  let schema = String(schemaValue ?? "");
  const blocks = [
    blockBounds(schema, START_MARKER, END_MARKER, "Publication Current"),
    blockBounds(schema, CAPTURE_START_MARKER, CAPTURE_END_MARKER, "Publication Capture"),
  ].sort((left, right) => right.start - left.start);
  for (const block of blocks) {
    schema = `${schema.slice(0, block.start)}${schema.slice(block.end + block.endMarker.length)}`;
  }
  return schema;
}
