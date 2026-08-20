export const PUBLICATION_WRITER_VERSION = "publication-reconciler-v1";

const ORDERED_VERSION = /^(.*)-v([1-9][0-9]*)$/;

export function publicationWriterVersionSatisfies(actualValue, minimumValue) {
  const actual = String(actualValue ?? "").trim();
  const minimum = String(minimumValue ?? "").trim();
  if (!actual || !minimum) return false;
  if (actual === minimum) return true;

  const actualMatch = actual.match(ORDERED_VERSION);
  const minimumMatch = minimum.match(ORDERED_VERSION);
  return Boolean(
    actualMatch
    && minimumMatch
    && actualMatch[1] === minimumMatch[1]
    && BigInt(actualMatch[2]) >= BigInt(minimumMatch[2]),
  );
}
