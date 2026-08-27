export function canonicalJsonValue(value) {
  if (Array.isArray(value)) return value.map(canonicalJsonValue);
  if (!value || typeof value !== "object") return value;
  return Object.fromEntries(
    Object.keys(value).sort().map((key) => [key, canonicalJsonValue(value[key])]),
  );
}

export function canonicalJsonString(value) {
  return JSON.stringify(canonicalJsonValue(value));
}

export function canonicalJsonEqual(left, right) {
  return canonicalJsonString(left) === canonicalJsonString(right);
}
