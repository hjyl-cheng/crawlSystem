// Preserve error classification and partial evidence for the existing central retry/API policy.
const ERROR_FIELDS = ['code', 'failureKind', 'failure_kind', 'source', 'status', 'statusCode',
  'targetStatusRaw', 'target_status_raw', 'youtube_failure_evidence', 'partial_detail',
  'playability_kind', 'playability_retry_mode', 'playability_reason_code', 'surface',
  'missing_fields', 'field', 'retry_mode', 'requestId', 'country', 'reason', 'reason_code',
  'required_surface', 'video_id', 'youtube_collection_failure', 'youtube_client_attempts', 'body', 'context'];

export function toChannelWire(value) {
  return JSON.parse(JSON.stringify(value, (_key, item) => {
    if (item instanceof Error) {
      return { __remote_error_v1: true, name: item.name, message: String(item.message).slice(0, 2000),
        ...Object.fromEntries(ERROR_FIELDS.filter((field) => item[field] !== undefined).map((field) => [field, item[field]])),
        ...(item.cause instanceof Error ? { cause: item.cause } : {}),
        ...(item instanceof AggregateError ? { errors: item.errors } : {}) };
    }
    return item;
  }));
}

export function fromChannelWire(value) {
  if (Array.isArray(value)) return value.map(fromChannelWire);
  if (!value || typeof value !== 'object') return value;
  if (value.__remote_error_v1 === true) {
    const error = Array.isArray(value.errors)
      ? new AggregateError(value.errors.map(fromChannelWire), value.message)
      : new Error(value.message);
    error.name = value.name;
    for (const field of ERROR_FIELDS) if (value[field] !== undefined) error[field] = fromChannelWire(value[field]);
    if (value.cause) error.cause = fromChannelWire(value.cause);
    return error;
  }
  return Object.fromEntries(Object.entries(value).map(([key, item]) => [key, fromChannelWire(item)]));
}

export function channelSnapshotWire(snapshot) {
  return toChannelWire({ metadata: snapshot.metadata, about_requested: snapshot.about_requested,
    about_observed: snapshot.about_observed, about_error: snapshot.about_error, raw: snapshot.raw });
}
