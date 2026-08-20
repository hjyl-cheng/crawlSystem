function normalizedCount(value) {
  if (value === null || value === undefined || value === "") return null;
  const count = Number(value);
  return Number.isFinite(count) && count >= 0 ? Math.round(count) : null;
}

export function evaluateChannelQualification({
  subscriberCount,
  minSubscriberCount,
  required = false,
}) {
  const count = normalizedCount(subscriberCount);
  const minimum = normalizedCount(minSubscriberCount) ?? 0;
  if (!required) {
    return { qualified: true, status: "not_required", reason: null, subscriberCount: count, minSubscriberCount: minimum };
  }
  if (count == null) {
    return {
      qualified: false,
      status: "rejected",
      reason: "subscriber_count_unknown",
      subscriberCount: null,
      minSubscriberCount: minimum,
    };
  }
  if (count < minimum) {
    return {
      qualified: false,
      status: "rejected",
      reason: "subscriber_count_below_minimum",
      subscriberCount: count,
      minSubscriberCount: minimum,
    };
  }
  return { qualified: true, status: "passed", reason: null, subscriberCount: count, minSubscriberCount: minimum };
}

export function evaluateDiscoveryChannelQualification({
  subscriberCount,
  minSubscriberCount,
}) {
  const observedSubscriberCount = normalizedCount(subscriberCount);
  if (observedSubscriberCount == null) {
    return {
      qualified: true,
      status: "needs_snapshot",
      reason: null,
      subscriberCount: null,
      minSubscriberCount: normalizedCount(minSubscriberCount) ?? 0,
      subscriberCountMissing: true,
    };
  }
  const result = evaluateChannelQualification({
    subscriberCount: observedSubscriberCount,
    minSubscriberCount,
    required: true,
  });
  return {
    ...result,
    subscriberCountMissing: observedSubscriberCount == null,
  };
}
