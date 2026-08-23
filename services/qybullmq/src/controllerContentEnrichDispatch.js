const SUMMARY_ACTION_FIELDS = Object.freeze([
  "enqueued",
  "recovered",
  "existing",
  "released",
  "failed",
]);

function errorMessage(error) {
  return String(error?.message ?? error ?? "unknown Content Enrich dispatch error").slice(0, 2000);
}

export async function dispatchContentEnrichForController({
  dispatcher,
  monitor = null,
  queueCounts = {},
  actions,
  logger = console,
} = {}) {
  if (!dispatcher || typeof dispatcher.dispatchAvailable !== "function") {
    throw new TypeError("a Content Enrich dispatcher is required");
  }
  if (!Array.isArray(actions)) throw new TypeError("actions must be an array");
  let ok = true;
  let summary = null;
  try {
    summary = await dispatcher.dispatchAvailable();
    if (SUMMARY_ACTION_FIELDS.some((field) => Number(summary?.[field] ?? 0) > 0)) {
      actions.push({ action: "dispatch-content-enrich", ...summary });
    }
  } catch (error) {
    ok = false;
    const message = errorMessage(error);
    actions.push({
      action: "dispatch-content-enrich-failed",
      error_message: message,
    });
    logger.error?.(JSON.stringify({
      event: "content_enrich_dispatch_failed",
      error: message,
    }));
  }
  if (!monitor) return { ok, summary };

  let operational = null;
  try {
    operational = await monitor.observe({
      queueCounts,
      dispatchSummary: summary,
      dispatchOk: ok,
    });
    for (const notification of operational?.alerts?.notifications ?? []) {
      const event = { event: "content_enrich_alert", ...notification };
      actions.push({ action: "content-enrich-alert", ...notification });
      if (notification.state === "resolved") logger.log?.(JSON.stringify(event));
      else logger.error?.(JSON.stringify(event));
    }
  } catch (error) {
    const message = errorMessage(error);
    actions.push({
      action: "observe-content-enrich-failed",
      error_message: message,
    });
    logger.error?.(JSON.stringify({
      event: "content_enrich_observation_failed",
      error: message,
    }));
  }
  return { ok, summary, operational };
}
