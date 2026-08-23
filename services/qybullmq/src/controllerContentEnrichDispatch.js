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
  actions,
  logger = console,
} = {}) {
  if (!dispatcher || typeof dispatcher.dispatchAvailable !== "function") {
    throw new TypeError("a Content Enrich dispatcher is required");
  }
  if (!Array.isArray(actions)) throw new TypeError("actions must be an array");
  try {
    const summary = await dispatcher.dispatchAvailable();
    if (SUMMARY_ACTION_FIELDS.some((field) => Number(summary?.[field] ?? 0) > 0)) {
      actions.push({ action: "dispatch-content-enrich", ...summary });
    }
    return { ok: true, summary };
  } catch (error) {
    const message = errorMessage(error);
    actions.push({
      action: "dispatch-content-enrich-failed",
      error_message: message,
    });
    logger.error?.(JSON.stringify({
      event: "content_enrich_dispatch_failed",
      error: message,
    }));
    return { ok: false, summary: null };
  }
}
