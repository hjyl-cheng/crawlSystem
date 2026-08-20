function requiredText(value, field) {
  const normalized = String(value ?? "").trim();
  if (!normalized) throw new TypeError(`${field} is required`);
  return normalized;
}

export async function dispatchManagedQueryQualityBatch({
  qualityBatchId,
  intentStore,
  outboxDispatcher,
  dispatchLimit = 500,
} = {}) {
  const batchId = requiredText(qualityBatchId, "qualityBatchId");
  if (!intentStore || typeof intentStore.prepareQueryQualityBatch !== "function") {
    throw new TypeError("intentStore is required");
  }
  if (!outboxDispatcher || typeof outboxDispatcher.dispatchAvailable !== "function") {
    throw new TypeError("outboxDispatcher is required");
  }
  const prepared = await intentStore.prepareQueryQualityBatch(batchId);
  const dispatch = await outboxDispatcher.dispatchAvailable({ limit: dispatchLimit });
  return {
    status: prepared.chunks.length > 0 ? 201 : 200,
    body: {
      ok: true,
      quality_batch_id: batchId,
      created_chunk_count: prepared.chunks.length,
      quality_chunk_ids: prepared.chunks.map((chunk) => chunk.quality_chunk_id),
      dispatch,
    },
  };
}
