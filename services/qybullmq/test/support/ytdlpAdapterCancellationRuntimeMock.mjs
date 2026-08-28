async function cancelPersistent() {
  const state = globalThis.__ytdlpAdapterCancellationState;
  if (state.scenario.endsWith("_active_one_shot")) return null;
  state.cancel();
  if (state.scenario.endsWith("_throw")) throw state.signal.reason;
  return null;
}

export async function persistentChannelUploads() {
  return cancelPersistent();
}

export async function persistentVideoDetail() {
  return cancelPersistent();
}

export async function persistentChannelMetadata() {
  return cancelPersistent();
}
