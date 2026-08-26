export async function fetchWithFingerprint(_engine, _url, init) {
  const state = globalThis.__youtubeHttpCancellationState;
  state.transportSignal = init.signal;
  if (state.scenario === "external_cancel_after_response") {
    state.cancel();
    state.transportAborted = init.signal.aborted;
    return new Response(null, { status: 204 });
  }
  if (state.scenario === "external_cancel") state.cancel();
  return new Promise((resolve, reject) => {
    const onAbort = () => {
      state.transportAborted = true;
      reject(init.signal.reason);
    };
    if (init.signal.aborted) {
      onAbort();
      return;
    }
    init.signal.addEventListener("abort", onAbort, { once: true });
  });
}
