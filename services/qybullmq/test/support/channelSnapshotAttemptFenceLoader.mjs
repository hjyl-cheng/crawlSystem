const pipelineUrlSuffix = "/src/pipelineV2.js";
const runtimeMockUrl = new URL("./channelSnapshotAttemptFenceRuntimeMock.mjs", import.meta.url).href;

export async function resolve(specifier, context, nextResolve) {
  if (
    context.parentURL?.endsWith(pipelineUrlSuffix)
    && ["./youtube.js", "./youtubeJs.js"].includes(specifier)
  ) {
    return { url: runtimeMockUrl, shortCircuit: true };
  }
  return nextResolve(specifier, context);
}
