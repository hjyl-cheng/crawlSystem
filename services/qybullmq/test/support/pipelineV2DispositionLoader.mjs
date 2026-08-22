const pipelineUrlSuffix = "/src/pipelineV2.js";
const runtimeMockUrl = new URL("./pipelineV2DispositionRuntimeMock.mjs", import.meta.url).href;
const mockedSpecifiers = new Set([
  "./db.js",
  "./migrationActivityGate.js",
  "./queues.js",
  "./storage.js",
  "./youtube.js",
  "./youtubeJs.js",
]);

export async function resolve(specifier, context, nextResolve) {
  if (context.parentURL?.endsWith(pipelineUrlSuffix) && mockedSpecifiers.has(specifier)) {
    return { url: runtimeMockUrl, shortCircuit: true };
  }
  return nextResolve(specifier, context);
}
