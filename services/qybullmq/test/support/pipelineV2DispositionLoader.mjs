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
  if (
    process.env.QY_PIPELINE_DISPOSITION_SCENARIO === "detail_transport_cancelled"
    && context.parentURL?.endsWith(pipelineUrlSuffix)
    && specifier === "./youtubeJs.js"
  ) {
    return nextResolve(specifier, context);
  }
  if (context.parentURL?.endsWith(pipelineUrlSuffix) && mockedSpecifiers.has(specifier)) {
    return { url: runtimeMockUrl, shortCircuit: true };
  }
  return nextResolve(specifier, context);
}
