const workerUrlSuffix = "/src/worker.js";
const lifecycleUrl = new URL("./migrationRetryIntentPostCommitLifecycle.mjs", import.meta.url).href;
const pipelineUrl = new URL("./migrationRetryIntentPostCommitPipeline.mjs", import.meta.url).href;
const warmupUrl = new URL("./migrationRetryIntentPostCommitWarmup.mjs", import.meta.url).href;

export async function resolve(specifier, context, nextResolve) {
  if (context.parentURL?.endsWith(workerUrlSuffix)) {
    if (specifier === "./channelCandidateWorkerLifecycle.js") {
      return { url: lifecycleUrl, shortCircuit: true };
    }
    if (specifier === "./pipelineV2.js") {
      return { url: pipelineUrl, shortCircuit: true };
    }
    if (["./youtubeJs.js", "./ytdlpSession.js"].includes(specifier)) {
      return { url: warmupUrl, shortCircuit: true };
    }
  }
  return nextResolve(specifier, context);
}
