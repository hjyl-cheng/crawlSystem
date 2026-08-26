const youtubeUrlSuffix = "/src/youtube.js";
const runtimeMockUrl = new URL("./ytdlpAdapterCancellationRuntimeMock.mjs", import.meta.url).href;
const childMockUrl = new URL("./ytdlpAdapterOneShotChildMock.mjs", import.meta.url).href;

export async function resolve(specifier, context, nextResolve) {
  if (context.parentURL?.endsWith(youtubeUrlSuffix) && specifier === "./ytdlpSession.js") {
    return { url: runtimeMockUrl, shortCircuit: true };
  }
  if (context.parentURL?.endsWith(youtubeUrlSuffix) && specifier === "node:child_process") {
    return { url: childMockUrl, shortCircuit: true };
  }
  return nextResolve(specifier, context);
}
