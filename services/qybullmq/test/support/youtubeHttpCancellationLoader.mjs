const youtubeUrlSuffix = "/src/youtube.js";
const transportMockUrl = new URL("./youtubeHttpCancellationTransportMock.mjs", import.meta.url).href;

export async function resolve(specifier, context, nextResolve) {
  if (context.parentURL?.endsWith(youtubeUrlSuffix) && specifier === "./fingerprintFetch.js") {
    return { url: transportMockUrl, shortCircuit: true };
  }
  return nextResolve(specifier, context);
}
