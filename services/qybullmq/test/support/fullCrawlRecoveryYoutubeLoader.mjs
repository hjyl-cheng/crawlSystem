const entrySuffix = "/src/fullCrawlYoutubeJs.js";
const fixtureUrl = new URL("./fullCrawlRecoveryYoutubeFixture.mjs", import.meta.url).href;

export async function resolve(specifier, context, nextResolve) {
  if (context.parentURL?.endsWith(entrySuffix) && specifier === "./youtubeJs.js") {
    return { url: fixtureUrl, shortCircuit: true };
  }
  return nextResolve(specifier, context);
}
