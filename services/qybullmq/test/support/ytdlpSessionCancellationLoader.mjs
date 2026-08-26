const sessionUrlSuffix = "/src/ytdlpSession.js";
const childProcessMockUrl = new URL("./ytdlpSessionChildProcessMock.mjs", import.meta.url).href;

export async function resolve(specifier, context, nextResolve) {
  if (context.parentURL?.endsWith(sessionUrlSuffix) && specifier === "node:child_process") {
    return { url: childProcessMockUrl, shortCircuit: true };
  }
  return nextResolve(specifier, context);
}
