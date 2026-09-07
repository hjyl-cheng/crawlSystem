import { isYoutubeJsFullCrawlFetchContract } from "./fullCrawlFetchContract.js";

export function incrementalVideoExecutorMode(environment = process.env) {
  const mode = String(environment.INCREMENTAL_VIDEO_EXECUTOR || "youtubejs_checkpoint_v1").trim();
  if (!["legacy", "youtubejs_checkpoint_v1"].includes(mode)) {
    throw new Error(`unsupported INCREMENTAL_VIDEO_EXECUTOR: ${mode}`);
  }
  return mode;
}

export function channelExtractorCapabilities(prepared, incrementalExecutor) {
  const youtubeOnly = prepared?.workloadKind === "channel_full"
    ? isYoutubeJsFullCrawlFetchContract(prepared.fetchContract)
    : prepared?.workloadKind === "channel_incremental"
      && incrementalExecutor === "youtubejs_checkpoint_v1";
  return { youtubejs: true, ytdlp: !youtubeOnly };
}
