import { appendFile, readFile } from "node:fs/promises";
import { fetchYoutubeJsVideoDetail, closeYoutubeJs } from "../src/youtubeJs.js";

// Input is a frozen read-only export of checkpoint Items, never a live queue.
const [inputPath, outputPath] = process.argv.slice(2);
if (!inputPath || !outputPath) throw new Error("input and output NDJSON paths required");
process.env.YOUTUBEJS_EXTRACTOR_MODE = "full";
const rows = (await readFile(inputPath, "utf8")).trim().split("\n").map(JSON.parse);
let previous = [];
try {
  previous = (await readFile(outputPath, "utf8")).trim().split("\n").filter(Boolean).map(JSON.parse);
} catch (error) {
  if (error.code !== "ENOENT") throw error;
}
const completed = new Set(previous.map((row) => row.video_id));
let cursor = 0;
let count = completed.size;
async function worker() {
  while (cursor < rows.length) {
    const row = rows[cursor++];
    if (completed.has(row.video_id)) continue;
    const result = { ...row, observed_at: new Date().toISOString() };
    try {
      result.detail = await fetchYoutubeJsVideoDetail(row.video_id, {
        strictRequiredSurfaces: true,
        signal: AbortSignal.timeout(90000),
      });
      result.status = "captured";
    } catch (error) {
      result.status = "request_failed";
      result.error = { name: error.name, message: error.message, surface: error.required_surface ?? null };
    }
    await appendFile(outputPath, `${JSON.stringify(result)}\n`);
    if (++count % 20 === 0 || count === rows.length) console.log(JSON.stringify({ completed: count, total: rows.length }));
  }
}
try {
  await Promise.all(Array.from({ length: 3 }, worker));
} finally {
  await closeYoutubeJs();
}
