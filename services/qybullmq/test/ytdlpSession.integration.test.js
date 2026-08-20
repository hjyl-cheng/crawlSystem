import assert from "node:assert/strict";
import test from "node:test";
import {
  acquirePersistentYtDlp,
  closePersistentYtDlp,
  persistentYtDlpState,
  releasePersistentYtDlp,
} from "../src/ytdlpSession.js";

test("persistent yt-dlp accepts the Safari profile before any channel request", { timeout: 15000 }, async () => {
  const previous = process.env.YTDLP_PERSISTENT_POOL_ENABLED;
  process.env.YTDLP_PERSISTENT_POOL_ENABLED = "true";
  try {
    const lease = await acquirePersistentYtDlp("UCintegration", "en", {
      proxyUrl: "http://127.0.0.1:9",
      profile: {
        profile_id: "integration-safari",
        impersonate_target: "safari184",
        fingerprint_json: { ytdlp_target: "safari-18.4:macos-15" },
        user_agent: "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 Version/18.4 Safari/605.1.15",
        visitor_data: "CgtJbnRlZ3JhdGlvbih7",
        cookie_state: { primary: { cookies: [] }, fallback: { cookies: [] } },
        timezone: "America/Sao_Paulo",
      },
    });

    assert.equal(lease.enabled, true, lease.error);
    assert.equal(lease.mode, "persistent");
    assert.equal(persistentYtDlpState().profile_id, "integration-safari");
    const released = await releasePersistentYtDlp();
    assert.deepEqual(released.cookie_state, {
      primary: { cookies: [] },
      fallback: { cookies: [] },
    });
  } finally {
    await closePersistentYtDlp();
    if (previous === undefined) delete process.env.YTDLP_PERSISTENT_POOL_ENABLED;
    else process.env.YTDLP_PERSISTENT_POOL_ENABLED = previous;
  }
});
