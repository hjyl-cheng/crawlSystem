import assert from "node:assert/strict";
import test from "node:test";
import {
  normalizePublicationLinks,
  publicationLinkTarget,
} from "../src/publicationLinks.js";
import {
  normalizePublicationImageUrl,
  normalizePublicationUrl,
} from "../src/publicationUrl.js";

test("Link target extraction supports the YouTube.js 17 AboutChannelView shape", () => {
  const raw = {
    link: {
      endpoint: {
        metadata: {
          url: "/redirect?q=https%3A%2F%2FExample.com%2Fcontact%3Futm_source%3Dyoutube",
        },
      },
    },
  };
  assert.equal(
    publicationLinkTarget(raw),
    "/redirect?q=https%3A%2F%2FExample.com%2Fcontact%3Futm_source%3Dyoutube",
  );
  assert.equal(
    normalizePublicationLinks([raw]).links[0].target_url,
    "https://example.com/contact",
  );
});

test("command payload wins over metadata and display text never becomes a target", () => {
  const withCommands = normalizePublicationLinks([{
    title: "Official",
    display_url: "display.example",
    link: {
      command_runs: [{
        on_tap: {
          payload: { url: "https://payload.example/path" },
          metadata: { url: "https://metadata.example/path" },
        },
      }],
    },
  }]);
  assert.equal(withCommands.ready, true);
  assert.equal(withCommands.links[0].target_url, "https://payload.example/path");

  const displayOnly = normalizePublicationLinks([{
    title: "Missing target",
    display_url: "example.com",
  }]);
  assert.equal(displayOnly.ready, false);
  assert.equal(displayOnly.valid_count, 0);
  assert.equal(displayOnly.issues[0].code, "channel_link_target_missing");
});

test("URL normalization removes transport noise but retains business query values", () => {
  assert.equal(
    normalizePublicationUrl("https://LastLink.com/app/login?type=cadastrar&utm_source=youtube&fbclid=token#top"),
    "https://lastlink.com/app/login?type=cadastrar",
  );
  assert.equal(
    normalizePublicationImageUrl("https://yt3.googleusercontent.com/avatar?token=temporary"),
    "https://yt3.googleusercontent.com/avatar",
  );
});

test("HTTP URLs containing dotted YouTube handles are not normalized as email addresses", () => {
  const targetUrl = "https://www.youtube.com/@serragg2.0";
  const result = normalizePublicationLinks([{
    title: "YouTube",
    target_url: targetUrl,
  }]);

  assert.equal(
    normalizePublicationUrl(targetUrl, { allowMailto: true }),
    targetUrl,
  );
  assert.equal(result.ready, true);
  assert.equal(result.links[0].target_url, targetUrl);
});

test("historical mailto-wrapped HTTP links recover to their original URL", () => {
  const legacyValue = "mailto:https://www.tiktok.com/@playdance.beelaura";

  assert.equal(
    normalizePublicationUrl(legacyValue, { allowMailto: true }),
    "https://www.tiktok.com/@playdance.beelaura",
  );
});

test("historical mailto-wrapped URLs cannot retain an email type", () => {
  const result = normalizePublicationLinks([{
    target_url: "mailto:https://www.youtube.com/@serragg2.0",
    link_type: "email",
    purpose: "contact",
  }]);

  assert.deepEqual(result.links[0], {
    title: null,
    display_url: null,
    target_url: "https://www.youtube.com/@serragg2.0",
    favicon_url: null,
    position: 0,
    link_type: "youtube",
    purpose: "public_reference",
  });
});

test("Links preserve source order, deduplicate canonical targets, and use Business vocabulary", () => {
  const result = normalizePublicationLinks([
    {
      title: "Twitter",
      target_url: "https://twitter.com/example?utm_campaign=channel",
      position: 4,
    },
    {
      title: "Duplicate",
      target_url: "https://twitter.com/example",
      position: 8,
    },
    {
      title: "Business email",
      target_url: "Creator@Example.com",
      position: 1,
    },
  ]);
  assert.equal(result.ready, true);
  assert.equal(result.valid_count, 2);
  assert.deepEqual(result.links.map((link) => ({
    target_url: link.target_url,
    link_type: link.link_type,
    purpose: link.purpose,
    position: link.position,
  })), [
    {
      target_url: "mailto:creator@example.com",
      link_type: "email",
      purpose: "contact",
      position: 1,
    },
    {
      target_url: "https://twitter.com/example",
      link_type: "x_twitter",
      purpose: "contact",
      position: 4,
    },
  ]);
});

test("untrusted Link labels cannot escape the controlled Business vocabulary", () => {
  const result = normalizePublicationLinks([{
    title: "Official site",
    target_url: "https://example.test/contact",
    link_type: "custom_button",
    purpose: "open_in_new_window",
  }]);

  assert.equal(result.ready, true);
  assert.equal(result.links[0].link_type, "website");
  assert.equal(result.links[0].purpose, "public_reference");
});

test("an unobserved Link surface is different from an explicitly empty one", () => {
  const unknown = normalizePublicationLinks([], { observed: false });
  const empty = normalizePublicationLinks([], { observed: true });
  assert.equal(unknown.ready, false);
  assert.equal(unknown.explicit_empty, false);
  assert.equal(empty.ready, true);
  assert.equal(empty.explicit_empty, true);
});

test("raw command targets outrank a precomputed target and bare link is the final fallback", () => {
  const command = normalizePublicationLinks([{
    target_url: "https://stale.example/",
    link: {
      command_runs: [{
        on_tap: { payload: { url: "https://current.example/contact" } },
      }],
    },
  }]);
  const bare = normalizePublicationLinks([{ link: "example.test/contact" }]);

  assert.equal(command.links[0].target_url, "https://current.example/contact");
  assert.equal(bare.links[0].target_url, "https://example.test/contact");
});
