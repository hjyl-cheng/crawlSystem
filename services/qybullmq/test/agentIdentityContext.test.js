import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import {
  buildFirstPartyIdentityContext,
  compactFirstPartyIdentity,
} from "../src/agentIdentityContext.js";

const PROMPT_TEMPLATE = readFileSync(new URL("../src/agentPromptTemplate.txt", import.meta.url), "utf8");

test("compactFirstPartyIdentity keeps About, recent video text, and owner comments only", () => {
  const identity = compactFirstPartyIdentity({
    title: "Beleza Ruiva Oficial",
    handle: "@belezaruivaoficial",
    about_description: `${"Olá, muito prazer eu me chamo Aline Castro sou CEO da Beleza Ruiva. ".repeat(20)}`,
    videos: [
      {
        source_content_id: "vid-1",
        title: "A fundadora da Beleza Ruiva trouxe um alerta",
        description: "SOBRE MIM: eu me chamo Aline Castro",
        published_at: "2026-01-02T00:00:00.000Z",
        comments_first_page: {
          comments: [
            { text: "quero essa cor", is_channel_owner: false },
            { text: "eu tenho 32 anos e comecei o canal depois da maternidade", is_channel_owner: true, is_pinned: true },
          ],
        },
      },
    ],
  });
  assert.equal(identity.title, "Beleza Ruiva Oficial");
  assert.equal(identity.handle, "@belezaruivaoficial");
  assert.equal(identity.about.endsWith("…"), true);
  assert.equal(identity.about.includes("Aline Castro"), true);
  assert.equal(identity.recent_videos[0].video_id, "vid-1");
  assert.deepEqual(identity.owner_comments, [{
    text: "eu tenho 32 anos e comecei o canal depois da maternidade",
    is_pinned: true,
  }]);
});

test("first-party identity context is omitted until the crawler supplies it", () => {
  assert.deepEqual(buildFirstPartyIdentityContext([{
    input_url: "https://youtube.com/a",
    first_party_identity: null,
  }]), {});
  assert.deepEqual(buildFirstPartyIdentityContext([{
    input_url: "https://youtube.com/a",
    first_party_identity: {
      title: "Beleza Ruiva Oficial",
      about_description: "Olá, muito prazer eu me chamo Aline Castro sou CEO da Beleza Ruiva.",
    },
  }]), {
    "https://youtube.com/a": {
      title: "Beleza Ruiva Oficial",
      about: "Olá, muito prazer eu me chamo Aline Castro sou CEO da Beleza Ruiva.",
    },
  });
});

test("canonical prompt forbids appearance-based gender and age guesses", () => {
  assert.match(PROMPT_TEMPLATE, /Do not invent those two fields from appearance, voice, a name, or a content niche/);
  assert.match(PROMPT_TEMPLATE, /When the internally correct answer would be unknown, output `brand_team`/);
  assert.match(PROMPT_TEMPLATE, /If there is a single primary creator but no qualifying age evidence, output 35/);
  assert.match(PROMPT_TEMPLATE, /crawler_first_party_identity_context/);
  assert.doesNotMatch(PROMPT_TEMPLATE, /Check the public appearance and voice of the main recurring host/);
  assert.doesNotMatch(PROMPT_TEMPLATE, /Do not use the same default age for every channel with insufficient evidence/);
});
