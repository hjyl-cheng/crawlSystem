import assert from "node:assert/strict";
import test from "node:test";
import {
  DEFAULT_AGENT_OUTPUT_SCHEMA,
  DEFAULT_AGENT_PROMPT_TEMPLATE,
} from "../src/agentConfig.js";
import {
  buildFirstPartyIdentityContext,
  buildPrompt,
  chatCompletionBody,
  compactFirstPartyIdentity,
  endpointFor,
  geminiApiKeyHeaders,
  geminiGenerateContentBody,
  normalizeAgentMetrics,
  parseJsonPayload,
  parseAgentResponseBody,
  runAgentBatch,
  validChannelCategories,
} from "../src/llmAgent.js";

function completeAgentRow(inputUrl) {
  return {
    input_url: inputUrl,
    country: "Brazil",
    creator_gender: "brand_team",
    creator_age_range: 0,
    creator_language: "Portuguese",
    audience_region: ["Brazil", "Portugal", "United States", "Mexico", "Spain", "Other"]
      .map((region, index) => ({ region, percentage: index === 0 ? 75 : 5 })),
    audience_age_gender: ["18-24", "25-34", "35-44", "45-54", "55-64", "65+"]
      .map((ageRange) => ({ age_range: ageRange, male: 8, female: 8 })),
    audience_language: [{ language: "Portuguese", percentage: 100 }],
    active_subscriber_ratio: 35,
    channel_tags: {
      tags: ["Kids", "Music", "Songs", "Education", "Family", "Portuguese", "Animation", "Dance", "Play", "Learning"],
      top_5_distribution: ["Kids", "Music", "Songs", "Education", "Family", "Other"]
        .map((tag, index) => ({ tag, percentage: index === 0 ? 50 : 10 })),
    },
    channel_categories: { level_1: "Music", level_2: ["Singing"] },
  };
}

test("mixed country requirements stay in one prompt with per-URL rules", () => {
  const prompt = buildPrompt("Analyze {{input_urls_json}}", [
    { input_url: "https://youtube.com/a", country_required: false, country: "Brazil" },
    { input_url: "https://youtube.com/b", country_required: true },
  ]);
  assert.match(prompt, /country_required_input_urls:[\s\S]*youtube\.com\/b/);
  assert.match(prompt, /country_resolved_input_urls:[\s\S]*youtube\.com\/a/);
  assert.match(prompt, /country_resolved_values:[\s\S]*Brazil/);
  assert.doesNotMatch(prompt, /return \"country\": null/);
});

test("crawler Content identities become Prompt input only when explicitly supplied", () => {
  const prompt = buildPrompt("Analyze {{input_urls_json}}", [{
    input_url: "https://youtube.com/a",
    country_required: true,
    input_content_ids: ["video-02", "video-01", "video-02"],
  }]);

  assert.match(
    prompt,
    /crawler_content_identity_context:[\s\S]*"https:\/\/youtube\.com\/a":[\s\S]*"video-01"[\s\S]*"video-02"/,
  );
  assert.equal(prompt.match(/"video-01"/g)?.length, 1);
  assert.equal(prompt.match(/"video-02"/g)?.length, 1);
});

test("first-party identity is omitted until the crawler supplies it", () => {
  const prompt = buildPrompt("Analyze {{input_urls_json}}", [{
    input_url: "https://youtube.com/a",
    country_required: true,
  }]);
  assert.match(prompt, /crawler_first_party_identity_context: \{\}/);
  assert.deepEqual(buildFirstPartyIdentityContext([{
    input_url: "https://youtube.com/a",
    first_party_identity: null,
  }]), {});
});

test("first-party identity keeps About, recent video text, and owner comments only", () => {
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
  assert.deepEqual(identity.recent_videos[0].video_id, "vid-1");
  assert.deepEqual(identity.owner_comments, [{
    text: "eu tenho 32 anos e comecei o canal depois da maternidade",
    is_pinned: true,
  }]);

  const prompt = buildPrompt("Analyze {{input_urls_json}}", [{
    input_url: "https://youtube.com/a",
    country_required: true,
    first_party_identity: {
      title: "Beleza Ruiva Oficial",
      about_description: "Olá, muito prazer eu me chamo Aline Castro sou CEO da Beleza Ruiva.",
      videos: [],
    },
  }]);
  assert.match(prompt, /crawler_first_party_identity_context:[\s\S]*Aline Castro/);
  assert.match(prompt, /primary evidence for creator_gender and creator_age_range/);
});

test("canonical prompt injects the current batch and contains the new category shape", () => {
  const prompt = buildPrompt(DEFAULT_AGENT_PROMPT_TEMPLATE, [
    { input_url: "https://www.youtube.com/channel/current", country_required: true },
  ]);
  assert.match(prompt, /https:\/\/www\.youtube\.com\/channel\/current/);
  assert.doesNotMatch(prompt, /UC8st6FaRPJcQXCvJk31caQA/);
  assert.doesNotMatch(prompt, /UCFLngIarKm8ES7nwIAnIAYA/);
  assert.doesNotMatch(prompt, /\{\{input_urls_json\}\}/);
  assert.match(prompt, /"level_1": "Fashion"/);
  assert.match(prompt, /"level_1": "Beauty Creators"/);
  assert.match(prompt, /"Portrait Clips"/);
  assert.match(prompt, /"Other Campus Content"/);
  assert.match(prompt, /"level_1": "Gaming"/);
  assert.match(prompt, /"level_1": "Software & Internet"/);
  assert.match(prompt, /creator_gender.*brand_team/s);
  assert.match(prompt, /Do not invent those two fields from appearance, voice, a name, or a content niche/);
  assert.match(prompt, /When the internally correct answer would be unknown, output `brand_team`/);
  assert.match(prompt, /If there is a single primary creator but no qualifying age evidence, output 35/);
  assert.equal(DEFAULT_AGENT_OUTPUT_SCHEMA.items.required.includes("audience_interests"), false);
});

test("missing agent country is never replaced with Brazil", () => {
  const metrics = normalizeAgentMetrics({}, "https://youtube.com/b", { countryRequired: true });
  assert.equal(metrics.audience_profile_agent.country, undefined);
});

test("default audience age and gender distribution totals 100 percent", () => {
  const metrics = normalizeAgentMetrics({}, "https://youtube.com/b", { countryRequired: true });
  assert.equal(
    metrics.audience_profile_agent.audience_age_gender.value
      .reduce((sum, row) => sum + row.male + row.female, 0),
    100,
  );
});

test("channel categories enforce one level-1 branch", () => {
  assert.equal(validChannelCategories({ level_1: "Beauty Creators", level_2: ["Beautiful Women"] }), true);
  assert.equal(validChannelCategories({ level_1: "Casual Vlogs", level_2: ["Portrait Clips"] }), true);
  assert.equal(validChannelCategories({ level_1: "Education", level_2: ["Other Campus Content"] }), true);
  assert.equal(validChannelCategories({ level_1: "Attractive Creators", level_2: ["Beautiful Women"] }), false);
  assert.equal(validChannelCategories({ level_1: "Fashion", level_2: ["Makeup", "Skincare"] }), true);
  assert.equal(validChannelCategories({ level_1: "Fashion", level_2: ["Makeup", "Tech News"] }), false);
  assert.equal(validChannelCategories({ level_1: "Gaming", level_2: ["Action Games", "Mobile Games"] }), true);
  assert.equal(validChannelCategories({ level_1: "Gaming", level_2: ["Tech News"] }), false);
  assert.equal(validChannelCategories({ level_1: "Software & Internet", level_2: ["Artificial Intelligence"] }), true);
});

test("new agent metrics preserve category hierarchy and brand-team gender", () => {
  const metrics = normalizeAgentMetrics({
    country: "Brazil",
    creator_gender: "brand_team",
    creator_age_range: 0,
    channel_categories: { level_1: "Tech", level_2: ["Mobile Tech", "Gadgets & Devices"] },
  }, "https://youtube.com/team");
  const profile = metrics.audience_profile_agent;
  assert.equal(profile.creator_gender.value, "brand_team");
  assert.equal(profile.creator_age_range.value, 0);
  assert.deepEqual(profile.channel_categories.value, {
    level_1: "Tech",
    level_2: ["Mobile Tech", "Gadgets & Devices"],
  });
  assert.equal(profile.audience_interests, undefined);
});

test("chat completions explicitly disable streaming for compatible gateways", () => {
  assert.deepEqual(chatCompletionBody("grok-4.3-console", "hello"), {
    model: "grok-4.3-console",
    messages: [{ role: "user", content: "hello" }],
    temperature: 0.2,
    stream: false,
  });
});

test("ordinary OpenAI-compatible JSON responses remain unchanged", () => {
  const payload = { choices: [{ message: { content: "ok" }, finish_reason: "stop" }] };
  assert.deepEqual(parseAgentResponseBody(JSON.stringify(payload), "application/json"), payload);
});

test("agent parser accepts the first complete JSON array when a gateway appends another value", () => {
  const first = [{ input_url: "https://youtube.com/channel/first", country: "Brazil" }];
  const duplicate = [{ input_url: "https://youtube.com/channel/duplicate", country: "Mexico" }];
  assert.deepEqual(
    parseJsonPayload(`${JSON.stringify(first)}\n${JSON.stringify(duplicate)}`),
    first,
  );
});

test("SSE chat completion deltas are assembled while heartbeats are ignored", () => {
  const body = [
    ": heartbeat",
    "",
    'data: {"choices":[{"delta":{"content":"[1"}}]}',
    "",
    'data: {"choices":[{"delta":{"content":",2]"},"finish_reason":"stop"}]}',
    "",
    "data: [DONE]",
    "",
  ].join("\n");
  assert.equal(
    parseAgentResponseBody(body, "text/event-stream").choices[0].message.content,
    "[1,2]",
  );
});

test("HTTP 200 SSE error events expose an actionable retry status", () => {
  const body = [
    ": heartbeat",
    "",
    "event: error",
    'data: {"error":{"message":"Console API returned 429","type":"upstream_error"}}',
    "",
    "data: [DONE]",
    "",
  ].join("\n");
  assert.throws(
    () => parseAgentResponseBody(body, "text/event-stream"),
    (error) => error.status === 429 && /agent HTTP 429/.test(error.message),
  );
});

test("incomplete Agent output retries within the same batch before failing the Clock request", async () => {
  const inputUrl = "https://www.youtube.com/channel/UCretry";
  let calls = 0;
  const result = await runAgentBatch({
    config: {
      provider: "openai-compatible",
      model: "grok-4.5",
      endpoint: "https://agent.example.test/v1/chat/completions",
      max_retries: 1,
    },
    channels: [{ channel_id: "UCretry", input_url: inputUrl, country_required: true }],
    callAgent: async () => {
      calls += 1;
      const row = completeAgentRow(inputUrl);
      if (calls === 1) delete row.audience_region;
      return JSON.stringify([row]);
    },
    wait: async () => {},
  });

  assert.equal(calls, 2);
  assert.equal(result.errors.length, 0);
  assert.equal(result.results.has("UCretry"), true);
});

test("Agent accepts an omitted country when About already resolved it", async () => {
  const inputUrl = "https://www.youtube.com/channel/UCresolved";
  const result = await runAgentBatch({
    config: {
      provider: "openai-compatible",
      model: "grok-4.5",
      endpoint: "https://agent.example.test/v1/responses",
      max_retries: 0,
    },
    channels: [{
      channel_id: "UCresolved",
      input_url: inputUrl,
      country: "Brazil",
      country_source: "youtube_about",
      country_required: false,
      input_content_ids: ["video-02", "video-01", "video-02"],
    }],
    callAgent: async () => {
      const row = completeAgentRow(inputUrl);
      delete row.country;
      return JSON.stringify([row]);
    },
    wait: async () => {},
  });

  assert.deepEqual(result.errors, []);
  assert.equal(
    result.results.get("UCresolved").metrics.audience_profile_agent.country.value,
    "Brazil",
  );
  assert.deepEqual(result.results.get("UCresolved").input_content_ids, ["video-01", "video-02"]);
});

test("Gemini base URLs become native generateContent endpoints", () => {
  assert.equal(
    endpointFor({
      provider: "gemini",
      model: "gemini-3.5-flash",
      endpoint: "https://aistudio.indexarc.net/v1beta",
    }),
    "https://aistudio.indexarc.net/v1beta/models/gemini-3.5-flash:generateContent",
  );
  assert.equal(
    endpointFor({
      provider: "gemini",
      model: "gemini-3.5-flash",
      endpoint: "https://aistudio.indexarc.net/v1beta/models/gemini-3.5-flash:generateContent",
    }),
    "https://aistudio.indexarc.net/v1beta/models/gemini-3.5-flash:generateContent",
  );
});

test("Gemini requests use x-goog-api-key and Google Search tools", () => {
  assert.deepEqual(geminiApiKeyHeaders("secret"), { "x-goog-api-key": "secret" });
  assert.deepEqual(geminiGenerateContentBody("hello", { googleSearch: {} }), {
    contents: [{ role: "user", parts: [{ text: "hello" }] }],
    tools: [{ googleSearch: {} }],
    generationConfig: { temperature: 0.4 },
  });
});
