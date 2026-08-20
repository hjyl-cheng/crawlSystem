import assert from "node:assert/strict";
import test from "node:test";
import {
  BrowserProfileStore,
  decryptProfileState,
  encryptProfileState,
  generateVisitorData,
  newClientProfile,
} from "../src/browserProfileStore.js";

const SECRET = "test-profile-secret-2026";

function fakeProfileDatabase() {
  const groups = [];
  const profiles = [];
  const client = {
    async query(sql, params = []) {
      if (sql.includes("pg_advisory_xact_lock")) return { rows: [] };
      if (sql.includes("max(profile_epoch)")) {
        const epochs = groups
          .filter((group) => group.identity_policy_id === params[0]
            && group.network_identity_key === params[1])
          .map((group) => group.profile_epoch);
        return { rows: [{ profile_epoch: Math.max(0, ...epochs) || null }] };
      }
      if (sql.includes("FROM crawler.browser_profile_groups") && sql.includes("status='active'")) {
        const v2 = sql.includes("identity_policy_id=$1");
        const rows = groups
          .filter((group) => (v2
            ? group.identity_policy_id === params[0]
              && group.identity_policy_version === params[1]
              && group.network_identity_key === params[2]
              && group.profile_epoch === params[3]
            : group.proxy_id === params[0]) && group.status === "active")
          .sort((left, right) => right.profile_revision - left.profile_revision);
        return { rows: rows.slice(0, 1) };
      }
      if (sql.startsWith("UPDATE crawler.browser_profile_groups") && sql.includes("profile_epoch<>")) {
        for (const group of groups) {
          if (group.identity_policy_id === params[0]
              && group.network_identity_key === params[1]
              && group.profile_epoch !== params[2]
              && group.status !== "retired") group.status = "retired";
        }
        return { rows: [] };
      }
      if (sql.startsWith("UPDATE crawler.browser_profile_groups") && sql.includes("status='retired'")) {
        const group = groups.find((item) => item.profile_group_id === params[0]);
        group.status = "retired";
        return { rows: [] };
      }
      if (sql.includes("COALESCE(max(profile_revision)")) {
        const v2 = sql.includes("identity_policy_id=$1");
        const revisions = groups.filter((group) => (v2
          ? group.identity_policy_id === params[0] && group.network_identity_key === params[1]
          : group.proxy_id === params[0])).map((group) => group.profile_revision);
        return { rows: [{ revision: Math.max(0, ...revisions) + 1 }] };
      }
      if (sql.startsWith("INSERT INTO crawler.browser_profile_groups")) {
        const v2 = sql.includes("VALUES ($1,NULL,NULL");
        const group = v2
          ? {
              profile_group_id: params[0],
              proxy_id: null,
              proxy_address_hash: null,
              profile_revision: params[1],
              language: params[2],
              country: params[3],
              timezone: params[4],
              identity_policy_id: params[5],
              identity_policy_version: params[6],
              network_identity_key: params[7],
              profile_epoch: params[8],
              status: "active",
            }
          : {
              profile_group_id: params[0],
              proxy_id: params[1],
              proxy_address_hash: params[2],
              profile_revision: params[3],
              language: params[4],
              country: params[5],
              timezone: params[6],
              status: "active",
            };
        groups.push(group);
        return { rows: [group] };
      }
      if (sql.startsWith("INSERT INTO crawler.browser_profiles")) {
        const [profile_id, profile_group_id, engine, impersonate_target, user_agent, visitor_data, fingerprint] = params;
        profiles.push({
          profile_id,
          profile_group_id,
          engine,
          impersonate_target,
          user_agent,
          visitor_data,
          fingerprint_json: JSON.parse(fingerprint),
          cookie_ciphertext: null,
        });
        return { rows: [] };
      }
      if (sql.startsWith("UPDATE crawler.browser_profile_groups") && sql.includes("last_used_at")) return { rows: [] };
      if (sql.includes("FROM crawler.browser_profiles")) {
        return { rows: profiles.filter((profile) => profile.profile_group_id === params[0]) };
      }
      throw new Error(`unexpected fake query: ${sql}`);
    },
  };
  return {
    groups,
    profiles,
    transaction: (action) => action(client),
  };
}

test("profile cookie state is authenticated and never stored as plaintext", () => {
  const state = { cookies: [{ name: "VISITOR_INFO1_LIVE", value: "sensitive-cookie" }] };
  const encrypted = encryptProfileState(state, SECRET);

  assert.doesNotMatch(encrypted, /sensitive-cookie/);
  assert.deepEqual(decryptProfileState(encrypted, SECRET), state);
  assert.throws(() => decryptProfileState(encrypted, "different-secret-2026"));
});

test("generated Chrome and Safari profiles carry stable client-family constraints", () => {
  assert.match(generateVisitorData(1_700_000_000_000), /^[A-Za-z0-9_-]+$/);
  assert.deepEqual(newClientProfile("youtubejs_chrome").fingerprint_json, {
    family: "chrome",
    platform: "Windows",
    max_connections: 2,
  });
  assert.deepEqual(newClientProfile("ytdlp_safari").fingerprint_json, {
    family: "safari",
    platform: "macOS",
    max_connections: 1,
    ytdlp_target: "safari-18.4:macos-15",
  });
});

test("one proxy keeps one active profile group and rotates when its address changes", async () => {
  const database = fakeProfileDatabase();
  const store = new BrowserProfileStore({
    transactionFn: database.transaction,
    queryFn: async () => ({ rows: [] }),
    secret: SECRET,
  });
  const locale = { language: "en", country: "BR", timezone: "America/Sao_Paulo" };

  const first = await store.loadOrCreate({ proxyId: 7, proxyAddressHash: "hash-a", ...locale });
  const reused = await store.loadOrCreate({ proxyId: 7, proxyAddressHash: "hash-a", ...locale });
  const rotated = await store.loadOrCreate({ proxyId: 7, proxyAddressHash: "hash-b", ...locale });

  assert.equal(reused.profile_group_id, first.profile_group_id);
  assert.equal(rotated.profile_revision, 2);
  assert.notEqual(rotated.profile_group_id, first.profile_group_id);
  assert.equal(database.groups.filter((group) => group.status === "active").length, 1);
  assert.equal(database.groups.find((group) => group.profile_group_id === first.profile_group_id).status, "retired");
  assert.deepEqual(Object.keys(rotated.clients).sort(), ["youtubejs_chrome", "ytdlp_safari"]);
});

test("Rota v2 profiles are keyed by Policy, anonymous network identity and Profile epoch", async () => {
  const database = fakeProfileDatabase();
  const store = new BrowserProfileStore({
    transactionFn: database.transaction,
    queryFn: async () => ({ rows: [] }),
    secret: SECRET,
  });
  const base = {
    identityPolicyId: "qy-br-channel-anonymous-v1",
    identityPolicyVersion: 1,
    networkIdentityKey: "net-opaque-1",
    language: "pt-BR",
    country: "BR",
    timezone: "America/Sao_Paulo",
  };

  const first = await store.loadOrCreate({ ...base, profileEpoch: 0 });
  const reused = await store.loadOrCreate({ ...base, profileEpoch: 0 });
  const rotated = await store.loadOrCreate({ ...base, profileEpoch: 1 });

  assert.equal(reused.profile_group_id, first.profile_group_id);
  assert.notEqual(rotated.profile_group_id, first.profile_group_id);
  assert.equal(first.proxy_id, null);
  assert.equal(first.profile_epoch, 0);
  assert.equal(rotated.profile_epoch, 1);
  assert.equal(database.groups.find((group) => group.profile_group_id === first.profile_group_id).status, "retired");
  await assert.rejects(store.loadOrCreate({ ...base, profileEpoch: 0 }), /cannot move backwards/);
});

test("a stale channel run id is omitted from a new execution attempt", async () => {
  let insertParams = null;
  const store = new BrowserProfileStore({
    transactionFn: async (action) => action({ query: async () => ({ rows: [] }) }),
    queryFn: async (sql, params = []) => {
      if (sql.includes("SELECT run_id FROM crawler.channel_runs")) return { rows: [] };
      if (sql.includes("INSERT INTO crawler.channel_execution_attempts")) {
        insertParams = params;
        return { rows: [] };
      }
      throw new Error(`unexpected query: ${sql}`);
    },
    secret: SECRET,
  });

  await store.beginAttempt({
    channelId: "UCstale",
    runId: "run:does-not-exist",
    queueName: "youtube-channel-crawl",
    jobId: "job-stale",
    jobAttempt: 1,
    workerId: "qy-channel-01",
    proxy: {
      slot_name: "bullmq-channel-01",
      proxy_user: "bullmq-channel-01",
      proxy_id: 7,
      proxy_address_hash: "hash-a",
    },
    profileGroup: {
      profile_group_id: "group-7-1",
      profile_revision: 1,
      clients: {
        youtubejs_chrome: { profile_id: "chrome-7-1" },
        ytdlp_safari: { profile_id: "safari-7-1" },
      },
    },
  });

  assert.ok(insertParams);
  assert.equal(insertParams[2], null);
});
