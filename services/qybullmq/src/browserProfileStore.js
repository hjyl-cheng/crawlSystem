import {
  createCipheriv,
  createDecipheriv,
  createHash,
  randomBytes,
  randomInt,
} from "node:crypto";
import { nanoid } from "nanoid";
import { query, withTransaction } from "./db.js";

const VISITOR_ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_-";

function encryptionSecret(env = process.env) {
  return String(
    env.BROWSER_PROFILE_ENCRYPTION_KEY
      || env.ROTA_BULLMQ_PROXY_PASSWORD
      || "",
  );
}

function encryptionKey(secret) {
  const value = String(secret || "");
  if (value.length < 12) {
    throw new Error("BROWSER_PROFILE_ENCRYPTION_KEY or ROTA_BULLMQ_PROXY_PASSWORD must contain at least 12 characters");
  }
  return createHash("sha256").update(`qy-browser-profile:${value}`).digest();
}

function positiveInteger(value, field) {
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed <= 0) {
    throw new TypeError(`${field} must be a positive integer`);
  }
  return parsed;
}

export function encryptProfileState(value, secret) {
  if (value === null || value === undefined) return null;
  const iv = randomBytes(12);
  const cipher = createCipheriv("aes-256-gcm", encryptionKey(secret), iv);
  const ciphertext = Buffer.concat([
    cipher.update(JSON.stringify(value), "utf8"),
    cipher.final(),
  ]);
  return [
    "v1",
    iv.toString("base64url"),
    cipher.getAuthTag().toString("base64url"),
    ciphertext.toString("base64url"),
  ].join(":");
}

export function decryptProfileState(payload, secret) {
  if (!payload) return null;
  const [version, ivRaw, tagRaw, ciphertextRaw] = String(payload).split(":");
  if (version !== "v1" || !ivRaw || !tagRaw || !ciphertextRaw) {
    throw new Error("unsupported browser profile ciphertext");
  }
  const decipher = createDecipheriv(
    "aes-256-gcm",
    encryptionKey(secret),
    Buffer.from(ivRaw, "base64url"),
  );
  decipher.setAuthTag(Buffer.from(tagRaw, "base64url"));
  const plaintext = Buffer.concat([
    decipher.update(Buffer.from(ciphertextRaw, "base64url")),
    decipher.final(),
  ]).toString("utf8");
  return JSON.parse(plaintext);
}

function encodeVarint(value) {
  let remaining = Math.max(0, Math.trunc(Number(value) || 0));
  const bytes = [];
  while (remaining > 0x7f) {
    bytes.push((remaining & 0x7f) | 0x80);
    remaining >>>= 7;
  }
  bytes.push(remaining);
  return Buffer.from(bytes);
}

export function generateVisitorData(now = Date.now()) {
  let visitorId = "";
  for (let index = 0; index < 11; index += 1) {
    visitorId += VISITOR_ALPHABET[randomInt(VISITOR_ALPHABET.length)];
  }
  const id = Buffer.from(visitorId, "utf8");
  return Buffer.concat([
    Buffer.from([0x0a, id.length]),
    id,
    Buffer.from([0x28]),
    encodeVarint(Math.floor(now / 1000)),
  ]).toString("base64url");
}

export function newClientProfile(engine, env = process.env) {
  if (engine === "youtubejs_chrome") {
    return {
      engine,
      impersonate_target: String(env.FINGERPRINT_CHROME_TARGET || "chrome136"),
      user_agent: String(
        env.FINGERPRINT_CHROME_USER_AGENT
          || "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
      ),
      visitor_data: generateVisitorData(),
      fingerprint_json: {
        family: "chrome",
        platform: "Windows",
        max_connections: 2,
      },
    };
  }
  if (engine === "ytdlp_safari") {
    return {
      engine,
      impersonate_target: String(env.FINGERPRINT_SAFARI_TARGET || "safari184"),
      user_agent: String(
        env.FINGERPRINT_SAFARI_USER_AGENT
          || "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.4 Safari/605.1.15",
      ),
      visitor_data: generateVisitorData(),
      fingerprint_json: {
        family: "safari",
        platform: "macOS",
        max_connections: 1,
        ytdlp_target: String(env.FINGERPRINT_SAFARI_YTDLP_TARGET || "safari-18.4:macos-15"),
      },
    };
  }
  throw new Error(`unsupported browser profile engine: ${engine}`);
}

function profileId(groupId, engine) {
  return `browser:${engine}:${groupId}:${nanoid(8)}`;
}

function hydrateGroup(group, profiles, secret) {
  const clients = {};
  for (const profile of profiles) {
    clients[profile.engine] = {
      profile_id: profile.profile_id,
      engine: profile.engine,
      impersonate_target: profile.impersonate_target,
      user_agent: profile.user_agent,
      visitor_data: profile.visitor_data,
      fingerprint_json: profile.fingerprint_json || {},
      cookie_state: decryptProfileState(profile.cookie_ciphertext, secret) || { cookies: [] },
      language: group.language,
      country: group.country,
      timezone: group.timezone,
    };
  }
  return {
    profile_group_id: group.profile_group_id,
    proxy_id: group.proxy_id == null ? null : Number(group.proxy_id),
    proxy_address_hash: group.proxy_address_hash ?? null,
    identity_policy_id: group.identity_policy_id ?? null,
    identity_policy_version: group.identity_policy_version == null
      ? null
      : Number(group.identity_policy_version),
    network_identity_key: group.network_identity_key ?? null,
    profile_epoch: group.profile_epoch == null ? null : Number(group.profile_epoch),
    profile_revision: Number(group.profile_revision),
    status: group.status,
    language: group.language,
    country: group.country,
    timezone: group.timezone,
    clients,
  };
}

export class BrowserProfileStore {
  constructor({
    queryFn = query,
    transactionFn = withTransaction,
    secret = encryptionSecret(),
    env = process.env,
  } = {}) {
    this.query = queryFn;
    this.transaction = transactionFn;
    this.secret = secret;
    this.env = env;
    encryptionKey(secret);
  }

  async loadOrCreate({
    proxyId,
    proxyAddressHash,
    identityPolicyId,
    identityPolicyVersion,
    networkIdentityKey,
    profileEpoch,
    language,
    country,
    timezone,
  }) {
    const policyId = String(identityPolicyId ?? "").trim();
    const networkKey = String(networkIdentityKey ?? "").trim();
    const policyVersion = Number(identityPolicyVersion);
    const epoch = Number(profileEpoch);
    const hasV2Identity = Boolean(policyId || networkKey || identityPolicyVersion != null || profileEpoch != null);
    if (hasV2Identity) {
      if (!policyId || !networkKey) throw new Error("identity_policy_id and network_identity_key are required");
      if (!Number.isSafeInteger(policyVersion) || policyVersion <= 0) {
        throw new Error("valid identity_policy_version is required");
      }
      if (!Number.isSafeInteger(epoch) || epoch < 0) throw new Error("valid profile_epoch is required");
      return this.#loadOrCreateV2({
        identityPolicyId: policyId,
        identityPolicyVersion: policyVersion,
        networkIdentityKey: networkKey,
        profileEpoch: epoch,
        language,
        country,
        timezone,
      });
    }

    const numericProxyId = Number(proxyId);
    if (!Number.isInteger(numericProxyId) || numericProxyId <= 0) throw new Error("valid proxy_id is required");
    if (!String(proxyAddressHash || "").trim()) throw new Error("proxy_address_hash is required");

    const result = await this.transaction(async (client) => {
      await client.query("SELECT pg_advisory_xact_lock(hashtext($1))", [`browser-profile:${numericProxyId}`]);
      const activeRows = await client.query(
        `SELECT * FROM crawler.browser_profile_groups
         WHERE proxy_id=$1 AND status='active'
         ORDER BY profile_revision DESC LIMIT 1 FOR UPDATE`,
        [numericProxyId],
      );
      let group = activeRows.rows[0] || null;
      if (group && group.proxy_address_hash !== proxyAddressHash) {
        await client.query(
          `UPDATE crawler.browser_profile_groups
           SET status='retired',retired_at=now(),updated_at=now()
           WHERE profile_group_id=$1`,
          [group.profile_group_id],
        );
        group = null;
      }

      if (!group) {
        const revisionRows = await client.query(
          "SELECT COALESCE(max(profile_revision),0)::int + 1 AS revision FROM crawler.browser_profile_groups WHERE proxy_id=$1",
          [numericProxyId],
        );
        const revision = Number(revisionRows.rows[0]?.revision || 1);
        const groupId = `profile-group:${numericProxyId}:${revision}:${nanoid(8)}`;
        const inserted = await client.query(
          `INSERT INTO crawler.browser_profile_groups (
             profile_group_id,proxy_id,proxy_address_hash,profile_revision,status,
             language,country,timezone,last_used_at
           ) VALUES ($1,$2,$3,$4,'active',$5,$6,$7,now())
           RETURNING *`,
          [groupId, numericProxyId, proxyAddressHash, revision, language, country, timezone],
        );
        group = inserted.rows[0];
        for (const engine of ["youtubejs_chrome", "ytdlp_safari"]) {
          const profile = newClientProfile(engine, this.env);
          await client.query(
            `INSERT INTO crawler.browser_profiles (
               profile_id,profile_group_id,engine,impersonate_target,user_agent,
               visitor_data,fingerprint_json
             ) VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb)`,
            [
              profileId(groupId, engine),
              groupId,
              engine,
              profile.impersonate_target,
              profile.user_agent,
              profile.visitor_data,
              JSON.stringify(profile.fingerprint_json),
            ],
          );
        }
      } else {
        await client.query(
          "UPDATE crawler.browser_profile_groups SET last_used_at=now(),updated_at=now() WHERE profile_group_id=$1",
          [group.profile_group_id],
        );
      }

      const profileRows = await client.query(
        "SELECT * FROM crawler.browser_profiles WHERE profile_group_id=$1 ORDER BY engine",
        [group.profile_group_id],
      );
      return { group, profiles: profileRows.rows };
    });
    return hydrateGroup(result.group, result.profiles, this.secret);
  }

  async #loadOrCreateV2({
    identityPolicyId,
    identityPolicyVersion,
    networkIdentityKey,
    profileEpoch,
    language,
    country,
    timezone,
  }) {
    const lockKey = `browser-profile-v2:${identityPolicyId}:${networkIdentityKey}`;
    const result = await this.transaction(async (client) => {
      await client.query("SELECT pg_advisory_xact_lock(hashtext($1))", [lockKey]);
      const newerEpoch = await client.query(
        `SELECT max(profile_epoch)::int AS profile_epoch
         FROM crawler.browser_profile_groups
         WHERE identity_policy_id=$1 AND network_identity_key=$2`,
        [identityPolicyId, networkIdentityKey],
      );
      if (Number(newerEpoch.rows[0]?.profile_epoch ?? 0) > profileEpoch) {
        throw new Error("profile_epoch cannot move backwards for a network identity");
      }
      const activeRows = await client.query(
        `SELECT * FROM crawler.browser_profile_groups
         WHERE identity_policy_id=$1 AND identity_policy_version=$2
           AND network_identity_key=$3 AND profile_epoch=$4 AND status='active'
         ORDER BY profile_revision DESC LIMIT 1 FOR UPDATE`,
        [identityPolicyId, identityPolicyVersion, networkIdentityKey, profileEpoch],
      );
      let group = activeRows.rows[0] || null;
      if (!group) {
        await client.query(
          `UPDATE crawler.browser_profile_groups
           SET status='retired',retired_at=COALESCE(retired_at,now()),updated_at=now()
           WHERE identity_policy_id=$1 AND network_identity_key=$2
             AND profile_epoch<>$3 AND status<>'retired'`,
          [identityPolicyId, networkIdentityKey, profileEpoch],
        );
        const revisionRows = await client.query(
          `SELECT COALESCE(max(profile_revision),0)::int + 1 AS revision
           FROM crawler.browser_profile_groups
           WHERE identity_policy_id=$1 AND network_identity_key=$2`,
          [identityPolicyId, networkIdentityKey],
        );
        const revision = Number(revisionRows.rows[0]?.revision || 1);
        const groupId = `profile-group:v2:${nanoid(16)}`;
        const inserted = await client.query(
          `INSERT INTO crawler.browser_profile_groups (
             profile_group_id,proxy_id,proxy_address_hash,profile_revision,status,
             language,country,timezone,last_used_at,identity_policy_id,
             identity_policy_version,network_identity_key,profile_epoch
           ) VALUES ($1,NULL,NULL,$2,'active',$3,$4,$5,now(),$6,$7,$8,$9)
           RETURNING *`,
          [
            groupId,
            revision,
            language,
            country,
            timezone,
            identityPolicyId,
            identityPolicyVersion,
            networkIdentityKey,
            profileEpoch,
          ],
        );
        group = inserted.rows[0];
        for (const engine of ["youtubejs_chrome", "ytdlp_safari"]) {
          const profile = newClientProfile(engine, this.env);
          await client.query(
            `INSERT INTO crawler.browser_profiles (
               profile_id,profile_group_id,engine,impersonate_target,user_agent,
               visitor_data,fingerprint_json
             ) VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb)`,
            [
              profileId(groupId, engine),
              groupId,
              engine,
              profile.impersonate_target,
              profile.user_agent,
              profile.visitor_data,
              JSON.stringify(profile.fingerprint_json),
            ],
          );
        }
      } else {
        await client.query(
          "UPDATE crawler.browser_profile_groups SET last_used_at=now(),updated_at=now() WHERE profile_group_id=$1",
          [group.profile_group_id],
        );
      }
      const profileRows = await client.query(
        "SELECT * FROM crawler.browser_profiles WHERE profile_group_id=$1 ORDER BY engine",
        [group.profile_group_id],
      );
      return { group, profiles: profileRows.rows };
    });
    return hydrateGroup(result.group, result.profiles, this.secret);
  }

  async checkpointCookies(profileGroupId, cookieStateByEngine = {}) {
    for (const [engine, cookieState] of Object.entries(cookieStateByEngine)) {
      if (cookieState === undefined) continue;
      await this.query(
        `UPDATE crawler.browser_profiles
         SET cookie_ciphertext=$3,cookie_updated_at=now(),updated_at=now()
         WHERE profile_group_id=$1 AND engine=$2`,
        [profileGroupId, engine, encryptProfileState(cookieState, this.secret)],
      );
    }
  }

  async beginAttempt({
    channelId,
    runId,
    queueName,
    jobId,
    jobAttempt,
    dispatchGeneration,
    workerId,
    proxy,
    profileGroup,
    task = null,
    prepared = null,
  }) {
    const normalizedDispatchGeneration = positiveInteger(
      dispatchGeneration,
      "dispatchGeneration",
    );
    const attemptId = task?.task_id
      ? `channel-attempt:${String(task.task_id)}`
      : `channel-attempt:${channelId}:${Date.now()}:${nanoid(8)}`;
    let validRunId = null;
    if (runId) {
      const run = await this.query(
        "SELECT run_id FROM crawler.channel_runs WHERE run_id=$1 LIMIT 1",
        [String(runId)],
      );
      validRunId = run.rows[0]?.run_id ?? null;
    }
    await this.query(
      `INSERT INTO crawler.channel_execution_attempts (
         attempt_id,channel_id,run_id,queue_name,job_id,job_attempt,dispatch_generation,worker_id,
         slot_name,proxy_user,proxy_id,proxy_address_hash,profile_group_id,
         profile_revision,youtubejs_profile_id,ytdlp_profile_id,
         workload_scope,worker_instance_id,business_run_id,attempt_number,task_id,
         route_generation,network_identity_key,identity_policy_id,identity_policy_version
       ) VALUES (
         $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,
         $16,$17,$18,$19,$20,$21,$22,$23,$24,$25
       )
       ON CONFLICT (attempt_id) DO NOTHING`,
      [
        attemptId,
        channelId,
        validRunId,
        queueName,
        jobId == null ? null : String(jobId),
        Number(jobAttempt || 0),
        normalizedDispatchGeneration,
        workerId,
        proxy.slot_name,
        proxy.proxy_user || proxy.slot_name,
        proxy.proxy_id == null ? null : Number(proxy.proxy_id),
        proxy.proxy_address_hash ?? null,
        profileGroup.profile_group_id,
        profileGroup.profile_revision,
        profileGroup.clients.youtubejs_chrome?.profile_id || null,
        profileGroup.clients.ytdlp_safari?.profile_id || null,
        proxy.workload_scope ?? null,
        proxy.worker_instance_id ?? null,
        prepared?.businessRunId ?? task?.business_run_id ?? null,
        task?.attempt_number == null ? null : Number(task.attempt_number),
        task?.task_id ?? null,
        proxy.route_generation == null ? null : Number(proxy.route_generation),
        proxy.network_identity_key ?? null,
        proxy.identity_policy_id ?? null,
        proxy.identity_policy_version == null ? null : Number(proxy.identity_policy_version),
      ],
    );
    return attemptId;
  }

  async finishAttempt(attemptId, {
    status,
    identityChanged = false,
    error = null,
    result = {},
  }) {
    await this.query(
      `UPDATE crawler.channel_execution_attempts
       SET status=$2,identity_changed=$3,error_class=$4,error_message=$5,
           result_json=$6::jsonb,finished_at=now(),updated_at=now()
       WHERE attempt_id=$1`,
      [
        attemptId,
        status,
        identityChanged,
        error?.name || null,
        error ? String(error?.message || error).slice(0, 2000) : null,
        JSON.stringify(result || {}),
      ],
    );
  }

  async markGroupStatus(profileGroupId, status) {
    await this.query(
      `UPDATE crawler.browser_profile_groups
       SET status=$2,retired_at=CASE WHEN $2='retired' THEN now() ELSE retired_at END,updated_at=now()
       WHERE profile_group_id=$1`,
      [profileGroupId, status],
    );
  }
}

let defaultStore = null;

export function browserProfileStore() {
  if (!defaultStore) defaultStore = new BrowserProfileStore();
  return defaultStore;
}
