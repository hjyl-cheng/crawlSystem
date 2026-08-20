import { publicationResultHash } from "./publicationResultHash.js";

const DOMAIN_ORDER = Object.freeze(["channel", "video", "agent"]);
const DOMAIN_SET = new Set(DOMAIN_ORDER);
const HASH_PATTERN = /^sha256:[0-9a-f]{64}$/;
const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

export class PublicationCurrentSeedConflict extends Error {
  constructor(message, details = {}) {
    super(message);
    this.name = "PublicationCurrentSeedConflict";
    this.details = details;
  }
}

function object(value) {
  return value && typeof value === "object" && !Array.isArray(value) ? value : null;
}

function text(value) {
  const output = String(value ?? "").trim();
  return output || null;
}

function positiveInteger(value) {
  const parsed = Number(value);
  return Number.isSafeInteger(parsed) && parsed > 0 ? parsed : null;
}

function timestamp(value) {
  if (value === null || value === undefined || value === "") return null;
  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? null : parsed.toISOString();
}

function candidateObservedAt(candidate) {
  const refs = object(candidate.source_refs) ?? {};
  return timestamp(
    candidate.complete_observed_at
      ?? object(refs.cursor)?.latest_complete_observed_at
      ?? object(refs.complete_observation)?.observed_at
      ?? object(refs.current)?.observed_at,
  );
}

export function normalizePublicationCurrentCandidate(channelId, domain, value) {
  const candidate = object(value);
  if (!candidate) throw new TypeError(`${domain} Current must be an object`);
  const contractVersion = positiveInteger(candidate.contract_version);
  if (!contractVersion) throw new TypeError(`${domain} contract_version must be a positive integer`);
  const policyVersion = text(candidate.policy_version);
  if (!policyVersion) throw new TypeError(`${domain} policy_version is required`);
  const sourceRefs = object(candidate.source_refs);
  if (!sourceRefs) throw new TypeError(`${domain} source_refs must be an object`);
  if (!Array.isArray(candidate.issues) || candidate.issues.some((issue) => !object(issue))) {
    throw new TypeError(`${domain} issues must be an array of objects`);
  }
  const ready = candidate.ready === true;
  if (ready && candidate.issues.length > 0) {
    throw new TypeError(`${domain} Ready Current cannot include readiness issues`);
  }
  if (!ready && candidate.issues.length === 0) {
    throw new TypeError(`${domain} NotReady Current must include at least one issue`);
  }
  const payload = object(candidate.payload);
  const resultHash = text(candidate.result_hash);
  const completeObservedAt = candidateObservedAt(candidate);
  if (ready) {
    if (!payload) throw new TypeError(`${domain} Ready Current requires an object payload`);
    if (text(payload.channel_id) !== channelId) {
      throw new TypeError(`${domain} payload channel_id must match the requested Channel`);
    }
    if (!HASH_PATTERN.test(resultHash ?? "")) {
      throw new TypeError(`${domain} Ready Current requires a canonical result_hash`);
    }
    if (publicationResultHash(domain, payload) !== resultHash) {
      throw new TypeError(`${domain} Ready Current result_hash does not match its payload`);
    }
    if (!completeObservedAt) {
      throw new TypeError(`${domain} Ready Current requires complete_observed_at provenance`);
    }
  }
  return {
    domain,
    contract_version: contractVersion,
    policy_version: policyVersion,
    readiness_status: ready ? "ready" : "not_ready",
    readiness_reasons: ready ? [] : candidate.issues,
    payload_json: ready ? payload : null,
    result_hash: ready ? resultHash : null,
    source_refs: sourceRefs,
    complete_observed_at: ready ? completeObservedAt : null,
  };
}

function normalizeInput(input) {
  const publicationStreamId = text(input?.publicationStreamId);
  if (!UUID_PATTERN.test(publicationStreamId ?? "")) {
    throw new TypeError("publicationStreamId must be a UUID");
  }
  const channelId = text(input?.channelId);
  if (!channelId) throw new TypeError("channelId is required");
  const currents = object(input?.currents);
  if (!currents) throw new TypeError("currents must be an object keyed by Domain");
  const unknown = Object.keys(currents).filter((domain) => !DOMAIN_SET.has(domain));
  if (unknown.length > 0) throw new TypeError(`unsupported Publication Domain: ${unknown.join(", ")}`);
  const domains = DOMAIN_ORDER.filter((domain) => Object.hasOwn(currents, domain));
  if (domains.length === 0) throw new TypeError("at least one Publication Domain Current is required");
  return {
    publicationStreamId,
    channelId,
    candidates: domains.map((domain) => (
      normalizePublicationCurrentCandidate(channelId, domain, currents[domain])
    )),
  };
}

function activeClient(client) {
  if (!client || typeof client.query !== "function") {
    throw new TypeError("an active PostgreSQL client is required");
  }
  return client;
}

function storedRow(row) {
  return {
    domain: row.domain,
    readiness_status: row.readiness_status,
    result_hash: row.result_hash,
    data_sequence: Number(row.data_sequence),
    current_revision_id: row.current_revision_id,
  };
}

export async function seedPublicationCurrents(clientValue, input) {
  const client = activeClient(clientValue);
  const { publicationStreamId, channelId, candidates } = normalizeInput(input);
  await client.query("/* publication-current:transaction-guard */ SAVEPOINT publication_current_seed_guard");
  await client.query("RELEASE SAVEPOINT publication_current_seed_guard");
  const domains = candidates.map((candidate) => candidate.domain);
  const existing = await client.query(
    `/* publication-current:lock-domains */
     SELECT domain,data_sequence,current_revision_id,result_hash
     FROM publication.domain_current
     WHERE publication_stream_id=$1::uuid
       AND channel_id=$2
       AND domain=ANY($3::text[])
     ORDER BY CASE domain WHEN 'channel' THEN 1 WHEN 'video' THEN 2 ELSE 3 END
     FOR UPDATE`,
    [publicationStreamId, channelId, domains],
  );
  const ownership = await client.query(
    `/* publication-current:lock-channel */
     SELECT channel_state.status AS channel_status,
            channel_state.onboarding_mode,
            channel_state.seed_status,
            stream_state.status AS stream_status
     FROM publication.channel_stream_state AS channel_state
     JOIN publication.stream AS stream_state
       ON stream_state.publication_stream_id=channel_state.publication_stream_id
     WHERE channel_state.publication_stream_id=$1::uuid
       AND channel_state.channel_id=$2
     FOR SHARE OF stream_state
     FOR UPDATE OF channel_state`,
    [publicationStreamId, channelId],
  );
  if (ownership.rows.length === 0) {
    return {
      status: "not_owned",
      reason: "channel_stream_state_missing",
      publication_stream_id: publicationStreamId,
      channel_id: channelId,
      domains: [],
    };
  }
  const state = ownership.rows[0];
  if (state.stream_status !== "active" || state.channel_status !== "owned") {
    return {
      status: "not_owned",
      reason: state.stream_status !== "active" ? "stream_sealed" : "channel_stream_sealed",
      publication_stream_id: publicationStreamId,
      channel_id: channelId,
      domains: [],
    };
  }
  if (!new Set(["baseline", "cutover"]).has(state.onboarding_mode)) {
    return {
      status: "not_seedable",
      reason: "bootstrap_requires_revision",
      publication_stream_id: publicationStreamId,
      channel_id: channelId,
      domains: [],
    };
  }

  const online = existing.rows.find((row) => (
    String(row.data_sequence) !== "0" || row.current_revision_id !== null
  ));
  if (online) {
    throw new PublicationCurrentSeedConflict(
      `refusing to seed ${online.domain} Current after online sequencing has started`,
      {
        publication_stream_id: publicationStreamId,
        channel_id: channelId,
        domain: online.domain,
        data_sequence: String(online.data_sequence),
        current_revision_id: online.current_revision_id,
      },
    );
  }
  const existingByDomain = new Map(existing.rows.map((row) => [row.domain, row]));
  const changedSeed = candidates.find((candidate) => {
    const current = existingByDomain.get(candidate.domain);
    return candidate.readiness_status === "ready"
      && current?.result_hash
      && current.result_hash !== candidate.result_hash;
  });
  if (changedSeed) {
    throw new PublicationCurrentSeedConflict(
      `refusing to replace seeded ${changedSeed.domain} Current without a Revision`,
      {
        publication_stream_id: publicationStreamId,
        channel_id: channelId,
        domain: changedSeed.domain,
        current_result_hash: existingByDomain.get(changedSeed.domain).result_hash,
        candidate_result_hash: changedSeed.result_hash,
      },
    );
  }

  const stored = [];
  for (const candidate of candidates) {
    const result = await client.query(
      `/* publication-current:store-domain */
       INSERT INTO publication.domain_current AS current (
         publication_stream_id,channel_id,domain,contract_version,policy_version,
         readiness_status,readiness_reasons,payload_json,result_hash,source_refs,
         complete_observed_at,data_sequence,current_revision_id
       ) VALUES (
         $1::uuid,$2,$3,$4,$5,$6,$7::jsonb,$8::jsonb,$9,$10::jsonb,$11::timestamptz,0,NULL
       )
       ON CONFLICT (publication_stream_id,channel_id,domain) DO UPDATE
       SET contract_version=CASE
             WHEN EXCLUDED.readiness_status='ready' OR current.payload_json IS NULL
               THEN EXCLUDED.contract_version
             ELSE current.contract_version
           END,
           policy_version=CASE
             WHEN EXCLUDED.readiness_status='ready' OR current.payload_json IS NULL
               THEN EXCLUDED.policy_version
             ELSE current.policy_version
           END,
           readiness_status=EXCLUDED.readiness_status,
           readiness_reasons=EXCLUDED.readiness_reasons,
           payload_json=CASE
             WHEN EXCLUDED.readiness_status='ready' THEN EXCLUDED.payload_json
             ELSE current.payload_json
           END,
           result_hash=CASE
             WHEN EXCLUDED.readiness_status='ready' THEN EXCLUDED.result_hash
             ELSE current.result_hash
           END,
           source_refs=CASE
             WHEN EXCLUDED.readiness_status='ready' OR current.payload_json IS NULL
               THEN EXCLUDED.source_refs
             ELSE current.source_refs
           END,
           complete_observed_at=CASE
             WHEN EXCLUDED.readiness_status='ready' OR current.payload_json IS NULL
               THEN EXCLUDED.complete_observed_at
             ELSE current.complete_observed_at
           END,
           updated_at=now()
       WHERE current.data_sequence=0
         AND current.current_revision_id IS NULL
         AND (
           EXCLUDED.readiness_status='not_ready'
           OR current.result_hash IS NULL
           OR current.result_hash=EXCLUDED.result_hash
         )
       RETURNING domain,readiness_status,result_hash,data_sequence,current_revision_id`,
      [
        publicationStreamId,
        channelId,
        candidate.domain,
        candidate.contract_version,
        candidate.policy_version,
        candidate.readiness_status,
        JSON.stringify(candidate.readiness_reasons),
        candidate.payload_json === null ? null : JSON.stringify(candidate.payload_json),
        candidate.result_hash,
        JSON.stringify(candidate.source_refs),
        candidate.complete_observed_at,
      ],
    );
    if (result.rows.length !== 1) {
      throw new PublicationCurrentSeedConflict(
        `concurrent online sequencing prevented ${candidate.domain} Current seed`,
        {
          publication_stream_id: publicationStreamId,
          channel_id: channelId,
          domain: candidate.domain,
        },
      );
    }
    stored.push(storedRow(result.rows[0]));
  }

  const seed = await client.query(
    `/* publication-current:seed-status */
     UPDATE publication.channel_stream_state AS channel_state
     SET seed_status=CASE
           WHEN channel_state.seed_status='complete' OR readiness.seed_complete THEN 'complete'
           ELSE 'pending'
         END,
         seed_completed_at=CASE
           WHEN channel_state.seed_status='complete' OR readiness.seed_complete
             THEN COALESCE(channel_state.seed_completed_at,now())
           ELSE NULL
         END,
         updated_at=now()
     FROM (
       SELECT count(*)=3
          AND count(*) FILTER (
            WHERE payload_json IS NOT NULL AND result_hash IS NOT NULL
          )=3 AS seed_complete
       FROM publication.domain_current
       WHERE publication_stream_id=$1::uuid AND channel_id=$2
     ) AS readiness
     WHERE channel_state.publication_stream_id=$1::uuid
       AND channel_state.channel_id=$2
     RETURNING channel_state.seed_status,channel_state.seed_completed_at`,
    [publicationStreamId, channelId],
  );
  return {
    status: stored.every((row) => row.readiness_status === "ready") ? "ready" : "not_ready",
    publication_stream_id: publicationStreamId,
    channel_id: channelId,
    seed_status: seed.rows[0].seed_status,
    seed_completed_at: seed.rows[0].seed_completed_at,
    domains: stored,
  };
}
