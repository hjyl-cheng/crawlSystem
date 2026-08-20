import {
  inspectPublicationInitialPackage,
  reconcilePublication,
} from "./publicationReconciler.js";
import { normalizePublicationGapDomains } from "./publicationGapRepairExecution.js";

const PUBLICATION_DOMAINS = Object.freeze(["channel", "video", "agent"]);
const PUBLICATION_ACTOR = "publication-channel-onboarding-v1";
const PUBLICATION_REASON = "automatic Bootstrap for a new Channel promoted after Publication capture began";
const CHANGE_REVISION_TYPES = new Set(["incremental", "repair"]);
const NON_AUTOMATIC_STREAM_ROLES = new Set(["dead_letter_recovery"]);

export class PublicationChannelOnboardingConflict extends Error {
  constructor(message, details = {}) {
    super(message);
    this.name = "PublicationChannelOnboardingConflict";
    this.details = details;
  }
}

function activeClient(client) {
  if (!client || typeof client.query !== "function") {
    throw new TypeError("an active PostgreSQL client is required");
  }
  return client;
}

function requiredText(value, field) {
  const output = String(value ?? "").trim();
  if (!output) throw new TypeError(`${field} is required`);
  return output;
}

function timestamp(value, field) {
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) throw new TypeError(`${field} must be a timestamp`);
  return parsed.toISOString();
}

function positiveInteger(value, fallback, field, maximum = 1000) {
  if (value == null || String(value).trim() === "") return fallback;
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < 1 || parsed > maximum) {
    throw new TypeError(`${field} must be an integer between 1 and ${maximum}`);
  }
  return parsed;
}

function supportsAutomaticChannelOnboarding(stream) {
  const identity = stream?.source_identity_json;
  if (!identity || typeof identity !== "object" || Array.isArray(identity)) return true;
  return !NON_AUTOMATIC_STREAM_ROLES.has(String(identity.stream_role ?? "").trim());
}

export function publicationGapRepairDomains(initialPackage = {}) {
  const domains = Array.isArray(initialPackage?.domains) ? initialPackage.domains : [];
  return normalizePublicationGapDomains(domains
    .filter((domain) => domain?.readiness_status !== "ready")
    .map((domain) => String(domain?.domain ?? "").trim())
    .filter((domain) => domain === "channel" || domain === "video"));
}

async function recordPublicationGapRepairRequired(client, {
  channelId,
  runId,
  initialPackage,
}) {
  const domains = publicationGapRepairDomains(initialPackage);
  if (domains.length === 0) return { recorded: false, domains: [] };
  const result = await client.query(
    `/* publication-auto-onboarding:record-source-gap */
     UPDATE crawler.channel_runs AS run
     SET result_json=jsonb_set(
           run.result_json,
           '{publication_gap_repair}',
           jsonb_build_object(
             'status','required',
             'reason','quality_policy_refresh',
             'domains',$3::jsonb,
             'readiness_domains',$4::jsonb,
             'detected_at',now()
           ),
           true
         ),
         updated_at=now()
     WHERE run.run_id=$1
       AND run.channel_id=$2
       AND run.crawl_mode='full'
       AND run.status='done'
       AND run.detail_status='done'
       AND run.publication_finalized_status='ready_auto'
       AND run.publication_finalized_at IS NOT NULL
       AND COALESCE(run.result_json#>>'{publication_gap_repair,status}','')<>'required'
       AND EXISTS (
         SELECT 1
         FROM crawler.channels AS channel
         WHERE channel.channel_id=$2
           AND channel.latest_run_id=run.run_id
           AND channel.registry_promotion_run_id=run.run_id
           AND channel.status='active'
           AND channel.agent_status='done'
       )
       AND NOT EXISTS (
         SELECT 1
         FROM publication.channel_stream_state AS owner
         WHERE owner.channel_id=$2
           AND owner.status='owned'
           AND (
             owner.onboarding_mode='bootstrap'
             AND owner.seed_status='pending'
             AND owner.ownership_reference->>'onboarding_mode'='automatic_bootstrap'
             AND owner.ownership_reference->>'initial_full_run_id'=$1
           ) IS NOT TRUE
       )
     RETURNING run.run_id`,
    [
      runId,
      channelId,
      JSON.stringify(domains),
      JSON.stringify(initialPackage?.domains ?? []),
    ],
  );
  return { recorded: result.rowCount === 1, domains };
}

function normalizeFullCrawlInput(input) {
  const revisionType = String(input?.revisionType ?? "incremental").trim();
  if (!CHANGE_REVISION_TYPES.has(revisionType)) {
    throw new TypeError("revisionType must be incremental or repair");
  }
  return {
    channelId: requiredText(input?.channelId, "channelId"),
    runId: input?.runId == null ? null : requiredText(input.runId, "runId"),
    asOf: timestamp(input?.asOf, "asOf"),
    revisionType,
  };
}

async function findOwnedStream(client, channelId) {
  const result = await client.query(
    `/* publication-auto-onboarding:find-owner */
     SELECT channel_state.publication_stream_id,channel_state.onboarding_mode,
            channel_state.seed_status,channel_state.ownership_reference,
            stream_state.status AS stream_status,
            stream_state.capture_enabled_at
     FROM publication.channel_stream_state AS channel_state
     JOIN publication.stream AS stream_state
       ON stream_state.publication_stream_id=channel_state.publication_stream_id
     WHERE channel_state.channel_id=$1 AND channel_state.status='owned'
     ORDER BY channel_state.owned_at,channel_state.publication_stream_id
     FOR UPDATE OF channel_state
     FOR SHARE OF stream_state`,
    [channelId],
  );
  if (result.rows.length > 1) {
    throw new PublicationChannelOnboardingConflict("multiple owned Publication Streams found", {
      channel_id: channelId,
    });
  }
  return result.rows[0] ?? null;
}

function isPublicationGapRepairRun(channel, runId, revisionType) {
  return revisionType === "repair"
    && String(channel?.latest_run_id ?? "") === String(runId ?? "")
    && String(channel?.current_finalized_run_id ?? "") === String(runId ?? "")
    && channel?.current_finalized_status === "ready_auto"
    && Boolean(channel?.current_finalized_at)
    && channel?.current_run_crawl_mode === "full"
    && String(channel?.current_run_repair_parent_id ?? "")
      === String(channel?.initial_full_run_id ?? "")
    && channel?.current_run_repair_mode === "channel"
    && channel?.current_run_publication_gap_status === "required"
    && String(channel?.current_run_publication_gap_root_run_id ?? "")
      === String(channel?.initial_full_run_id ?? "")
    && channel?.promotion_publication_gap_status === "required";
}

async function loadNewChannel(client, channelId, runId, revisionType = "incremental") {
  const result = await client.query(
    `/* publication-auto-onboarding:load-channel */
     SELECT channel.channel_id,channel.status,channel.created_at,channel.latest_run_id,
            channel.agent_status,
            promotion_run.run_id AS initial_full_run_id,
            promotion_run.publication_finalized_status AS initial_full_run_finalized_status,
            promotion_run.publication_finalized_at AS initial_full_run_finalized_at,
            current_finalized.run_id AS current_finalized_run_id,
            current_finalized.status AS current_finalized_status,
            current_finalized.finalized_at AS current_finalized_at,
            current_run.crawl_mode AS current_run_crawl_mode,
            current_run.result_json#>>'{final_repair,parent_run_id}' AS current_run_repair_parent_id,
            current_run.result_json#>>'{final_repair,mode}' AS current_run_repair_mode,
            current_run.result_json#>>'{publication_gap_repair,status}'
              AS current_run_publication_gap_status,
            current_run.result_json#>>'{publication_gap_repair,root_run_id}'
              AS current_run_publication_gap_root_run_id,
            promotion_run.result_json#>>'{publication_gap_repair,status}'
              AS promotion_publication_gap_status,
            promotion_candidate.candidate_id AS initial_candidate_id,
            promotion_candidate.accepted_at AS promotion_accepted_at
     FROM crawler.channels AS channel
     LEFT JOIN crawler.channel_candidates AS promotion_candidate
       ON promotion_candidate.candidate_id=channel.registry_promotion_candidate_id
      AND promotion_candidate.channel_id=channel.channel_id
      AND promotion_candidate.status='accepted'
      AND promotion_candidate.accepted_at IS NOT NULL
     LEFT JOIN crawler.channel_runs AS promotion_run
       ON promotion_run.run_id=channel.registry_promotion_run_id
      AND promotion_run.channel_id=channel.channel_id
      AND promotion_run.candidate_id=promotion_candidate.candidate_id
      AND promotion_run.crawl_mode='full'
     LEFT JOIN crawler.finalized_profiles AS current_finalized
      ON current_finalized.channel_id=channel.channel_id
      AND current_finalized.run_id=channel.latest_run_id
     LEFT JOIN crawler.channel_runs AS current_run
       ON current_run.run_id=channel.latest_run_id
      AND current_run.channel_id=channel.channel_id
     WHERE channel.channel_id=$1
     FOR SHARE OF channel`,
    [channelId],
  );
  const channel = result.rows[0] ?? null;
  if (!channel || channel.status !== "active") {
    return { status: "channel_not_eligible", channel: null };
  }
  if (
    !channel.initial_candidate_id
    || !channel.initial_full_run_id
    || !channel.promotion_accepted_at
  ) {
    return { status: "not_new_channel", channel };
  }
  if (
    runId
    && String(channel.initial_full_run_id) !== runId
    && !isPublicationGapRepairRun(channel, runId, revisionType)
  ) {
    return { status: "not_initial_full_run", channel };
  }
  if (
    channel.initial_full_run_finalized_status !== "ready_auto"
    || !channel.initial_full_run_finalized_at
  ) {
    return { status: "initial_full_run_not_finalized", channel };
  }
  if (
    channel.agent_status !== "done"
    || channel.current_finalized_status !== "ready_auto"
    || !channel.current_finalized_at
  ) {
    return { status: "initial_package_not_ready", channel };
  }
  return { status: "eligible", channel };
}

async function eligibleOnlineStreams(client, channel) {
  const streams = await client.query(
    `/* publication-auto-onboarding:active-streams */
     SELECT publication_stream_id,source_identity_json,capture_enabled_at
     FROM publication.stream
     WHERE status='active' AND capture_enabled_at IS NOT NULL
       AND $1::timestamptz>=capture_enabled_at
     ORDER BY capture_enabled_at,publication_stream_id
     FOR SHARE`,
    [channel.promotion_accepted_at],
  );
  const eligible = [];
  for (const stream of streams.rows) {
    if (!supportsAutomaticChannelOnboarding(stream)) continue;
    const deliveries = await client.query(
      `/* publication-auto-onboarding:online-routes */
       SELECT count(*)::int AS route_count,
              bool_and(delivery.mode='online') AS all_online,
              array_agg(DISTINCT delivery.destination ORDER BY delivery.destination) AS destinations
       FROM publication.channel_delivery_state AS delivery
       JOIN publication.channel_stream_state AS channel_state
         ON channel_state.publication_stream_id=delivery.publication_stream_id
        AND channel_state.channel_id=delivery.channel_id
       WHERE delivery.publication_stream_id=$1::uuid
         AND channel_state.status='owned'`,
      [stream.publication_stream_id],
    );
    const route = deliveries.rows[0] ?? {};
    if (Number(route.route_count ?? 0) === 0 || route.all_online !== true) {
      continue;
    }
    eligible.push({
      publicationStreamId: String(stream.publication_stream_id),
      captureEnabledAt: timestamp(stream.capture_enabled_at, "capture_enabled_at"),
      destinations: (route.destinations ?? []).map((destination) => requiredText(destination, "destination")),
    });
  }
  return eligible;
}

async function ensureSourceDeliveries(client, channelId, stream, ownershipReference) {
  for (const destination of stream.destinations) {
    await client.query(
      `/* publication-auto-onboarding:insert-delivery */
       INSERT INTO publication.channel_delivery_state (
         destination,publication_stream_id,channel_id,mode,source_ownership_reference,
         online_at,state_changed_by,state_reason
       ) VALUES ($1,$2::uuid,$3,'online',$4::jsonb,now(),$5,$6)
       ON CONFLICT DO NOTHING`,
      [
        destination,
        stream.publicationStreamId,
        channelId,
        JSON.stringify(ownershipReference),
        PUBLICATION_ACTOR,
        PUBLICATION_REASON,
      ],
    );
  }
  const deliveries = await client.query(
    `/* publication-auto-onboarding:verify-deliveries */
     SELECT destination,mode
     FROM publication.channel_delivery_state
     WHERE publication_stream_id=$1::uuid AND channel_id=$2
     ORDER BY destination
     FOR SHARE`,
    [stream.publicationStreamId, channelId],
  );
  const actual = new Map(deliveries.rows.map((row) => [String(row.destination), row.mode]));
  if (
    actual.size !== stream.destinations.length
    || stream.destinations.some((destination) => actual.get(destination) !== "online")
  ) {
    throw new PublicationChannelOnboardingConflict("automatic Publication Delivery registration diverged", {
      channel_id: channelId,
      publication_stream_id: stream.publicationStreamId,
      expected_destinations: stream.destinations,
      actual_deliveries: deliveries.rows,
    });
  }
  await client.query(
    `/* publication-auto-onboarding:repair-outbox */
     INSERT INTO publication.outbox (destination,revision_id,status)
     SELECT destination.value,revision.revision_id,'pending'
     FROM unnest($3::text[]) AS destination(value)
     CROSS JOIN publication.revision AS revision
     WHERE revision.publication_stream_id=$1::uuid AND revision.channel_id=$2
     ON CONFLICT (destination,revision_id) DO NOTHING`,
    [stream.publicationStreamId, channelId, stream.destinations],
  );
}

async function registerSourceOwnership(client, channel, stream, runId) {
  const ownershipReference = {
    onboarding_mode: "automatic_bootstrap",
    channel_registry_created_at: timestamp(channel.created_at, "channel.created_at"),
    channel_promotion_accepted_at: timestamp(
      channel.promotion_accepted_at,
      "channel.promotion_accepted_at",
    ),
    initial_full_run_id: requiredText(channel.initial_full_run_id, "channel.initial_full_run_id"),
    initial_candidate_id: Number(channel.initial_candidate_id),
    capture_enabled_at: stream.captureEnabledAt,
    ...(runId ? { bootstrap_source_run_id: runId } : {}),
  };
  const ownership = await client.query(
    `/* publication-auto-onboarding:insert-owner */
     INSERT INTO publication.channel_stream_state (
       publication_stream_id,channel_id,onboarding_mode,ownership_reference,
       state_changed_by,state_reason
     ) VALUES ($1::uuid,$2,'bootstrap',$3::jsonb,$4,$5)
     ON CONFLICT DO NOTHING
     RETURNING publication_stream_id,channel_id`,
    [
      stream.publicationStreamId,
      channel.channel_id,
      JSON.stringify(ownershipReference),
      PUBLICATION_ACTOR,
      PUBLICATION_REASON,
    ],
  );
  const owner = await findOwnedStream(client, channel.channel_id);
  if (!owner || String(owner.publication_stream_id) !== stream.publicationStreamId) {
    throw new PublicationChannelOnboardingConflict(
      "new Channel was concurrently claimed by another Publication Stream",
      {
        channel_id: channel.channel_id,
        expected_publication_stream_id: stream.publicationStreamId,
        actual_publication_stream_id: owner?.publication_stream_id ?? null,
      },
    );
  }

  await ensureSourceDeliveries(client, channel.channel_id, stream, ownershipReference);
  return {
    status: ownership.rowCount === 1 ? "registered" : "existing",
    publication_stream_id: stream.publicationStreamId,
    destinations: stream.destinations,
  };
}

export async function ensureAutomaticPublicationOnboarding(clientValue, inputValue) {
  const client = activeClient(clientValue);
  const channelId = requiredText(inputValue?.channelId, "channelId");
  const runId = inputValue?.runId == null ? null : requiredText(inputValue.runId, "runId");
  const revisionType = String(inputValue?.revisionType ?? "incremental").trim();
  await client.query("/* publication-auto-onboarding:transaction-guard */ SAVEPOINT publication_auto_onboarding_guard");
  await client.query("RELEASE SAVEPOINT publication_auto_onboarding_guard");
  const existing = await findOwnedStream(client, channelId);
  if (existing) {
    if (
      existing.onboarding_mode === "bootstrap"
      && existing.seed_status === "pending"
      && existing.ownership_reference?.onboarding_mode === "automatic_bootstrap"
    ) {
      const loaded = await loadNewChannel(client, channelId, runId, revisionType);
      if (!loaded.channel) {
        return { status: loaded.status, publication_stream_id: null, destinations: [] };
      }
      if (loaded.status !== "eligible") {
        return { status: loaded.status, publication_stream_id: null, destinations: [] };
      }
      const initialPackage = await inspectPublicationInitialPackage(client, {
        channelId,
        asOf: inputValue?.asOf ?? loaded.channel.current_finalized_at,
      });
      if (initialPackage.status !== "ready") {
        return {
          status: "initial_package_not_ready",
          publication_stream_id: null,
          destinations: [],
          domains: initialPackage.domains,
        };
      }
      const streams = await eligibleOnlineStreams(client, loaded.channel);
      const stream = streams.find((candidate) => (
        candidate.publicationStreamId === String(existing.publication_stream_id)
      ));
      if (!stream) {
        return {
          status: "automatic_onboarding_not_online",
          publication_stream_id: null,
          destinations: [],
        };
      }
      if (
        String(existing.ownership_reference?.initial_full_run_id ?? "")
        !== String(loaded.channel.initial_full_run_id)
      ) {
        throw new PublicationChannelOnboardingConflict(
          "incomplete automatic Publication Owner is not bound to the Promotion Run",
          {
            channel_id: channelId,
            publication_stream_id: String(existing.publication_stream_id),
            expected_initial_full_run_id: String(loaded.channel.initial_full_run_id),
            actual_initial_full_run_id: existing.ownership_reference?.initial_full_run_id ?? null,
          },
        );
      }
      await ensureSourceDeliveries(client, channelId, stream, existing.ownership_reference);
      return {
        status: "recovering",
        publication_stream_id: stream.publicationStreamId,
        destinations: stream.destinations,
      };
    }
    return {
      status: "existing",
      publication_stream_id: String(existing.publication_stream_id),
      destinations: null,
    };
  }

  const loaded = await loadNewChannel(client, channelId, runId, revisionType);
  if (!loaded.channel) return { status: loaded.status, publication_stream_id: null, destinations: [] };
  if (loaded.status !== "eligible") {
    return { status: loaded.status, publication_stream_id: null, destinations: [] };
  }
  const streams = await eligibleOnlineStreams(client, loaded.channel);
  if (streams.length === 0) {
    return { status: "automatic_onboarding_not_online", publication_stream_id: null, destinations: [] };
  }
  if (streams.length > 1) {
    throw new PublicationChannelOnboardingConflict(
      "multiple online Publication Streams are eligible to claim a new Channel",
      {
        channel_id: channelId,
        publication_stream_ids: streams.map((stream) => stream.publicationStreamId),
      },
    );
  }
  const initialPackage = await inspectPublicationInitialPackage(client, {
    channelId,
    asOf: inputValue?.asOf ?? loaded.channel.current_finalized_at,
  });
  if (initialPackage.status !== "ready") {
    return {
      status: "initial_package_not_ready",
      publication_stream_id: null,
      destinations: [],
      domains: initialPackage.domains,
    };
  }
  return registerSourceOwnership(client, loaded.channel, streams[0], runId);
}

function assertCompleteAutomaticBootstrap(publication, onboarding) {
  const domainNames = publication.domains.map((domain) => domain.domain);
  const revisionNames = publication.revisions.map((revision) => revision.domain);
  const destinations = onboarding.destinations ?? [];
  const domainsMatch = domainNames.length === PUBLICATION_DOMAINS.length
    && domainNames.every((domain, index) => domain === PUBLICATION_DOMAINS[index]);
  const revisionsMatch = revisionNames.length === PUBLICATION_DOMAINS.length
    && revisionNames.every((domain, index) => domain === PUBLICATION_DOMAINS[index]);
  const complete = publication.status === "revised"
    && publication.seed_status === "complete"
    && domainsMatch
    && publication.domains.every((domain) => domain.status === "revision_created")
    && revisionsMatch
    && publication.revisions.every((revision) => (
      revision.revision_type === "bootstrap"
      && revision.data_sequence === 1
      && revision.previous_data_sequence === null
      && revision.outbox.length === destinations.length
      && revision.outbox.every((outbox, index) => (
        outbox.destination === destinations[index] && outbox.status === "pending"
      ))
    ));
  if (!complete) {
    throw new PublicationChannelOnboardingConflict(
      "automatic Publication Bootstrap did not create one complete Initial Package",
      {
        channel_id: publication.channel_id,
        publication_stream_id: publication.publication_stream_id,
        publication_status: publication.status,
        seed_status: publication.seed_status,
        domains: publication.domains,
        revisions: publication.revisions,
      },
    );
  }
}

async function assertStoredAutomaticBootstrap(client, publication, onboarding) {
  const stored = await client.query(
    `/* publication-auto-onboarding:verify-bootstrap */
     SELECT current.domain,current.data_sequence AS current_sequence,
            current.current_revision_id,current.result_hash AS current_result_hash,
            revision.revision_id,revision.data_sequence,revision.result_hash,
            revision.revision_type,revision.previous_data_sequence,
            revision.previous_result_hash,
            COALESCE(
              array_agg(outbox.destination ORDER BY outbox.destination)
                FILTER (WHERE outbox.destination IS NOT NULL),
              ARRAY[]::text[]
            ) AS destinations
     FROM publication.domain_current AS current
     JOIN publication.revision AS revision
      ON revision.publication_stream_id=current.publication_stream_id
      AND revision.channel_id=current.channel_id
      AND revision.domain=current.domain
      AND revision.data_sequence<=current.data_sequence
     LEFT JOIN publication.outbox AS outbox ON outbox.revision_id=revision.revision_id
     WHERE current.publication_stream_id=$1::uuid AND current.channel_id=$2
       AND current.readiness_status='ready'
       AND current.payload_json IS NOT NULL
       AND current.result_hash IS NOT NULL
     GROUP BY current.domain,current.data_sequence,current.current_revision_id,current.result_hash,
              revision.revision_id,revision.data_sequence,revision.result_hash,
              revision.revision_type,revision.previous_data_sequence,revision.previous_result_hash
     ORDER BY CASE current.domain WHEN 'channel' THEN 1 WHEN 'video' THEN 2 ELSE 3 END,
              revision.data_sequence`,
    [publication.publication_stream_id, publication.channel_id],
  );
  const expectedDestinations = [...(onboarding.destinations ?? [])].sort();
  const byDomain = new Map(PUBLICATION_DOMAINS.map((domain) => [
    domain,
    stored.rows.filter((row) => row.domain === domain),
  ]));
  const complete = publication.seed_status === "complete"
    && PUBLICATION_DOMAINS.every((domain) => {
      const revisions = byDomain.get(domain);
      if (revisions.length === 0) return false;
      const currentSequence = Number(revisions[0].current_sequence);
      if (revisions.length !== currentSequence) return false;
      return revisions.every((row, index) => {
        const previous = revisions[index - 1];
        const sequence = index + 1;
        const isBootstrap = sequence === 1
          && row.revision_type === "bootstrap"
          && row.previous_data_sequence == null
          && row.previous_result_hash == null;
        const isContinuation = sequence > 1
          && row.revision_type !== "bootstrap"
          && Number(row.previous_data_sequence) === sequence - 1
          && row.previous_result_hash === previous.result_hash;
        const isCurrent = sequence !== currentSequence || (
          String(row.revision_id) === String(row.current_revision_id)
          && row.result_hash === row.current_result_hash
        );
        return Number(row.data_sequence) === sequence
          && (isBootstrap || isContinuation)
          && isCurrent
          && JSON.stringify((row.destinations ?? []).map(String).sort())
            === JSON.stringify(expectedDestinations);
      });
    });
  if (!complete) {
    throw new PublicationChannelOnboardingConflict(
      "automatic Publication Bootstrap storage is not one complete Initial Package",
      {
        channel_id: publication.channel_id,
        publication_stream_id: publication.publication_stream_id,
        seed_status: publication.seed_status,
        expected_destinations: expectedDestinations,
        stored_domains: stored.rows,
      },
    );
  }
}

export async function reconcilePublicationAfterFullCrawl(clientValue, inputValue) {
  const client = activeClient(clientValue);
  const input = normalizeFullCrawlInput(inputValue);
  const onboarding = await ensureAutomaticPublicationOnboarding(client, input);
  if (!new Set(["registered", "recovering", "existing"]).has(onboarding.status)) {
    const publicationGapRepair = onboarding.status === "initial_package_not_ready"
      ? await recordPublicationGapRepairRequired(client, {
          channelId: input.channelId,
          runId: input.runId,
          initialPackage: onboarding,
        })
      : { recorded: false, domains: [] };
    return {
      status: "not_owned",
      channel_id: input.channelId,
      domains: [],
      revisions: [],
      onboarding,
      publication_gap_repair: publicationGapRepair,
    };
  }
  const publication = await reconcilePublication(client, {
    channelId: input.channelId,
    domains: PUBLICATION_DOMAINS,
    asOf: input.asOf,
    revisionType: input.revisionType,
  });
  if (onboarding.status === "registered") {
    assertCompleteAutomaticBootstrap(publication, onboarding);
  }
  if (new Set(["registered", "recovering"]).has(onboarding.status)) {
    await assertStoredAutomaticBootstrap(client, publication, onboarding);
  }
  return { ...publication, onboarding };
}

export async function reconcileAutomaticPublicationBacklog({
  query,
  withTransaction,
  limit = 25,
} = {}) {
  if (typeof query !== "function" || typeof withTransaction !== "function") {
    throw new TypeError("query and withTransaction are required");
  }
  const batchLimit = positiveInteger(limit, 25, "limit");
  const candidates = await query(
    `/* publication-auto-onboarding:backlog */
     SELECT channel.channel_id,channel.registry_promotion_run_id AS run_id,
            current_finalized.finalized_at AS publication_as_of
     FROM crawler.channels AS channel
     JOIN crawler.channel_candidates AS promotion_candidate
       ON promotion_candidate.candidate_id=channel.registry_promotion_candidate_id
      AND promotion_candidate.channel_id=channel.channel_id
      AND promotion_candidate.status='accepted'
      AND promotion_candidate.accepted_at IS NOT NULL
     JOIN crawler.channel_runs AS promotion_run
       ON promotion_run.run_id=channel.registry_promotion_run_id
      AND promotion_run.channel_id=channel.channel_id
      AND promotion_run.candidate_id=promotion_candidate.candidate_id
      AND promotion_run.crawl_mode='full'
     JOIN crawler.finalized_profiles AS current_finalized
       ON current_finalized.channel_id=channel.channel_id
      AND current_finalized.run_id=channel.latest_run_id
      AND current_finalized.status='ready_auto'
      AND current_finalized.finalized_at IS NOT NULL
     WHERE channel.status='active'
       AND channel.agent_status='done'
       AND promotion_run.publication_finalized_status='ready_auto'
       AND promotion_run.publication_finalized_at IS NOT NULL
       AND COALESCE(
             promotion_run.result_json#>>'{publication_gap_repair,status}',
             ''
           )<>'required'
       AND NOT EXISTS (
         SELECT 1 FROM publication.channel_stream_state AS owner
         WHERE owner.channel_id=channel.channel_id AND owner.status='owned'
           AND (
             owner.onboarding_mode='bootstrap'
             AND owner.seed_status='pending'
             AND owner.ownership_reference->>'onboarding_mode'='automatic_bootstrap'
             AND owner.ownership_reference->>'initial_full_run_id'
                   =channel.registry_promotion_run_id
           ) IS NOT TRUE
       )
       AND EXISTS (
         SELECT 1
         FROM publication.stream AS stream
         WHERE stream.status='active' AND stream.capture_enabled_at IS NOT NULL
           AND promotion_candidate.accepted_at>=stream.capture_enabled_at
           AND EXISTS (
             SELECT 1
             FROM publication.channel_delivery_state AS delivery
             JOIN publication.channel_stream_state AS route_owner
               ON route_owner.publication_stream_id=delivery.publication_stream_id
              AND route_owner.channel_id=delivery.channel_id
             WHERE delivery.publication_stream_id=stream.publication_stream_id
               AND route_owner.status='owned' AND delivery.mode='online'
           )
           AND NOT EXISTS (
             SELECT 1
             FROM publication.channel_delivery_state AS delivery
             JOIN publication.channel_stream_state AS route_owner
               ON route_owner.publication_stream_id=delivery.publication_stream_id
              AND route_owner.channel_id=delivery.channel_id
             WHERE delivery.publication_stream_id=stream.publication_stream_id
               AND route_owner.status='owned' AND delivery.mode<>'online'
           )
       )
     ORDER BY promotion_candidate.accepted_at,channel.channel_id
     LIMIT $1`,
    [batchLimit],
  );
  const summary = {
    scanned: candidates.rows.length,
    registered: 0,
    reconciled: 0,
    skipped: 0,
    failed: 0,
    failures: [],
  };
  for (const row of candidates.rows) {
    try {
      const result = await withTransaction((client) => reconcilePublicationAfterFullCrawl(client, {
        channelId: row.channel_id,
        runId: row.run_id,
        asOf: row.publication_as_of,
        revisionType: "incremental",
      }));
      if (result.onboarding?.status === "registered") summary.registered += 1;
      if (result.status === "not_owned") summary.skipped += 1;
      else summary.reconciled += 1;
    } catch (error) {
      summary.failed += 1;
      summary.failures.push({
        channel_id: String(row.channel_id),
        error: String(error?.message || error).slice(0, 1000),
      });
    }
  }
  return summary;
}
