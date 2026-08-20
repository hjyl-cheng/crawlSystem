export const PUBLICATION_COMPARISON_MAX_CHANNELS = 100;

const CHANNEL_ID_PATTERN = /^UC[A-Za-z0-9_-]{22}$/;
const DOMAINS = Object.freeze(["channel", "video", "agent"]);
const DOMAIN_ORDER = new Map(DOMAINS.map((domain, index) => [domain, index]));

function requiredArray(value, field) {
  if (!Array.isArray(value)) throw new TypeError(`${field} must be an array`);
  return value;
}

export function normalizePublicationComparisonChannelIds(value) {
  const values = Array.isArray(value) ? value : [value];
  const output = [];
  const seen = new Set();
  for (const raw of values) {
    const channelId = String(raw ?? "").trim();
    if (!channelId || seen.has(channelId)) continue;
    if (!CHANNEL_ID_PATTERN.test(channelId)) {
      throw new TypeError(`无效的 YouTube Channel ID：${channelId}`);
    }
    seen.add(channelId);
    output.push(channelId);
  }
  if (output.length === 0) throw new TypeError("请至少选择一个频道");
  if (output.length > PUBLICATION_COMPARISON_MAX_CHANNELS) {
    throw new TypeError(`一次最多比对 ${PUBLICATION_COMPARISON_MAX_CHANNELS} 个频道`);
  }
  return output.sort();
}

function byChannel(rows) {
  const grouped = new Map();
  for (const row of rows) {
    const channelId = String(row.channel_id ?? "");
    const values = grouped.get(channelId) ?? [];
    values.push(row);
    grouped.set(channelId, values);
  }
  return grouped;
}

function text(value) {
  return value == null ? null : String(value);
}

function timestamp(value) {
  if (value == null || value === "") return null;
  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? null : parsed.toISOString();
}

function revisionMatches(source, business) {
  return business != null
    && text(source.revision_id) === text(business.revision_id)
    && text(source.publication_stream_id) === text(business.publication_stream_id)
    && text(source.domain) === text(business.domain)
    && Number(source.data_sequence) === Number(business.data_sequence)
    && text(source.result_hash) === text(business.result_hash)
    && text(source.payload_hash) === text(business.payload_hash);
}

function vectorEntry(row, domain) {
  const value = row?.version_vector?.[domain];
  return value && typeof value === "object" ? value : null;
}

function vectorMatchesRevision(row, revision) {
  const entry = vectorEntry(row, revision.domain);
  return entry != null
    && text(entry.publication_stream_id) === text(revision.publication_stream_id)
    && Number(entry.sequence) === Number(revision.data_sequence)
    && text(entry.revision_id) === text(revision.revision_id)
    && text(entry.result_hash) === text(revision.result_hash);
}

function nullableMetric(value) {
  return value == null || value === "" ? null : String(value);
}

function channelSnapshotMatchesRevision(projection, revision) {
  const payload = revision.payload_json ?? {};
  const sourceObservedAt = revision.source_refs?.complete_observation?.observed_at ?? null;
  return projection.action === "upsert"
    && text(projection.snapshot_id) !== null
    && text(projection.title) === text(payload.title)
    && nullableMetric(projection.subscriber_count) === nullableMetric(payload.subscriber_count)
    && nullableMetric(projection.total_view_count) === nullableMetric(payload.total_view_count)
    && nullableMetric(projection.video_count) === nullableMetric(payload.total_video_count)
    && timestamp(projection.channel_observed_at) === timestamp(sourceObservedAt);
}

function cursorMatchesRevision(source, business) {
  return business != null
    && text(source.publication_stream_id) === text(business.publication_stream_id)
    && Number(source.data_sequence) === Number(business.active_sequence)
    && text(source.revision_id) === text(business.active_revision_id)
    && text(source.result_hash) === text(business.active_result_hash);
}

function liveVectorMatchesCursor(row, cursor) {
  const entry = vectorEntry(row, cursor.domain);
  return entry != null
    && text(entry.publication_stream_id) === text(cursor.publication_stream_id)
    && Number(entry.sequence) === Number(cursor.active_sequence)
    && text(entry.revision_id) === text(cursor.active_revision_id)
    && text(entry.result_hash) === text(cursor.active_result_hash);
}

function latestRevisionPerDomain(revisions) {
  const latest = new Map();
  for (const revision of revisions) {
    const previous = latest.get(revision.domain);
    if (previous == null || Number(revision.data_sequence) > Number(previous.data_sequence)) {
      latest.set(revision.domain, revision);
    }
  }
  return DOMAINS.map((domain) => latest.get(domain)).filter(Boolean);
}

function issue(code, message, severity = "error") {
  return { code, message, severity };
}

function comparisonForChannel(channelId, data) {
  const sourceChannels = data.sourceChannels.get(channelId) ?? [];
  const sourceRevisions = (data.sourceRevisions.get(channelId) ?? [])
    .sort((left, right) => (
      (DOMAIN_ORDER.get(left.domain) ?? 99) - (DOMAIN_ORDER.get(right.domain) ?? 99)
        || Number(left.data_sequence) - Number(right.data_sequence)
    ));
  const sourceCurrents = data.sourceCurrents.get(channelId) ?? [];
  const aboutObservations = data.sourceAboutObservations.get(channelId) ?? [];
  const businessRevisions = data.businessRevisions.get(channelId) ?? [];
  const businessCursors = data.businessCursors.get(channelId) ?? [];
  const projections = data.businessProjections.get(channelId) ?? [];
  const liveRows = data.businessLive.get(channelId) ?? [];

  const businessRevisionById = new Map(
    businessRevisions.map((row) => [text(row.revision_id), row]),
  );
  const businessCursorByDomain = new Map(
    businessCursors.map((row) => [row.domain, row]),
  );
  const deliveredRevisions = sourceRevisions.filter((row) => row.outbox_status === "delivered");
  const baselineCoveredRevisions = sourceRevisions.filter(
    (row) => row.outbox_status === "covered_by_baseline",
  );
  const openRevisions = sourceRevisions.filter((row) => (
    ["held", "pending", "leased", "retry_wait"].includes(row.outbox_status)
  ));
  const deadRevisions = sourceRevisions.filter((row) => row.outbox_status === "dead_letter");
  const transported = deliveredRevisions.filter((source) => (
    revisionMatches(source, businessRevisionById.get(text(source.revision_id)))
  ));
  const activated = deliveredRevisions.filter((source) => (
    businessRevisionById.get(text(source.revision_id))?.activated === true
  ));
  const represented = activated.filter((source) => (
    projections.some((projection) => vectorMatchesRevision(projection, source))
  ));

  const channelRevisions = activated.filter((row) => (
    row.domain === "channel" && row.revision_type !== "retraction"
  ));
  const matchedChannelSnapshots = channelRevisions.filter((revision) => {
    const matching = projections.filter((projection) => (
      vectorMatchesRevision(projection, revision)
    ));
    return matching.length > 0
      && matching.every((projection) => channelSnapshotMatchesRevision(projection, revision));
  });

  const expectedCurrents = latestRevisionPerDomain(deliveredRevisions);
  const currentMatched = expectedCurrents.filter((source) => (
    cursorMatchesRevision(source, businessCursorByDomain.get(source.domain))
  ));
  const live = liveRows[0] ?? null;
  const liveVectorMatches = businessCursors.length === DOMAINS.length
    && live != null
    && businessCursors.every((cursor) => liveVectorMatchesCursor(live, cursor));

  const referencedObservationIds = new Set(
    sourceRevisions
      .filter((row) => row.domain === "channel")
      .map((row) => text(row.source_refs?.complete_observation?.observation_id))
      .filter(Boolean),
  );
  const firstPublishedObservationAt = sourceRevisions
    .filter((row) => row.domain === "channel")
    .map((row) => timestamp(row.source_refs?.complete_observation?.observed_at))
    .filter(Boolean)
    .sort()[0] ?? null;
  const preBootstrapObservations = aboutObservations.filter((row) => (
    firstPublishedObservationAt != null
      && timestamp(row.observed_at) < firstPublishedObservationAt
  ));
  const noChangeObservations = aboutObservations.filter((row) => (
    !referencedObservationIds.has(text(row.observation_id))
      && !preBootstrapObservations.includes(row)
  ));

  const issues = [];
  const hasPublication = deliveredRevisions.length > 0
    || baselineCoveredRevisions.length > 0
    || businessRevisions.length > 0;
  if (!hasPublication) {
    issues.push(issue("not_published", "尚未形成可对账的 Publication Revision", "warning"));
  }
  if (transported.length !== deliveredRevisions.length) {
    issues.push(issue(
      "revision_mismatch",
      `源端已投递 Revision ${deliveredRevisions.length} 条，业务库精确匹配 ${transported.length} 条`,
    ));
  }
  if (activated.length !== deliveredRevisions.length) {
    issues.push(issue(
      "activation_incomplete",
      `已投递 Revision 中仅 ${activated.length}/${deliveredRevisions.length} 完成业务 Activation`,
    ));
  }
  if (represented.length !== activated.length) {
    issues.push(issue(
      "history_gap",
      `已激活 Revision 中仅 ${represented.length}/${activated.length} 被历史 Snapshot 表示`,
    ));
  }
  if (matchedChannelSnapshots.length !== channelRevisions.length) {
    issues.push(issue(
      "about_snapshot_mismatch",
      `三项 About 指标快照仅 ${matchedChannelSnapshots.length}/${channelRevisions.length} 完整匹配`,
    ));
  }
  if (sourceCurrents.length !== DOMAINS.length) {
    issues.push(issue(
      "source_current_incomplete",
      `爬虫库 Domain Current 仅 ${sourceCurrents.length}/${DOMAINS.length} 个域`,
    ));
  }
  if (expectedCurrents.length !== DOMAINS.length || currentMatched.length !== DOMAINS.length) {
    issues.push(issue(
      "current_mismatch",
      `Channel/Video/Agent Current 精确匹配 ${currentMatched.length}/${DOMAINS.length}`,
    ));
  }
  if (businessCursors.length === DOMAINS.length && !liveVectorMatches) {
    issues.push(issue("live_vector_mismatch", "业务查询 Current 未指向最新三域 Version Vector"));
  }
  if (openRevisions.length > 0) {
    issues.push(issue(
      "delivery_pending",
      `${openRevisions.length} 条 Revision 仍在 Publication 投递中`,
      "warning",
    ));
  }
  if (deadRevisions.length > 0) {
    issues.push(issue("delivery_dead_letter", `${deadRevisions.length} 条 Revision 为 Dead Letter`));
  }

  const errors = issues.filter((value) => value.severity === "error");
  const status = !hasPublication
    ? "not_published"
    : errors.length > 0
      ? "mismatch"
      : openRevisions.length > 0
        ? "pending"
        : "matched";
  const sourceChannel = sourceChannels[0] ?? {};
  return {
    channel_id: channelId,
    title: sourceChannel.title ?? "",
    handle: sourceChannel.handle ?? "",
    status,
    issues,
    revisions: {
      delivered: deliveredRevisions.length,
      matched: transported.length,
      baseline_covered: baselineCoveredRevisions.length,
      pending: openRevisions.length,
      dead_letter: deadRevisions.length,
    },
    history: {
      activated: activated.length,
      represented: represented.length,
    },
    about: {
      raw_observations: aboutObservations.length,
      revision_snapshots: channelRevisions.length,
      matched_snapshots: matchedChannelSnapshots.length,
      pre_bootstrap_observations: preBootstrapObservations.length,
      no_change_observations: noChangeObservations.length,
    },
    current: {
      expected_domains: expectedCurrents.length,
      matched_domains: currentMatched.length,
      live_vector_match: liveVectorMatches,
      snapshot_id: live?.snapshot_id ?? null,
      watermark: live?.watermark ?? null,
    },
  };
}

export function comparePublicationRows(channelIdsValue, rows) {
  const channelIds = normalizePublicationComparisonChannelIds(channelIdsValue);
  const data = {
    sourceChannels: byChannel(requiredArray(rows.sourceChannels, "sourceChannels")),
    sourceRevisions: byChannel(requiredArray(rows.sourceRevisions, "sourceRevisions")),
    sourceCurrents: byChannel(requiredArray(rows.sourceCurrents, "sourceCurrents")),
    sourceAboutObservations: byChannel(requiredArray(
      rows.sourceAboutObservations,
      "sourceAboutObservations",
    )),
    businessRevisions: byChannel(requiredArray(rows.businessRevisions, "businessRevisions")),
    businessCursors: byChannel(requiredArray(rows.businessCursors, "businessCursors")),
    businessProjections: byChannel(requiredArray(rows.businessProjections, "businessProjections")),
    businessLive: byChannel(requiredArray(rows.businessLive, "businessLive")),
  };
  const channels = channelIds.map((channelId) => comparisonForChannel(channelId, data));
  return {
    generated_at: new Date().toISOString(),
    total: channels.length,
    matched: channels.filter((row) => row.status === "matched").length,
    mismatch: channels.filter((row) => row.status === "mismatch").length,
    pending: channels.filter((row) => row.status === "pending").length,
    not_published: channels.filter((row) => row.status === "not_published").length,
    channels,
  };
}

async function sourceRows(client, channelIds, destination) {
  const channels = await client.query(
    `SELECT channel_id,title,handle,status
     FROM crawler.channels
     WHERE channel_id=ANY($1::text[])`,
    [channelIds],
  );
  const revisions = await client.query(
    `SELECT revision.channel_id,revision.revision_id,revision.publication_stream_id,
            revision.domain,revision.data_sequence,revision.revision_type,
            revision.result_hash,revision.payload_hash,revision.payload_json,
            revision.source_refs,outbox.status AS outbox_status
     FROM publication.revision revision
     LEFT JOIN publication.outbox outbox
       ON outbox.revision_id=revision.revision_id AND outbox.destination=$2
     WHERE revision.channel_id=ANY($1::text[])
     ORDER BY revision.channel_id,revision.domain,revision.data_sequence`,
    [channelIds, destination],
  );
  const currents = await client.query(
    `SELECT current.channel_id,current.domain,current.publication_stream_id,
            current.data_sequence,current.current_revision_id,current.result_hash
     FROM publication.domain_current current
     JOIN publication.channel_stream_state stream
       ON stream.publication_stream_id=current.publication_stream_id
      AND stream.channel_id=current.channel_id
      AND stream.status='owned'
     WHERE current.channel_id=ANY($1::text[])
     ORDER BY current.channel_id,current.domain`,
    [channelIds],
  );
  const about = await client.query(
    `SELECT channel_id,observation_id,observed_at,subscriber_count,
            total_view_count,total_video_count,facts_hash
     FROM crawler.channel_about_metric_snapshots
     WHERE channel_id=ANY($1::text[])
     ORDER BY channel_id,observed_at,observation_id`,
    [channelIds],
  );
  return {
    sourceChannels: channels.rows,
    sourceRevisions: revisions.rows,
    sourceCurrents: currents.rows,
    sourceAboutObservations: about.rows,
  };
}

async function businessRows(client, channelIds) {
  const revisions = await client.query(
    `SELECT revision.channel_id,revision.revision_id,revision.publication_stream_id,
            revision.domain,revision.data_sequence,revision.revision_type,
            revision.result_hash,revision.payload_hash,
            EXISTS (
              SELECT 1 FROM publication.activation_item item
              WHERE item.revision_id=revision.revision_id
            ) AS activated
     FROM publication.revision revision
     WHERE revision.channel_id=ANY($1::text[])
     ORDER BY revision.channel_id,revision.domain,revision.data_sequence`,
    [channelIds],
  );
  const cursors = await client.query(
    `SELECT channel_id,domain,publication_stream_id,active_sequence,
            active_revision_id,active_result_hash
     FROM publication.consumer_cursor
     WHERE channel_id=ANY($1::text[])
     ORDER BY channel_id,domain`,
    [channelIds],
  );
  const projections = await client.query(
    `SELECT item.channel_id,item.action,item.snapshot_id,item.version_vector,
            snapshot.channel_observed_at,snapshot.title,snapshot.subscriber_count,
            snapshot.total_view_count,snapshot.video_count
     FROM publication.projection_batch_item item
     JOIN publication.projection_batch batch
       ON batch.batch_id=item.batch_id AND batch.status='published'
     LEFT JOIN public.channel_snapshots snapshot
       ON snapshot.id=item.snapshot_id AND snapshot.channel_id=item.channel_id
     WHERE item.channel_id=ANY($1::text[])
     ORDER BY item.channel_id,item.projected_at,item.batch_id`,
    [channelIds],
  );
  const live = await client.query(
    `SELECT live.channel_id,live.snapshot_id,live.watermark,
            live.subscribers,live.total_views,live.channel_video_count,
            projected.version_vector
     FROM public.creator_search_live live
     LEFT JOIN LATERAL (
       SELECT item.version_vector
       FROM publication.projection_batch_item item
       JOIN publication.projection_batch batch
         ON batch.batch_id=item.batch_id AND batch.status='published'
       WHERE item.channel_id=live.channel_id AND item.snapshot_id=live.snapshot_id
       ORDER BY item.projected_at DESC,item.batch_id DESC
       LIMIT 1
     ) projected ON true
     WHERE live.channel_id=ANY($1::text[])
     ORDER BY live.channel_id`,
    [channelIds],
  );
  return {
    businessRevisions: revisions.rows,
    businessCursors: cursors.rows,
    businessProjections: projections.rows,
    businessLive: live.rows,
  };
}

export async function comparePublicationChannels({
  crawlerClient,
  businessClient,
  channelIds: channelIdsValue,
  destination = "business",
  expectedBusinessDatabase = null,
  expectedBusinessRole = null,
}) {
  if (!crawlerClient || typeof crawlerClient.query !== "function") {
    throw new TypeError("crawlerClient is required");
  }
  if (!businessClient || typeof businessClient.query !== "function") {
    throw new TypeError("businessClient is required");
  }
  const channelIds = normalizePublicationComparisonChannelIds(channelIdsValue);
  const source = await sourceRows(crawlerClient, channelIds, destination);
  if (expectedBusinessDatabase != null || expectedBusinessRole != null) {
    const identity = (await businessClient.query(
      "SELECT current_database() AS database_name,current_user AS role_name",
    )).rows[0] ?? {};
    if (
      identity.database_name !== expectedBusinessDatabase
      || identity.role_name !== expectedBusinessRole
    ) {
      throw new Error(
        `业务对账身份不匹配：${identity.database_name}/${identity.role_name}`,
      );
    }
  }
  // The Business snapshot starts after Source reads and can include every delivered row observed above.
  const business = await businessRows(businessClient, channelIds);
  return comparePublicationRows(channelIds, { ...source, ...business });
}
