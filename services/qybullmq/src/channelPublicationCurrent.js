import { normalizeAboutCurrentIdentity } from "./aboutCurrent.js";
import { observationFactsHash } from "./crawlObservationStore.js";
import {
  CHANNEL_PUBLICATION_CONTRACT_VERSION,
  PUBLICATION_POLICY_VERSION,
} from "./publicationContract.js";
import { normalizePublicationLinks } from "./publicationLinks.js";
import {
  normalizePublicationImageUrl,
  normalizePublicationUrl,
} from "./publicationUrl.js";
import { normalizeJoinedDateCurrent } from "./youtubeJoinedDate.js";
import { normalizeVerifiedCurrent } from "./verifiedCurrent.js";

const ALLOWED_LIFECYCLE_STATUSES = new Set(["active", "dormant"]);
const BLOCKING_CHANNEL_FIELDS = new Set(["channel_id", "lifecycle_status"]);
const RESOLVED_METRIC_STATUSES = new Set(["exact", "estimated"]);
const UNRESOLVED_METRIC_STATUSES = new Set(["unavailable", "unresolved"]);

function hasOwn(value, key) {
  return value != null && Object.prototype.hasOwnProperty.call(value, key);
}

function object(value) {
  return value && typeof value === "object" && !Array.isArray(value) ? value : {};
}

function text(value) {
  const output = String(value ?? "").trim();
  return output || null;
}

function nonnegativeInteger(value) {
  if (value === null || value === undefined || value === "") return null;
  const parsed = Number(value);
  return Number.isSafeInteger(parsed) && parsed >= 0 ? parsed : null;
}

function isoTimestamp(value) {
  if (value === null || value === undefined || value === "") return null;
  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? null : parsed.toISOString();
}

function businessEmailCurrent(channel) {
  const available = typeof channel.youtube_business_email_available === "boolean"
    ? channel.youtube_business_email_available
    : null;
  const observedAt = isoTimestamp(channel.youtube_business_email_observed_at);
  if (available === null || observedAt === null) {
    return { available: null, observed_at: null, status: "unknown" };
  }
  return {
    available,
    observed_at: observedAt,
    status: available ? "available" : "not_available",
  };
}

function compareText(left, right) {
  return Buffer.compare(Buffer.from(String(left ?? ""), "utf8"), Buffer.from(String(right ?? ""), "utf8"));
}

function issueKey(issue) {
  return [issue.domain, issue.code, issue.field].map((value) => value ?? "").join("\u0000");
}

function sortedIssues(issues) {
  const unique = new Map();
  for (const issue of issues) unique.set(issueKey(issue), issue);
  return [...unique.values()].sort((left, right) => (
    compareText(left.domain, right.domain)
    || compareText(left.code, right.code)
    || compareText(left.field, right.field)
  ));
}

function metric(row, name) {
  const status = text(row?.[`${name}_status`]);
  const rawValue = row?.[name];
  const value = nonnegativeInteger(rawValue);
  const validStatus = RESOLVED_METRIC_STATUSES.has(status) || UNRESOLVED_METRIC_STATUSES.has(status);
  const valid = validStatus
    && (RESOLVED_METRIC_STATUSES.has(status) ? value !== null : rawValue == null);
  return {
    value: valid && RESOLVED_METRIC_STATUSES.has(status) ? value : null,
    text: text(row?.[`${name}_text`]),
    status,
    source: text(row?.[`${name}_source`]),
    valid,
  };
}

function verifiedCurrent(row, header) {
  const channel = normalizeVerifiedCurrent(row.is_verified, row.is_verified_status);
  if (channel.valid && channel.status !== "unknown") return channel;

  const fallback = normalizeVerifiedCurrent(header.is_verified, header.is_verified_status);
  if (fallback.valid && fallback.status !== "unknown") return fallback;
  return channel.valid ? channel : fallback;
}

function observedList(row, header, { key, statusKey }) {
  const rowValue = row[key];
  const rowObserved = text(row[statusKey]) === "observed"
    || (Array.isArray(rowValue) && rowValue.length > 0);
  const headerObserved = hasOwn(header, key) && header[key] !== null;
  const value = rowObserved ? rowValue : headerObserved ? header[key] : rowValue;
  return {
    value,
    observed: rowObserved || headerObserved,
  };
}

function fieldCheck(field, value, source, complete) {
  return {
    field,
    complete: complete === true,
    source,
    present: value !== null && value !== undefined,
  };
}

export function buildChannelPublicationCurrent(rowValue = {}) {
  const channel = object(rowValue);
  const sourceJson = object(channel.source_json);
  const header = object(sourceJson.channel_header);
  const keywordCurrent = observedList(channel, header, {
    key: "keywords",
    statusKey: "keywords_status",
  });
  const tabCurrent = observedList(channel, header, {
    key: "available_tabs",
    statusKey: "available_tabs_status",
  });
  const descriptionStatus = text(channel.description_status);
  const description = descriptionStatus === "empty"
    ? null
    : channel.about_description ?? channel.summary ?? header.description;
  const descriptionComplete = descriptionStatus === "empty" || Boolean(text(description));
  const identity = normalizeAboutCurrentIdentity({
    title: channel.title ?? header.title,
    handle: channel.handle ?? header.handle,
    avatar_url: channel.avatar_url ?? header.avatar_url,
    keywords: keywordCurrent.value,
    available_tabs: tabCurrent.value,
    description,
  });
  const channelId = text(channel.channel_id);
  const canonicalUrl = normalizePublicationUrl(channel.channel_url ?? header.channel_url);
  const vanityUrl = normalizePublicationUrl(channel.vanity_channel_url ?? header.vanity_channel_url);
  const rssUrl = normalizePublicationUrl(
    channel.rss_url
      ?? header.rss_url
      ?? (channelId ? `https://www.youtube.com/feeds/videos.xml?channel_id=${encodeURIComponent(channelId)}` : null),
  );
  const avatarUrl = normalizePublicationImageUrl(identity.avatar_url);
  const familySafe = typeof channel.is_family_safe === "boolean"
    ? channel.is_family_safe
    : typeof header.is_family_safe === "boolean" ? header.is_family_safe : null;
  const verified = verifiedCurrent(channel, header);
  const joined = normalizeJoinedDateCurrent({
    joinedAt: channel.joined_at,
    joinedDateText: channel.joined_date_text ?? header.joined_date_text,
    precision: channel.joined_at_precision,
  });
  const subscriber = metric(channel, "subscriber_count");
  const views = metric(channel, "total_view_count");
  const videos = metric(channel, "total_video_count");
  const businessEmail = businessEmailCurrent(channel);
  const channelLinks = Array.isArray(channel.external_links) ? channel.external_links : null;
  const headerLinks = Array.isArray(header.external_links) ? header.external_links : null;
  const channelLinksObserved = text(channel.external_links_status) === "observed";
  const headerLinksObserved = text(header.external_links_status) === "observed";
  const linksObserved = channelLinksObserved
    || (channelLinks?.length ?? 0) > 0
    || headerLinksObserved
    || (headerLinks?.length ?? 0) > 0;
  const rawLinks = channelLinksObserved || (channelLinks?.length ?? 0) > 0
    ? channelLinks
    : headerLinksObserved || (headerLinks?.length ?? 0) > 0
      ? headerLinks
      : channelLinks;
  const links = normalizePublicationLinks(rawLinks, { observed: linksObserved });
  const tabs = new Set(identity.available_tabs.map((value) => value.toLocaleLowerCase("en")));
  const lifecycle = text(channel.status);

  const checks = [
    fieldCheck("channel_id", channelId, "crawler.channels.channel_id", Boolean(channelId)),
    fieldCheck("title", identity.title, "channel metadata", Boolean(identity.title)),
    fieldCheck("canonical_url", canonicalUrl, "crawler.channels.channel_url", Boolean(canonicalUrl)),
    fieldCheck("avatar", avatarUrl, "channel metadata/header", Boolean(avatarUrl)),
    fieldCheck("keywords", identity.keywords, "channel metadata/header", keywordCurrent.observed),
    fieldCheck("available_tabs", identity.available_tabs, "channel metadata", tabCurrent.observed),
    fieldCheck("description", identity.summary, "about/metadata fallback", descriptionComplete),
    fieldCheck("rss_url", rssUrl, "channel metadata/identity", Boolean(rssUrl)),
    fieldCheck("is_family_safe", familySafe, "channel metadata", familySafe !== null),
    fieldCheck(
      "is_verified",
      verified.value,
      "channel header",
      verified.valid && verified.status !== "unknown",
    ),
    fieldCheck("joined_date", joined.value, "about", Boolean(joined.value)),
    fieldCheck("subscriber_count", subscriber.value, "about", subscriber.valid),
    fieldCheck("total_view_count", views.value, "about", views.valid),
    fieldCheck("total_video_count", videos.value, "about", videos.valid),
    fieldCheck("links", links.links, "about", links.ready),
    fieldCheck(
      "youtube_business_email_available",
      businessEmail.available,
      "youtube about business email marker",
      businessEmail.status !== "unknown",
    ),
    fieldCheck("lifecycle_status", lifecycle, "crawler.channels.status", ALLOWED_LIFECYCLE_STATUSES.has(lifecycle)),
  ];
  const issues = [];
  const warnings = [...links.issues];
  for (const check of checks) {
    if (check.complete) continue;
    const finding = {
      domain: "channel",
      code: check.field === "lifecycle_status"
        ? "channel_lifecycle_not_publishable"
        : check.field === "is_verified" && verified.valid && verified.status === "unknown"
          ? "channel_verified_state_unknown"
          : "channel_contract_field_incomplete",
      field: check.field,
    };
    if (BLOCKING_CHANNEL_FIELDS.has(check.field)) issues.push(finding);
    else warnings.push(finding);
  }
  if (text(header.channel_id) && text(header.channel_id) !== channelId) {
    issues.push({ domain: "channel", code: "channel_identity_conflict", field: "channel_id" });
  }

  const payload = {
    channel_id: channelId,
    title: identity.title,
    canonical_url: canonicalUrl,
    vanity_channel_url: vanityUrl,
    handle: identity.handle,
    avatar: avatarUrl ? [{ url: avatarUrl, position: 0 }] : [],
    rss_url: rssUrl,
    keywords: identity.keywords,
    is_family_safe: familySafe,
    is_verified: verified.value,
    is_verified_status: verified.status,
    has_videos: tabs.has("videos"),
    has_shorts: tabs.has("shorts"),
    has_live_streams: tabs.has("live") || tabs.has("streams"),
    description: identity.summary,
    subscriber_count: subscriber.value,
    subscriber_count_status: subscriber.status,
    total_video_count: videos.value,
    total_video_count_status: videos.status,
    total_view_count: views.value,
    total_view_count_status: views.status,
    joined_date: joined.value,
    joined_date_status: joined.status,
    joined_date_raw: joined.raw,
    country_code: text(channel.country_code),
    country_name: text(channel.country_canonical_name) ?? text(channel.country),
    links: links.links,
    youtube_business_email_available: businessEmail.available,
    youtube_business_email_observed_at: businessEmail.observed_at,
    lifecycle_status: lifecycle,
  };
  const readinessIssues = sortedIssues(issues);
  const ready = readinessIssues.length === 0;
  return {
    ready,
    contract_version: CHANNEL_PUBLICATION_CONTRACT_VERSION,
    policy_version: PUBLICATION_POLICY_VERSION,
    payload,
    result_hash: ready ? observationFactsHash(payload) : null,
    checks,
    links: {
      ready: links.ready,
      explicit_empty: links.explicit_empty,
      source_count: links.source_count,
      valid_count: links.valid_count,
      target_hash: links.ready ? observationFactsHash(links.links) : null,
      issues: links.issues,
    },
    issues: readinessIssues,
    warnings: sortedIssues(warnings),
    comparison_values: {
      title: identity.title,
      handle: identity.handle,
      avatar_url: avatarUrl,
      description: identity.summary,
      is_verified: verified.valid ? verified.value : null,
      subscriber_count: subscriber.value,
      total_view_count: views.value,
      total_video_count: videos.value,
      joined_date: joined.value,
      link_count: links.ready ? links.valid_count : null,
      youtube_business_email_available: businessEmail.available,
    },
  };
}
