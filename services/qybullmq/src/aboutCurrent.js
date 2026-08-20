import { normalizePublicationLinks } from "./publicationLinks.js";
import {
  normalizePublicationImageUrl,
  normalizePublicationUrl,
} from "./publicationUrl.js";
import { normalizeJoinedDateCurrent } from "./youtubeJoinedDate.js";
import { normalizeVerifiedCurrent } from "./verifiedCurrent.js";
import { normalizeYoutubeBusinessEmailCurrent } from "./youtubeBusinessEmailAvailability.js";

function text(value) {
  if (value === null || value === undefined) return null;
  const output = String(value).trim();
  return output || null;
}

function hasOwn(value, key) {
  return value != null && Object.prototype.hasOwnProperty.call(value, key);
}

function stringList(value) {
  const items = Array.isArray(value) ? value : value == null ? [] : [value];
  return [...new Set(items.map(text).filter(Boolean))].sort();
}

function keywordList(value) {
  if (Array.isArray(value)) return stringList(value);
  const raw = text(value);
  if (!raw) return [];
  if (raw.startsWith("[") && raw.endsWith("]")) {
    try {
      const parsed = JSON.parse(raw);
      if (Array.isArray(parsed)) return stringList(parsed);
    } catch {
      // Fall through to YouTube's quoted keyword syntax.
    }
  }
  const items = [];
  const tokenPattern = /"((?:\\.|[^"\\])*)"|'((?:\\.|[^'\\])*)'|([^,;\s]+)/g;
  for (const match of raw.matchAll(tokenPattern)) {
    const item = text((match[1] ?? match[2] ?? match[3] ?? "").replace(/\\(["'\\])/g, "$1"));
    if (item) items.push(item);
  }
  return stringList(items);
}

export function normalizeAboutCurrentIdentity(metadata = {}) {
  return {
    title: text(metadata.title),
    handle: text(metadata.handle),
    avatar_url: text(metadata.avatar_url),
    keywords: keywordList(metadata.keywords),
    available_tabs: stringList(metadata.available_tabs),
    summary: text(metadata.description),
  };
}

export function normalizeAboutObservationCurrent(metadata = {}, {
  aboutObserved = false,
  locale = "en",
} = {}) {
  const identity = normalizeAboutCurrentIdentity({
    ...metadata,
    avatar_url: normalizePublicationImageUrl(metadata.avatar_url),
  });
  const joined = normalizeJoinedDateCurrent({
    joinedAt: metadata.joined_at,
    joinedDateText: metadata.joined_date_text,
    precision: metadata.joined_at_precision,
    locale,
  });
  const linksObserved = metadata.external_links_status === "observed"
    || metadata.external_links_observed === true
    || (aboutObserved && Array.isArray(metadata.external_links));
  const links = normalizePublicationLinks(metadata.external_links, { observed: linksObserved });
  const verified = normalizeVerifiedCurrent(metadata.is_verified, metadata.is_verified_status);
  const businessEmail = normalizeYoutubeBusinessEmailCurrent(
    metadata.youtube_business_email_available,
    metadata.youtube_business_email_status,
    { aboutObserved },
  );
  return {
    aboutDescription: identity.summary,
    descriptionStatus: identity.summary ? "exact" : aboutObserved ? "empty" : "unresolved",
    country: aboutObserved ? text(metadata.country) : null,
    joinedDateText: joined.raw,
    joinedAt: joined.value,
    joinedAtPrecision: joined.precision,
    externalLinks: links.ready ? links.links : null,
    externalLinksStatus: links.ready ? "observed" : "unresolved",
    rssUrl: normalizePublicationUrl(metadata.rss_url),
    vanityChannelUrl: normalizePublicationUrl(metadata.vanity_channel_url),
    isFamilySafe: typeof metadata.is_family_safe === "boolean" ? metadata.is_family_safe : null,
    isVerified: verified.value,
    isVerifiedStatus: verified.status,
    youtubeBusinessEmailAvailable: businessEmail.available,
    youtubeBusinessEmailStatus: businessEmail.status,
    keywordsStatus: hasOwn(metadata, "keywords") ? "observed" : "unresolved",
    availableTabsStatus: Array.isArray(metadata.available_tabs) ? "observed" : "unresolved",
    identity,
    normalization: {
      links_ready: links.ready,
      link_issues: links.issues,
      joined_date_status: joined.status,
    },
  };
}
