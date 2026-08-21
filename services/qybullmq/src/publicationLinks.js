import {
  normalizePublicationImageUrl,
  normalizePublicationUrl,
} from "./publicationUrl.js";

const CONTACT_TYPES = new Set([
  "email",
  "facebook",
  "instagram",
  "phone",
  "telegram",
  "tiktok",
  "whatsapp",
  "x_twitter",
]);
const LINK_TYPES = new Set([
  ...CONTACT_TYPES,
  "spotify",
  "website",
  "youtube",
]);
const LINK_PURPOSES = new Set(["contact", "public_reference"]);

function object(value) {
  return value && typeof value === "object" && !Array.isArray(value) ? value : {};
}

function text(value) {
  if (value === null || value === undefined) return null;
  if (typeof value === "object") {
    try {
      const rendered = value.toString();
      return rendered && rendered !== "[object Object]" ? rendered.trim() || null : null;
    } catch {
      return null;
    }
  }
  const output = String(value).trim();
  return output || null;
}

function nonnegativeInteger(value) {
  if (value === null || value === undefined || value === "") return null;
  const parsed = Number(value);
  return Number.isSafeInteger(parsed) && parsed >= 0 ? parsed : null;
}

function nested(value, path) {
  let current = value;
  for (const key of path) {
    if (current === null || current === undefined) return null;
    current = current[key];
  }
  return text(current);
}

function compareText(left, right) {
  return Buffer.compare(Buffer.from(String(left ?? ""), "utf8"), Buffer.from(String(right ?? ""), "utf8"));
}

export function publicationLinkTarget(rawValue) {
  const raw = object(rawValue);
  const paths = [
    ["link", "command_runs", 0, "on_tap", "payload", "url"],
    ["link", "commandRuns", 0, "onTap", "payload", "url"],
    ["link", "command_runs", 0, "on_tap", "metadata", "url"],
    ["link", "commandRuns", 0, "onTap", "metadata", "url"],
    ["link", "endpoint", "payload", "url"],
    ["link", "endpoint", "metadata", "url"],
    ["link", "runs", 0, "endpoint", "payload", "url"],
    ["link", "runs", 0, "endpoint", "metadata", "url"],
    ["endpoint", "payload", "url"],
    ["endpoint", "metadata", "url"],
    ["navigation_endpoint", "payload", "url"],
    ["navigation_endpoint", "metadata", "url"],
    ["navigationEndpoint", "payload", "url"],
    ["navigationEndpoint", "metadata", "url"],
    ["target_url"],
    ["url"],
  ];
  for (const path of paths) {
    const value = nested(raw, path);
    if (value) return value;
  }
  const directLink = typeof raw.link === "string" ? text(raw.link) : null;
  return directLink;
}

export function publicationLinkType(urlValue) {
  const url = String(urlValue ?? "");
  if (url.startsWith("mailto:")) return "email";
  if (url.startsWith("tel:")) return "phone";
  let host;
  try {
    host = new URL(url).hostname.replace(/^www\./i, "").toLocaleLowerCase("en");
  } catch {
    return null;
  }
  const matches = (...domains) => domains.some((domain) => host === domain || host.endsWith(`.${domain}`));
  if (matches("instagram.com")) return "instagram";
  if (matches("facebook.com", "fb.com")) return "facebook";
  if (matches("x.com", "twitter.com")) return "x_twitter";
  if (matches("tiktok.com")) return "tiktok";
  if (matches("spotify.com")) return "spotify";
  if (matches("youtube.com", "youtu.be")) return "youtube";
  if (matches("whatsapp.com", "wa.me")) return "whatsapp";
  if (matches("t.me", "telegram.me")) return "telegram";
  return "website";
}

function publicationLinkPurpose(type, title, displayUrl) {
  const context = `${text(title) ?? ""} ${text(displayUrl) ?? ""}`.toLocaleLowerCase("en");
  if (/(^|\W)(pix|paypal|donat|doa|coffee|apoie|shop|loja|compr)/i.test(context)) {
    return "public_reference";
  }
  if (CONTACT_TYPES.has(type)
      || /(contat|contact|business|commercial|comercial|inquir|e-?mail|parce|partner|collab|sponsor|suporte|support|atendimento)/i.test(context)) {
    return "contact";
  }
  return "public_reference";
}

function issue(code, index) {
  return { domain: "channel", code, field: `links[${index}]` };
}

export function normalizePublicationLinks(rawLinks, { observed = Array.isArray(rawLinks) } = {}) {
  if (!observed || !Array.isArray(rawLinks)) {
    return {
      ready: false,
      explicit_empty: false,
      source_count: Array.isArray(rawLinks) ? rawLinks.length : null,
      valid_count: 0,
      links: [],
      issues: [{ domain: "channel", code: "channel_links_state_unknown", field: "links" }],
    };
  }

  const issues = [];
  const links = [];
  const seen = new Set();
  for (const [index, rawValue] of rawLinks.entries()) {
    const raw = object(rawValue);
    const direct = publicationLinkTarget(raw);
    const targetUrl = normalizePublicationUrl(direct, { allowMailto: true, allowTel: true });
    if (!targetUrl) {
      issues.push(issue(direct ? "channel_link_target_invalid" : "channel_link_target_missing", index));
      continue;
    }
    if (seen.has(targetUrl)) continue;
    seen.add(targetUrl);
    const inferredType = publicationLinkType(targetUrl);
    const declaredType = text(raw.link_type);
    const declaredTypeConflicts = LINK_TYPES.has(declaredType) && declaredType !== inferredType;
    const type = inferredType;
    const title = text(raw.title);
    const displayUrl = text(raw.display_url) ?? text(raw.displayUrl);
    const declaredPurpose = text(raw.purpose);
    links.push({
      title,
      display_url: displayUrl,
      target_url: targetUrl,
      favicon_url: normalizePublicationImageUrl(raw.favicon_url ?? raw.faviconUrl),
      position: nonnegativeInteger(raw.position) ?? index,
      link_type: type,
      purpose: !declaredTypeConflicts && LINK_PURPOSES.has(declaredPurpose)
        ? declaredPurpose
        : publicationLinkPurpose(type, title, displayUrl),
    });
  }
  links.sort((left, right) => left.position - right.position || compareText(left.target_url, right.target_url));
  return {
    ready: issues.length === 0,
    explicit_empty: rawLinks.length === 0,
    source_count: rawLinks.length,
    valid_count: links.length,
    links,
    issues,
  };
}
