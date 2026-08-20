const TRACKING_PARAMS = new Set([
  "_ga",
  "_gl",
  "_r",
  "_t",
  "dclid",
  "fbclid",
  "gclid",
  "igsh",
  "igshid",
  "mc_cid",
  "mc_eid",
  "msclkid",
  "si",
  "yclid",
]);

const QUERYLESS_IMAGE_HOSTS = new Set([
  "i.ytimg.com",
  "yt3.ggpht.com",
  "yt3.googleusercontent.com",
]);

function text(value) {
  const output = String(value ?? "").trim();
  return output || null;
}

function youtubeHost(hostname) {
  return /(^|\.)youtube\.com$/i.test(hostname);
}

function querylessImageHost(hostname) {
  return QUERYLESS_IMAGE_HOSTS.has(hostname)
    || hostname.endsWith(".googleusercontent.com");
}

function normalizeMailto(candidate) {
  const rawAddress = candidate.replace(/^mailto:/i, "").split(/[?#]/, 1)[0];
  let address;
  try {
    address = decodeURIComponent(rawAddress).trim().toLocaleLowerCase("en");
  } catch {
    return null;
  }
  return /^[^@\s]+@[^@\s]+\.[^@\s]+$/.test(address) ? `mailto:${address}` : null;
}

function normalizeTelephone(candidate) {
  const rawNumber = candidate.replace(/^tel:/i, "").split(/[?#]/, 1)[0].trim();
  const international = rawNumber.startsWith("+");
  const digits = rawNumber.replace(/\D/g, "");
  if (digits.length < 8 || digits.length > 15) return null;
  return `tel:${international ? "+" : ""}${digits}`;
}

export function normalizePublicationUrl(rawValue, {
  allowMailto = false,
  allowTel = false,
  image = false,
} = {}) {
  let candidate = text(rawValue);
  if (!candidate) return null;

  const legacyMailtoUrl = candidate.match(/^mailto:(https?:\/\/.*)$/i);
  if (legacyMailtoUrl) candidate = legacyMailtoUrl[1];

  const bareEmail = !candidate.includes("/")
    && !/^[a-z][a-z0-9+.-]*:/i.test(candidate)
    && /^[^@\s]+@[^@\s]+\.[^@\s]+$/.test(candidate);
  if (allowMailto && (/^mailto:/i.test(candidate) || bareEmail)) {
    return normalizeMailto(candidate);
  }
  if (allowTel && /^tel:/i.test(candidate)) return normalizeTelephone(candidate);

  for (let redirects = 0; redirects < 3; redirects += 1) {
    let parsed;
    try {
      if (candidate.startsWith("//")) {
        parsed = new URL(`https:${candidate}`);
      } else if (candidate.startsWith("/")) {
        parsed = new URL(candidate, "https://www.youtube.com");
      } else {
        const absolute = /^[a-z][a-z0-9+.-]*:/i.test(candidate)
          ? candidate
          : `https://${candidate}`;
        parsed = new URL(absolute);
      }
    } catch {
      return null;
    }

    if (!["http:", "https:"].includes(parsed.protocol)
        || !parsed.hostname
        || parsed.username
        || parsed.password) {
      return null;
    }

    const redirectTarget = youtubeHost(parsed.hostname)
      && parsed.pathname.replace(/\/+$/, "") === "/redirect"
      ? parsed.searchParams.get("q") ?? parsed.searchParams.get("url")
      : null;
    if (redirectTarget) {
      candidate = redirectTarget;
      continue;
    }

    parsed.hash = "";
    parsed.hostname = parsed.hostname.toLocaleLowerCase("en");
    if (image && querylessImageHost(parsed.hostname)) {
      parsed.search = "";
    } else {
      for (const key of [...parsed.searchParams.keys()]) {
        const normalizedKey = key.toLocaleLowerCase("en");
        if (normalizedKey.startsWith("utm_") || TRACKING_PARAMS.has(normalizedKey)) {
          parsed.searchParams.delete(key);
        }
      }
    }
    if ((parsed.protocol === "https:" && parsed.port === "443")
        || (parsed.protocol === "http:" && parsed.port === "80")) {
      parsed.port = "";
    }
    return parsed.toString();
  }
  return null;
}

export function normalizePublicationImageUrl(rawValue) {
  return normalizePublicationUrl(rawValue, { image: true });
}
