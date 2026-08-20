const MUTATING_METHODS = new Set(["POST", "PUT", "PATCH", "DELETE"]);

export function cookieValues(cookieHeader, name) {
  if (!cookieHeader || !name) return [];
  const values = [];
  for (const part of String(cookieHeader).split(";")) {
    const separator = part.indexOf("=");
    if (separator < 0) continue;
    const key = part.slice(0, separator).trim();
    if (key !== name) continue;
    values.push(part.slice(separator + 1).trim());
  }
  return values;
}

export function sessionTokenFromCookie(cookieHeader, name) {
  const values = cookieValues(cookieHeader, name);
  return values.length === 1 && values[0] ? values[0] : null;
}

export function sessionCookie({ name, token, domain, maxAgeSeconds }) {
  return [
    `${name}=${token}`,
    `Max-Age=${Math.max(0, Math.trunc(maxAgeSeconds))}`,
    `Domain=${domain}`,
    "Path=/",
    "HttpOnly",
    "Secure",
    "SameSite=Strict",
  ].join("; ");
}

export function clearedSessionCookie({ name, domain }) {
  return sessionCookie({ name, token: "", domain, maxAgeSeconds: 0 });
}

function normalizedHost(value) {
  return String(value || "").trim().toLowerCase().replace(/\.$/, "");
}

export function allowedReturnTo(value, { allowedHosts, dashboardOrigin }) {
  const fallback = `${dashboardOrigin}/`;
  const candidate = String(value || "").trim();
  if (!candidate) return fallback;

  if (candidate.startsWith("/") && !candidate.startsWith("//")) {
    return new URL(candidate, dashboardOrigin).toString();
  }

  try {
    const url = new URL(candidate);
    if (url.protocol !== "https:") return fallback;
    if (!allowedHosts.has(normalizedHost(url.hostname))) return fallback;
    url.username = "";
    url.password = "";
    return url.toString();
  } catch {
    return fallback;
  }
}

function sameOrigin(value, expectedOrigin) {
  if (!value) return false;
  try {
    return new URL(value).origin === expectedOrigin;
  } catch {
    return false;
  }
}

export function mutationOriginAllowed({ method, origin, referer, expectedOrigin }) {
  if (!MUTATING_METHODS.has(String(method || "").toUpperCase())) return true;
  if (origin) return sameOrigin(origin, expectedOrigin);
  return sameOrigin(referer, expectedOrigin);
}

export function originalOrigin({ proto, host }) {
  const normalizedProto = String(proto || "").toLowerCase();
  const normalized = normalizedHost(host);
  if (normalizedProto !== "https" || !normalized) return null;
  return `https://${normalized}`;
}

export function isDocumentNavigation(fetchDest) {
  return String(fetchDest || "").toLowerCase() === "document";
}
