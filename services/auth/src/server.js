import { readFile } from "node:fs/promises";
import { timingSafeEqual } from "node:crypto";
import bcrypt from "bcryptjs";
import express from "express";
import Redis from "ioredis";
import { loginPage } from "./loginPage.js";
import {
  allowedReturnTo,
  clearedSessionCookie,
  isDocumentNavigation,
  mutationOriginAllowed,
  originalOrigin,
  sessionCookie,
  sessionTokenFromCookie,
} from "./httpPolicy.js";
import { credentialVersion, LoginLimiter, SessionStore } from "./sessionStore.js";

async function requiredSecret(path, label) {
  const value = (await readFile(path, "utf8")).trim();
  if (!value) throw new Error(`${label} is empty`);
  return value;
}

function positiveInteger(value, fallback, label) {
  const parsed = Number(value ?? fallback);
  if (!Number.isInteger(parsed) || parsed <= 0) throw new Error(`${label} must be a positive integer`);
  return parsed;
}

function sameText(left, right) {
  const a = Buffer.from(String(left));
  const b = Buffer.from(String(right));
  return a.length === b.length && timingSafeEqual(a, b);
}

function requestMetadata(req) {
  const proto = req.get("x-original-proto") || req.protocol;
  const host = req.get("x-original-host") || req.get("host");
  return {
    proto,
    host,
    origin: req.get("x-original-origin") || req.get("origin"),
    referer: req.get("x-original-referer") || req.get("referer"),
    method: req.get("x-original-method") || req.method,
    uri: req.get("x-original-uri") || req.originalUrl,
    fetchDest: req.get("x-original-fetch-dest") || req.get("sec-fetch-dest"),
    clientIp: req.get("x-client-ip") || req.ip,
  };
}

function noStore(res) {
  res.set("Cache-Control", "no-store");
  res.set("Pragma", "no-cache");
}

const config = {
  port: positiveInteger(process.env.PORT, 3000, "PORT"),
  username: await requiredSecret(
    process.env.QY_AUTH_USERNAME_FILE || "/run/qy-auth-secrets/username",
    "username",
  ),
  passwordHash: await requiredSecret(
    process.env.QY_AUTH_PASSWORD_HASH_FILE || "/run/qy-auth-secrets/password_hash",
    "password hash",
  ),
  cookieName: process.env.QY_AUTH_COOKIE_NAME || "__Secure-qy_session",
  cookieDomain: process.env.QY_AUTH_COOKIE_DOMAIN || "example.test",
  dashboardOrigin: process.env.QY_AUTH_DASHBOARD_ORIGIN || "https://qydashboard.example.test",
  allowedHosts: new Set(
    String(process.env.QY_AUTH_ALLOWED_HOSTS || [
      "qydashboard.example.test",
      "qybullmq.example.test",
      "qyproxy.example.test",
      "qyminio.example.test",
    ].join(","))
      .split(",")
      .map((value) => value.trim().toLowerCase())
      .filter(Boolean),
  ),
  sessionTtlSeconds: positiveInteger(process.env.QY_AUTH_SESSION_TTL_SECONDS, 604800, "session TTL"),
  renewThresholdSeconds: positiveInteger(process.env.QY_AUTH_RENEW_THRESHOLD_SECONDS, 259200, "renew threshold"),
  loginMaxFailures: positiveInteger(process.env.QY_AUTH_LOGIN_MAX_FAILURES, 5, "login max failures"),
  loginWindowSeconds: positiveInteger(process.env.QY_AUTH_LOGIN_WINDOW_SECONDS, 600, "login window"),
};

if (!/^\$2[aby]\$/.test(config.passwordHash)) {
  throw new Error("password hash must be bcrypt");
}
if (config.renewThresholdSeconds >= config.sessionTtlSeconds) {
  throw new Error("renew threshold must be shorter than session TTL");
}

const redis = new Redis({
  host: process.env.REDIS_HOST || "127.0.0.1",
  port: Number(process.env.REDIS_PORT || 6379),
  password: process.env.REDIS_PASSWORD || undefined,
  keyPrefix: process.env.REDIS_KEY_PREFIX || "",
  maxRetriesPerRequest: 1,
  enableReadyCheck: true,
});
const sessions = new SessionStore(redis, {
  sessionTtlSeconds: config.sessionTtlSeconds,
  renewThresholdSeconds: config.renewThresholdSeconds,
  credentialVersion: credentialVersion(config.username, config.passwordHash),
});
const limiter = new LoginLimiter(redis, {
  maxFailures: config.loginMaxFailures,
  windowSeconds: config.loginWindowSeconds,
});

function token(req) {
  return sessionTokenFromCookie(req.get("cookie"), config.cookieName);
}

function setSessionCookie(res, value, maxAgeSeconds = config.sessionTtlSeconds) {
  res.set("Set-Cookie", sessionCookie({
    name: config.cookieName,
    token: value,
    domain: config.cookieDomain,
    maxAgeSeconds,
  }));
}

function clearSessionCookie(res) {
  res.set("Set-Cookie", clearedSessionCookie({
    name: config.cookieName,
    domain: config.cookieDomain,
  }));
}

function expectedOrigin(metadata) {
  return originalOrigin(metadata);
}

function validMutationOrigin(metadata) {
  const expected = expectedOrigin(metadata);
  return expected && mutationOriginAllowed({
    method: metadata.method,
    origin: metadata.origin,
    referer: metadata.referer,
    expectedOrigin: expected,
  });
}

function returnTo(value) {
  return allowedReturnTo(value, config);
}

const app = express();
app.disable("x-powered-by");
app.set("trust proxy", 1);
app.use(express.urlencoded({ extended: false, limit: "16kb" }));
app.use((req, res, next) => {
  const started = Date.now();
  res.on("finish", () => {
    console.log(JSON.stringify({
      method: req.method,
      path: req.path,
      status: res.statusCode,
      duration_ms: Date.now() - started,
      client_ip: req.get("x-client-ip") || req.ip,
    }));
  });
  next();
});

app.get("/healthz", async (_req, res) => {
  try {
    const result = await redis.ping();
    res.json({ ok: result === "PONG", redis: "ok" });
  } catch (error) {
    res.status(503).json({ ok: false, redis: "unavailable", error: error.message });
  }
});

app.get("/login", async (req, res) => {
  noStore(res);
  const destination = returnTo(req.query.return_to);
  try {
    const verified = await sessions.verify(token(req));
    if (verified.valid) return res.redirect(303, destination);
  } catch (error) {
    console.error("login session lookup failed", error);
  }
  return res.type("html").send(loginPage({
    returnTo: destination,
    loggedOut: req.query.logged_out === "1",
  }));
});

app.post("/auth/login", async (req, res) => {
  noStore(res);
  const metadata = requestMetadata(req);
  const destination = returnTo(req.body?.return_to);
  if (!validMutationOrigin(metadata) || expectedOrigin(metadata) !== config.dashboardOrigin) {
    return res.status(403).type("html").send(loginPage({
      returnTo: destination,
      error: "登录请求来源无效",
    }));
  }

  const username = String(req.body?.username || "").trim();
  const password = String(req.body?.password || "");
  const limit = await limiter.status(metadata.clientIp, username);
  if (limit.blocked) {
    res.set("Retry-After", String(limit.retryAfterSeconds));
    return res.status(429).type("html").send(loginPage({
      returnTo: destination,
      error: "登录失败次数过多，请稍后重试",
    }));
  }

  const passwordMatches = password.length <= 1024
    ? await bcrypt.compare(password, config.passwordHash)
    : false;
  if (!sameText(username, config.username) || !passwordMatches) {
    await limiter.fail(metadata.clientIp, username);
    return res.status(401).type("html").send(loginPage({
      returnTo: destination,
      error: "账号或密码错误",
    }));
  }

  await limiter.clear(metadata.clientIp, username);
  const created = await sessions.create(config.username);
  setSessionCookie(res, created.token);
  return res.redirect(303, destination);
});

app.get("/auth/verify", async (req, res) => {
  noStore(res);
  const metadata = requestMetadata(req);
  if (!validMutationOrigin(metadata)) return res.sendStatus(403);

  try {
    const currentToken = token(req);
    const verified = await sessions.verify(currentToken, {
      renew: req.get("x-auth-renew") === "1",
    });
    if (!verified.valid) {
      res.set("X-Auth-Reason", verified.reason);
      return res.sendStatus(401);
    }
    res.set("X-Authenticated-User", verified.session.username);
    if (verified.renewed) {
      setSessionCookie(res, currentToken, verified.maxAgeSeconds);
      res.set("X-Auth-Renewed", "1");
    }
    return res.sendStatus(204);
  } catch (error) {
    console.error("session verification failed", error);
    return res.sendStatus(503);
  }
});

app.get("/auth/unauthorized", (req, res) => {
  noStore(res);
  const metadata = requestMetadata(req);
  if (!isDocumentNavigation(metadata.fetchDest)) {
    return res.status(401).json({ error: "authentication_required" });
  }
  const destination = returnTo(`https://${metadata.host}${metadata.uri}`);
  const loginUrl = new URL("/login", config.dashboardOrigin);
  loginUrl.searchParams.set("return_to", destination);
  return res.redirect(302, loginUrl.toString());
});

app.post("/auth/logout", async (req, res) => {
  noStore(res);
  const metadata = requestMetadata(req);
  if (!validMutationOrigin(metadata) || expectedOrigin(metadata) !== config.dashboardOrigin) {
    return res.sendStatus(403);
  }
  try {
    await sessions.revoke(token(req));
  } finally {
    clearSessionCookie(res);
  }
  return res.redirect(303, "/login?logged_out=1");
});

app.use((_req, res) => res.sendStatus(404));
app.use((error, _req, res, _next) => {
  console.error(error);
  res.sendStatus(500);
});

const server = app.listen(config.port, "0.0.0.0", () => {
  console.log(`qy auth gateway listening on :${config.port}`);
});

async function shutdown(signal) {
  console.log(`received ${signal}, shutting down`);
  server.close(async () => {
    await redis.quit();
    process.exit(0);
  });
}

process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);
