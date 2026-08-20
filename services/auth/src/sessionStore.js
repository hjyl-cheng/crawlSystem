import { createHash, randomBytes } from "node:crypto";

function epochSeconds(milliseconds) {
  return Math.floor(milliseconds / 1000);
}

export function hashToken(token) {
  return createHash("sha256").update(String(token)).digest("hex");
}

export function credentialVersion(username, passwordHash) {
  return createHash("sha256")
    .update(String(username))
    .update("\0")
    .update(String(passwordHash))
    .digest("hex");
}

export class SessionStore {
  constructor(redis, {
    keyPrefix = "qy:auth:session:",
    sessionTtlSeconds = 7 * 24 * 60 * 60,
    renewThresholdSeconds = 3 * 24 * 60 * 60,
    credentialVersion: configuredCredentialVersion,
    now = () => Date.now(),
  }) {
    this.redis = redis;
    this.keyPrefix = keyPrefix;
    this.sessionTtlSeconds = sessionTtlSeconds;
    this.renewThresholdSeconds = renewThresholdSeconds;
    this.credentialVersion = configuredCredentialVersion;
    this.now = now;
  }

  key(token) {
    return `${this.keyPrefix}${hashToken(token)}`;
  }

  async create(username) {
    const token = randomBytes(32).toString("base64url");
    const now = epochSeconds(this.now());
    const session = {
      username,
      credential_version: this.credentialVersion,
      created_at: now,
      renewed_at: now,
      expires_at: now + this.sessionTtlSeconds,
    };
    await this.redis.set(
      this.key(token),
      JSON.stringify(session),
      "EX",
      this.sessionTtlSeconds,
    );
    return { token, session };
  }

  async verify(token, { renew = false } = {}) {
    if (!token) return { valid: false, reason: "missing" };
    const key = this.key(token);
    const raw = await this.redis.get(key);
    if (!raw) return { valid: false, reason: "expired" };

    let session;
    try {
      session = JSON.parse(raw);
    } catch {
      await this.redis.del(key);
      return { valid: false, reason: "invalid" };
    }

    const now = epochSeconds(this.now());
    if (session.credential_version !== this.credentialVersion) {
      await this.redis.del(key);
      return { valid: false, reason: "credentials_changed" };
    }
    if (!Number.isFinite(session.expires_at) || session.expires_at <= now) {
      await this.redis.del(key);
      return { valid: false, reason: "expired" };
    }

    const shouldRenew = renew
      && session.expires_at - now <= this.renewThresholdSeconds;
    if (shouldRenew) {
      session = {
        ...session,
        renewed_at: now,
        expires_at: now + this.sessionTtlSeconds,
      };
      await this.redis.set(
        key,
        JSON.stringify(session),
        "EX",
        this.sessionTtlSeconds,
      );
    }

    return {
      valid: true,
      renewed: shouldRenew,
      session,
      maxAgeSeconds: shouldRenew ? this.sessionTtlSeconds : null,
    };
  }

  async revoke(token) {
    if (!token) return false;
    return (await this.redis.del(this.key(token))) > 0;
  }
}

export class LoginLimiter {
  constructor(redis, {
    keyPrefix = "qy:auth:login-failure:",
    maxFailures = 5,
    windowSeconds = 10 * 60,
  } = {}) {
    this.redis = redis;
    this.keyPrefix = keyPrefix;
    this.maxFailures = maxFailures;
    this.windowSeconds = windowSeconds;
  }

  key(ip, username) {
    const identity = hashToken(`${ip}\0${String(username).toLowerCase()}`);
    return `${this.keyPrefix}${identity}`;
  }

  async status(ip, username) {
    const key = this.key(ip, username);
    const count = Number(await this.redis.get(key) || 0);
    const ttl = count > 0 ? Number(await this.redis.ttl(key)) : 0;
    return {
      blocked: count >= this.maxFailures,
      count,
      retryAfterSeconds: Math.max(0, ttl),
    };
  }

  async fail(ip, username) {
    const key = this.key(ip, username);
    const count = Number(await this.redis.incr(key));
    if (count === 1) await this.redis.expire(key, this.windowSeconds);
    return this.status(ip, username);
  }

  async clear(ip, username) {
    await this.redis.del(this.key(ip, username));
  }
}
