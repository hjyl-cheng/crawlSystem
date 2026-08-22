export function integerEnvironment(
  environment,
  name,
  fallback,
  { minimum, maximum },
) {
  const raw = String(environment[name] ?? "").trim();
  if (!raw) return fallback;
  const value = Number(raw);
  if (!Number.isSafeInteger(value) || value < minimum || value > maximum) {
    throw new TypeError(`${name} must be an integer between ${minimum} and ${maximum}`);
  }
  return value;
}

function decimalEnvironment(environment, name, fallback, { minimum, maximum }) {
  const raw = String(environment[name] ?? "").trim();
  if (!raw) return fallback;
  const value = Number(raw);
  if (!Number.isFinite(value) || value < minimum || value > maximum) {
    throw new TypeError(`${name} must be a number between ${minimum} and ${maximum}`);
  }
  return value;
}

function enumEnvironment(environment, name, fallback, allowed) {
  const value = String(environment[name] ?? fallback).trim();
  if (!allowed.includes(value)) {
    throw new TypeError(`${name} must be one of: ${allowed.join(", ")}`);
  }
  return value;
}

export function requiredEnvironment(environment, name) {
  const value = String(environment[name] ?? "").trim();
  if (!value) throw new TypeError(`${name} is required`);
  return value;
}

export function percentile(values, ratio) {
  if (values.length === 0) return null;
  const sorted = values.slice().sort((left, right) => left - right);
  return sorted[Math.floor((sorted.length - 1) * ratio)];
}

function roundedRate(units, milliseconds) {
  if (milliseconds <= 0) return null;
  return Math.round(((units * 1000) / milliseconds) * 1000) / 1000;
}

export class ServiceThroughputMetric {
  constructor() {
    this.units = 0;
    this.productiveMilliseconds = 0;
    this.productiveDurations = [];
  }

  observe({ durationMs, units, productive }) {
    const duration = Math.max(0, Number(durationMs) || 0);
    const unitCount = Math.max(0, Number(units) || 0);
    this.units += unitCount;
    if (!productive) return;
    this.productiveMilliseconds += duration;
    this.productiveDurations.push(Math.round(duration));
  }

  report(elapsedMs) {
    return {
      units: this.units,
      productive_runs: this.productiveDurations.length,
      productive_ms: Math.round(this.productiveMilliseconds),
      end_to_end_per_second: roundedRate(this.units, elapsedMs),
      productive_capacity_per_second: roundedRate(
        this.units,
        this.productiveMilliseconds,
      ),
      productive_latency_ms: {
        p50: percentile(this.productiveDurations, 0.5),
        p95: percentile(this.productiveDurations, 0.95),
      },
    };
  }
}

export function parseShmMountBytes(mountLine) {
  const line = String(mountLine ?? "").split("\n").find((candidate) => (
    candidate.split(" ")[1] === "/dev/shm"
  ));
  if (!line) throw new Error("PostgreSQL /dev/shm mount is missing");
  const options = line.split(" ")[3]?.split(",") ?? [];
  const size = options.find((option) => option.startsWith("size="))?.slice(5);
  const match = /^(\d+)([kmgt]?)$/i.exec(size ?? "");
  if (!match) throw new Error("PostgreSQL /dev/shm mount size is missing");
  const multiplier = {
    "": 1,
    k: 1024,
    m: 1024 ** 2,
    g: 1024 ** 3,
    t: 1024 ** 4,
  }[match[2].toLowerCase()];
  return Number(match[1]) * multiplier;
}

export function businessPublicationLoadOptions(environment = process.env) {
  const mode = enumEnvironment(environment, "INC009_SOAK_MODE", "steady", [
    "steady",
    "capacity",
  ]);
  const durationSeconds = integerEnvironment(
    environment,
    "INC009_SOAK_DURATION_SECONDS",
    3600,
    { minimum: 10, maximum: 86400 },
  );
  const defaultMinimumAudits = mode === "steady" && durationSeconds >= 3600 ? 100 : 1;
  const capacityMinimumRates = mode === "capacity" ? {
    publisher_revisions: 42.8,
    ingress_revisions: 81.4,
    reconciler_channels: 26.6,
    projector_channels: 3.98,
  } : {
    publisher_revisions: 0,
    ingress_revisions: 0,
    reconciler_channels: 0,
    projector_channels: 0,
  };
  return {
    mode,
    durationSeconds,
    channelsPerMinute: integerEnvironment(
      environment,
      "INC009_SOAK_CHANNELS_PER_MINUTE",
      12,
      { minimum: 1, maximum: 600 },
    ),
    capacityChannels: integerEnvironment(
      environment,
      "INC009_CAPACITY_CHANNELS",
      300,
      { minimum: 10, maximum: 10000 },
    ),
    auditIntervalSeconds: integerEnvironment(
      environment,
      "INC009_SOAK_AUDIT_INTERVAL_SECONDS",
      30,
      { minimum: 0, maximum: 3600 },
    ),
    minimumAudits: integerEnvironment(
      environment,
      "INC009_SOAK_MINIMUM_AUDITS",
      defaultMinimumAudits,
      { minimum: 1, maximum: 100000 },
    ),
    drainTimeoutSeconds: integerEnvironment(
      environment,
      "INC009_SOAK_DRAIN_TIMEOUT_SECONDS",
      300,
      { minimum: 10, maximum: 3600 },
    ),
    expectedShmMib: integerEnvironment(
      environment,
      "INC009_SOAK_EXPECTED_SHM_MIB",
      64,
      { minimum: 16, maximum: 65536 },
    ),
    expectedPostgresVersion: String(
      environment.INC009_SOAK_EXPECTED_POSTGRES_VERSION ?? "18.4",
    ).trim(),
    minimumRates: {
      publisher_revisions: decimalEnvironment(
        environment,
        "INC009_MIN_PUBLISHER_REVISIONS_PER_SECOND",
        capacityMinimumRates.publisher_revisions,
        { minimum: 0, maximum: 1000000 },
      ),
      ingress_revisions: decimalEnvironment(
        environment,
        "INC009_MIN_INGRESS_REVISIONS_PER_SECOND",
        capacityMinimumRates.ingress_revisions,
        { minimum: 0, maximum: 1000000 },
      ),
      reconciler_channels: decimalEnvironment(
        environment,
        "INC009_MIN_RECONCILER_CHANNELS_PER_SECOND",
        capacityMinimumRates.reconciler_channels,
        { minimum: 0, maximum: 1000000 },
      ),
      projector_channels: decimalEnvironment(
        environment,
        "INC009_MIN_PROJECTOR_CHANNELS_PER_SECOND",
        capacityMinimumRates.projector_channels,
        { minimum: 0, maximum: 1000000 },
      ),
    },
  };
}
