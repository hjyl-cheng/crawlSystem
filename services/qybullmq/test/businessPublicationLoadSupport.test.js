import assert from "node:assert/strict";
import test from "node:test";
import {
  ServiceThroughputMetric,
  businessPublicationLoadOptions,
  parseShmMountBytes,
} from "../scripts/businessPublicationLoadSupport.mjs";

test("Business Publication capacity mode has independent role regression thresholds", () => {
  const options = businessPublicationLoadOptions({
    INC009_SOAK_MODE: "capacity",
    INC009_CAPACITY_CHANNELS: "300",
    INC009_SOAK_EXPECTED_SHM_MIB: "64",
    INC009_MIN_PUBLISHER_REVISIONS_PER_SECOND: "120.5",
    INC009_MIN_INGRESS_REVISIONS_PER_SECOND: "125.5",
    INC009_MIN_RECONCILER_CHANNELS_PER_SECOND: "40.5",
    INC009_MIN_PROJECTOR_CHANNELS_PER_SECOND: "35.5",
  });

  assert.equal(options.mode, "capacity");
  assert.equal(options.capacityChannels, 300);
  assert.equal(options.minimumAudits, 1);
  assert.equal(options.expectedShmMib, 64);
  assert.deepEqual(options.minimumRates, {
    publisher_revisions: 120.5,
    ingress_revisions: 125.5,
    reconciler_channels: 40.5,
    projector_channels: 35.5,
  });
});

test("Business Publication capacity defaults enforce 90 percent of the measured baseline", () => {
  const options = businessPublicationLoadOptions({ INC009_SOAK_MODE: "capacity" });

  assert.equal(options.capacityChannels, 300);
  assert.deepEqual(options.minimumRates, {
    publisher_revisions: 42.8,
    ingress_revisions: 81.4,
    reconciler_channels: 26.6,
    projector_channels: 3.98,
  });
  assert.deepEqual(
    businessPublicationLoadOptions({}).minimumRates,
    {
      publisher_revisions: 0,
      ingress_revisions: 0,
      reconciler_channels: 0,
      projector_channels: 0,
    },
  );
});

test("Service throughput separates paced arrival rate from productive capacity", () => {
  const metric = new ServiceThroughputMetric();
  metric.observe({ durationMs: 100, units: 10, productive: true });
  metric.observe({ durationMs: 200, units: 20, productive: true });
  metric.observe({ durationMs: 50, units: 0, productive: false });

  assert.deepEqual(metric.report(1_000), {
    units: 30,
    productive_runs: 2,
    productive_ms: 300,
    end_to_end_per_second: 30,
    productive_capacity_per_second: 100,
    productive_latency_ms: { p50: 100, p95: 100 },
  });
});

test("Business Publication load reads the PostgreSQL container shm mount exactly", () => {
  assert.equal(parseShmMountBytes(
    "shm /dev/shm tmpfs rw,nosuid,nodev,size=65536k,inode64 0 0",
  ), 64 * 1024 * 1024);
  assert.equal(parseShmMountBytes(
    "shm /dev/shm tmpfs rw,nosuid,nodev,size=256m,inode64 0 0",
  ), 256 * 1024 * 1024);
  assert.throws(() => parseShmMountBytes("overlay / overlay rw 0 0"), /\/dev\/shm/);
});
