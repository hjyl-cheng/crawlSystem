import assert from "node:assert/strict";
import test from "node:test";
import {
  discoveryPressureReason,
  discoveryPressureRecovered,
  discoveryPressureRecoveredForReason,
  proxyUnavailableRatio,
} from "../src/backpressurePolicy.js";

const limits = {
  pauseChannelBacklog: 100,
  resumeChannelBacklog: 40,
  pauseChannelEtaSeconds: 300,
  resumeChannelEtaSeconds: 120,
  pauseChannelFailureRate: 0.2,
  resumeChannelFailureRate: 0.1,
  pauseProxyCooldownRatio: 0.35,
  resumeProxyCooldownRatio: 0.15,
  minimumFailureSamples: 10,
  pauseDetailBacklog: 80,
  resumeDetailBacklog: 30,
  pauseDataApiBacklog: 20,
  resumeDataApiBacklog: 8,
  pauseAgentBacklog: 10,
  resumeAgentBacklog: 4,
};

const healthy = {
  channelBacklog: 10,
  channelEtaSeconds: 20,
  channelTerminalSamples: 50,
  channelFailureRate: 0.02,
  proxyCooldownRatio: 0.05,
  detailBacklog: 0,
  dataApiBacklog: 0,
  agentBacklog: 0,
};

test("discovery pressure prioritizes channel ETA and proxy health", () => {
  assert.equal(discoveryPressureReason({ ...healthy, channelEtaSeconds: 301 }, limits), "channel_eta_high");
  assert.equal(discoveryPressureReason({ ...healthy, channelFailureRate: 0.25 }, limits), "channel_failure_rate_high");
  assert.equal(discoveryPressureReason({ ...healthy, proxyCooldownRatio: 0.4 }, limits), "proxy_cooldown_ratio_high");
});

test("failure pressure requires a meaningful terminal sample", () => {
  assert.equal(discoveryPressureReason({ ...healthy, channelTerminalSamples: 2, channelFailureRate: 1 }, limits), null);
});

test("discovery resumes only after all metrics cross hysteresis thresholds", () => {
  assert.equal(discoveryPressureRecovered(healthy, limits), true);
  assert.equal(discoveryPressureRecovered({ ...healthy, channelEtaSeconds: 121 }, limits), false);
  assert.equal(discoveryPressureRecovered({ ...healthy, proxyCooldownRatio: 0.16 }, limits), false);
});

test("discovery recovers the pressure that paused it without waiting on unrelated hysteresis", () => {
  const channelRecoveredWithCoolingProxies = {
    ...healthy,
    channelBacklog: 40,
    proxyCooldownRatio: 0.29,
  };
  assert.equal(
    discoveryPressureRecoveredForReason(
      "channel_backlog_high",
      channelRecoveredWithCoolingProxies,
      limits,
    ),
    true,
  );
  assert.equal(
    discoveryPressureRecoveredForReason(
      "proxy_cooldown_ratio_high",
      channelRecoveredWithCoolingProxies,
      limits,
    ),
    false,
  );
});

test("reason-specific recovery keeps each pressure on its own resume threshold", () => {
  assert.equal(
    discoveryPressureRecoveredForReason(
      "channel_backlog_high",
      { ...healthy, channelBacklog: 41 },
      limits,
    ),
    false,
  );
  assert.equal(
    discoveryPressureRecoveredForReason(
      "agent_backlog_high",
      { ...healthy, agentBacklog: 4, channelBacklog: 90 },
      limits,
    ),
    true,
  );
});

test("proxy pressure includes failed exits awaiting a cooldown retest", () => {
  assert.equal(proxyUnavailableRatio({ active: 13, cooldown: 6, total: 24 }), 11 / 24);
  assert.equal(proxyUnavailableRatio({ active: 21, cooldown: 0, total: 24 }), 3 / 24);
  assert.equal(proxyUnavailableRatio({ active: 8, cooldown: 2 }), 0.2);
});
