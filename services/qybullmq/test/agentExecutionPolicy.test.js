import assert from "node:assert/strict";
import test from "node:test";

import {
  automaticLocalAgentConfigs,
  selectAgentConfigForJob,
} from "../src/agentExecutionPolicy.js";

const LOCAL = {
  config_id: 21,
  provider: "local-offline",
  model: "qy-channel-profile",
  enabled: true,
};
const GROK = {
  config_id: 9,
  provider: "openai-compatible",
  model: "grok",
  enabled: true,
};

test("automatic jobs use local offline config even when an old Grok config ID is queued", () => {
  const selected = selectAgentConfigForJob({
    job: { data: { agent_config_id: GROK.config_id } },
    requestedConfig: GROK,
    localConfig: LOCAL,
  });

  assert.equal(selected, LOCAL);
});

test("external Agent execution requires an explicit opt-in marker and config", () => {
  const selected = selectAgentConfigForJob({
    job: {
      data: {
        agent_config_id: GROK.config_id,
        external_agent_opt_in: true,
      },
    },
    requestedConfig: GROK,
    localConfig: LOCAL,
  });
  assert.equal(selected, GROK);

  assert.throws(() => selectAgentConfigForJob({
    job: { data: { external_agent_opt_in: true } },
    requestedConfig: null,
    localConfig: LOCAL,
  }), /explicit external Agent config is required/);
});

test("automatic controller capacity includes only enabled local offline configs", () => {
  assert.deepEqual(
    automaticLocalAgentConfigs([GROK, LOCAL, { ...LOCAL, config_id: 22, enabled: false }]),
    [LOCAL],
  );
});
