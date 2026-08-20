import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import { createControllerLifecycle } from "../src/controllerLifecycle.js";

function deferred() {
  let resolve;
  const promise = new Promise((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

test("shutdown waits for the active Controller Tick before closing shared resources", async () => {
  const gate = deferred();
  const started = deferred();
  const events = [];
  const lifecycle = createControllerLifecycle({
    tick: async () => {
      events.push("tick:start");
      started.resolve();
      await gate.promise;
      events.push("tick:end");
    },
    closeResources: async () => {
      events.push("resources:close");
    },
  });

  const activeTick = lifecycle.run();
  await started.promise;
  const firstShutdown = lifecycle.shutdown();
  const secondShutdown = lifecycle.shutdown();

  assert.equal(firstShutdown, secondShutdown, "shutdown must be idempotent");
  assert.equal(await lifecycle.run(), false, "shutdown must reject new Tick wakeups");
  await Promise.resolve();
  assert.deepEqual(events, ["tick:start"]);

  gate.resolve();
  await Promise.all([activeTick, firstShutdown]);
  assert.deepEqual(events, ["tick:start", "tick:end", "resources:close"]);
});

test("the production Controller delegates Tick and resource shutdown to the lifecycle", async () => {
  const source = await readFile(new URL("../src/controller.js", import.meta.url), "utf8");
  assert.match(source, /createControllerLifecycle\(\{\s*tick: runControllerTick,\s*closeResources: closeControllerResources,/);
  assert.match(source, /shutdownPromise = controllerLifecycle\.shutdown\(\)/);
  assert.match(source, /if \(!controllerLifecycle\.isShuttingDown\(\) && immediateWakeRequested\)/);
});
