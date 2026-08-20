export function createControllerLifecycle({ tick, closeResources } = {}) {
  if (typeof tick !== "function") throw new TypeError("tick is required");
  if (typeof closeResources !== "function") throw new TypeError("closeResources is required");

  let activeTick = null;
  let shuttingDown = false;
  let shutdownPromise = null;

  function run() {
    if (shuttingDown) return Promise.resolve(false);
    if (activeTick) return activeTick;
    activeTick = Promise.resolve()
      .then(tick)
      .finally(() => {
        activeTick = null;
      });
    return activeTick;
  }

  function shutdown() {
    if (shutdownPromise) return shutdownPromise;
    shuttingDown = true;
    shutdownPromise = (async () => {
      let tickError = null;
      try {
        if (activeTick) await activeTick;
      } catch (error) {
        tickError = error;
      }
      await closeResources();
      if (tickError) throw tickError;
    })();
    return shutdownPromise;
  }

  return {
    run,
    shutdown,
    isRunning: () => activeTick != null,
    isShuttingDown: () => shuttingDown,
  };
}
