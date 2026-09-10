import { createControllerLifecycle } from './controllerLifecycle.js';

// Each task owns its non-reentrancy guard. A slow maintenance task must never
// hold the guard used by intake. Shared resources close only after every task.
export function createControllerWorkLoops({ tasks, closeResources = async () => {}, onResult = () => {}, onError = () => {} }) {
  const timers = new Map();
  let stopping = false;
  let shutdownPromise;
  const loops = new Map(Object.entries(tasks).map(([name, task]) => [name,
    createControllerLifecycle({
      tick: async () => {
        const started = Date.now();
        try {
          const result = await task.run();
          onResult({ name, duration_ms: Date.now() - started, result });
          return result;
        } catch (error) {
          onError({ name, duration_ms: Date.now() - started, error });
          throw error;
        }
      },
      closeResources: async () => {},
    }),
  ]));
  function run(name) {
    if (stopping) return Promise.resolve(false);
    if (!loops.has(name)) throw new Error(`Unknown controller task: ${name}`);
    return loops.get(name).run();
  }
  return {
    run,
    start() {
      if (stopping || timers.size) return;
      for (const [name, task] of Object.entries(tasks)) {
        const wake = () => { void run(name).catch(() => {}); };
        timers.set(name, setInterval(wake, Math.max(100, task.intervalMs ?? 15000)));
        wake();
      }
    },
    shutdown() {
      if (shutdownPromise) return shutdownPromise;
      stopping = true;
      for (const timer of timers.values()) clearInterval(timer);
      timers.clear();
      shutdownPromise = (async () => {
        // All resources must drain even if one task failed during shutdown.
        await Promise.allSettled([...loops.values()].map(loop => loop.shutdown()));
        await closeResources();
      })();
      return shutdownPromise;
    },
  };
}
