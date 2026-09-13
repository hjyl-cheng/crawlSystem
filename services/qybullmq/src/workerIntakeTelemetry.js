// Only named, managed consumers participate in this admission protocol. Legacy
// workers stay visible to the dispatcher during a rolling upgrade.
export const intakeWorkerName = (kind, id) => `intake-${kind}-${id}`;
export async function publishWorkerIntake(worker, accepting) {
  const redis = await worker.client;
  await redis.set(worker.toKey(`intake:${worker.opts.name}`), accepting ? '1' : '0', 'EX', 15);
}
