export class ManagedRequestTracker {
  constructor() {
    this.active = 0;
    this.accepting = true;
    this.waiters = new Set();
  }

  begin() {
    if (!this.accepting) throw new Error("identity runtime is quiescing");
    this.active += 1;
    let completed = false;
    return () => {
      if (completed) return;
      completed = true;
      this.active = Math.max(0, this.active - 1);
      if (this.active !== 0) return;
      for (const resolve of this.waiters) resolve();
      this.waiters.clear();
    };
  }

  reset() {
    if (this.active !== 0) throw new Error("cannot reuse an identity runtime with active requests");
    this.accepting = true;
  }

  async quiesce() {
    this.accepting = false;
    if (this.active !== 0) {
      await new Promise((resolve) => this.waiters.add(resolve));
    }
    return { active_managed_requests: this.active };
  }
}
