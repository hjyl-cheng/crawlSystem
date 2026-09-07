import { createHash, randomUUID as nodeRandomUUID } from "node:crypto";

const READY_KEEP_ROUTE = "READY_KEEP_ROUTE";
const READY_NEW_ROUTE = "READY_NEW_ROUTE";
const PENDING_NEW_ROUTE = "PENDING_NEW_ROUTE";
const PAUSED_NO_RESERVE = "PAUSED_NO_RESERVE";
const RETRYABLE_OBSERVATIONS = new Set([
  "proxy_transport",
  "youtube_rate_limited",
  "youtube_challenge",
]);

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

export class RotaSlotDeferredError extends Error {
  constructor(reason, { retryAfterMs = 5000 } = {}) {
    super(`Rota Slot job deferred: ${reason}`);
    this.name = "RotaSlotDeferredError";
    this.reason = reason;
    this.retryAfterMs = retryAfterMs;
  }
}

export class RotaSlotContractError extends Error {
  constructor(message) {
    super(message);
    this.name = "RotaSlotContractError";
  }
}

export class RotaExecutionBudgetExhaustedError extends Error {
  constructor(cause = null) {
    super("Rota Execution Route budget exhausted", cause ? { cause } : undefined);
    this.name = "RotaExecutionBudgetExhaustedError";
    this.code = "EXECUTION_ROUTE_BUDGET_EXHAUSTED";
    this.reason = "execution_route_budget_exhausted";
    this.payload = cause?.payload ?? null;
  }
}

export class RotaBusinessRunBudgetExhaustedError extends Error {
  constructor(cause = null) {
    super("Rota Business Run budget exhausted", cause ? { cause } : undefined);
    this.name = "RotaBusinessRunBudgetExhaustedError";
    this.code = "BUSINESS_RUN_BUDGET_EXHAUSTED";
    this.reason = "business_run_budget_exhausted";
    this.payload = cause?.payload ?? null;
  }
}

function positiveInteger(value, field) {
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed <= 0) {
    throw new RotaSlotContractError(`${field} must be a positive integer`);
  }
  return parsed;
}

function nonNegativeInteger(value, field) {
  if (value === null || value === undefined || value === "") {
    throw new RotaSlotContractError(`${field} must be a non-negative integer`);
  }
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < 0) {
    throw new RotaSlotContractError(`${field} must be a non-negative integer`);
  }
  return parsed;
}

function requiredString(value, field) {
  const normalized = String(value ?? "").trim();
  if (!normalized) throw new RotaSlotContractError(`${field} is required`);
  return normalized;
}

function isLeaseGone(error) {
  const seen = new Set();
  for (let current = error; current && !seen.has(current); current = current?.cause) {
    seen.add(current);
    if (String(current?.code ?? "").trim().toUpperCase() === "LEASE_GONE") return true;
  }
  return false;
}

function isLeaseConflict(error) {
  const seen = new Set();
  for (let current = error; current && !seen.has(current); current = current?.cause) {
    seen.add(current);
    if (String(current?.code ?? "").trim().toUpperCase() === "LEASE_CONFLICT") return true;
  }
  return false;
}

function proxyUrl({ baseUrl, proxyUser, password }) {
  const url = new URL(baseUrl);
  if (!["http:", "https:"].includes(url.protocol)) {
    throw new RotaSlotContractError(`unsupported Rota proxy protocol: ${url.protocol}`);
  }
  url.username = requiredString(proxyUser, "assignment.proxy_user");
  url.password = requiredString(password, "ROTA_BULLMQ_PROXY_PASSWORD");
  return url.toString();
}

function jobExecutionId(job) {
  const queue = requiredString(job?.queueName, "job.queueName");
  const id = requiredString(job?.id, "job.id");
  const generation = positiveInteger(
    job?.data?.dispatch_generation,
    "job.data.dispatch_generation",
  );
  const attempt = positiveInteger(job?.attemptsStarted, "job.attemptsStarted");
  const digest = createHash("sha256")
    .update(JSON.stringify([queue, id, generation, attempt]))
    .digest("hex");
  return `exec:v1:${digest}`;
}

function completionOutcome(result) {
  if (result?.kind === "retryable_network_failure") return "failed";
  if (result?.kind === "cancelled") return "cancelled";
  if (result?.kind === "business_terminal") return "success";
  if (result?.kind === "managed_work_complete") return "success";
  throw new RotaSlotContractError(`unsupported attempt result kind: ${result?.kind ?? "missing"}`);
}

function businessComplete(result) {
  if (result?.kind === "business_terminal") return true;
  return result?.kind === "managed_work_complete" && result.businessState === "terminal";
}

function cloneAssignment(value) {
  return value ? Object.freeze({ ...value }) : null;
}

class ControlCommandLane {
  constructor() {
    this.tail = Promise.resolve();
    this.sequence = 0;
    this.closed = false;
  }

  enqueue(kind, command, { allowClosing = false } = {}) {
    if (this.closed && !allowClosing) {
      return Promise.reject(new RotaSlotContractError(`control lane is closing; cannot enqueue ${kind}`));
    }
    const sequence = ++this.sequence;
    const result = this.tail.then(() => command(sequence));
    this.tail = result.catch(() => {});
    return result;
  }

  beginClose() {
    this.closed = true;
  }

  drain() {
    return this.tail;
  }
}

export class RotaSlotAdapter {
  constructor({
    client,
    role,
    workerId,
    workerInstanceId = nodeRandomUUID(),
    resolvedPolicy,
    proxyBaseUrl,
    proxyPassword,
    identityRuntime,
    renewIntervalMs = 5000,
    leaseSafetyMarginMs = 10_000,
    routeReadyWaitMs = 10_000,
    maxRouteSwitchesPerExecution = 2,
    maxCompletionAttempts = 3,
    completionRetryDelayMs = 100,
    sleepImpl = sleep,
    randomUUID = nodeRandomUUID,
    monotonicNow = () => performance.now(),
    setTimeoutImpl = setTimeout,
    clearTimeoutImpl = clearTimeout,
  } = {}) {
    this.client = client;
    this.role = requiredString(role, "role").toLowerCase();
    this.workerId = requiredString(workerId, "workerId");
    this.workerInstanceId = requiredString(workerInstanceId, "workerInstanceId");
    this.resolvedPolicy = resolvedPolicy;
    this.policy = resolvedPolicy?.policy;
    this.proxyBaseUrl = requiredString(proxyBaseUrl, "proxyBaseUrl");
    this.proxyPassword = requiredString(proxyPassword, "proxyPassword");
    this.identityRuntime = identityRuntime;
    this.renewIntervalMs = Math.max(1000, Number(renewIntervalMs) || 5000);
    this.leaseSafetyMarginMs = Math.max(0, Number(leaseSafetyMarginMs) || 0);
    this.routeReadyWaitMs = Math.max(1, Number(routeReadyWaitMs) || 10_000);
    this.maxRouteSwitchesPerExecution = Math.max(0, Number(maxRouteSwitchesPerExecution) || 0);
    this.maxCompletionAttempts = Math.max(1, Number(maxCompletionAttempts) || 3);
    this.completionRetryDelayMs = Math.max(1, Number(completionRetryDelayMs) || 100);
    this.sleepImpl = sleepImpl;
    this.randomUUID = randomUUID;
    this.monotonicNow = monotonicNow;
    this.setTimeoutImpl = setTimeoutImpl;
    this.clearTimeoutImpl = clearTimeoutImpl;
    this.lane = new ControlCommandLane();
    this.assignment = null;
    this.slotReady = false;
    this.controlState = null;
    this.leaseSafeUntil = 0;
    this.activeTask = null;
    this.activeRuntime = null;
    this.activeAttemptController = null;
    this.pendingCompletion = null;
    this.completionUncertain = false;
    this.completionRecoveryPromise = null;
    this.idleRuntime = null;
    this.renewTimer = null;
    this.renewPromise = null;
    this.reclaimPromise = null;
    this.started = false;
    this.closing = false;
    this.activeJob = false;
    this.activeJobCompletion = null;
    this.closePromise = null;
  }

  async start() {
    if (this.started) return this.status();
    if (!this.client || !this.policy || this.policy.role !== this.role) {
      throw new RotaSlotContractError("Rota Slot Adapter requires a role-matched identity policy");
    }
    let result;
    do {
      const requestStarted = this.monotonicNow();
      result = await this.lane.enqueue("claim", () => this.client.claim({
        claim_request_id: this.randomUUID(),
        protocol_version: 2,
        role: this.role,
        worker_id: this.workerId,
        worker_instance_id: this.workerInstanceId,
        identity_policy_id: this.policy.id,
        identity_policy_version: this.policy.version,
      }));
      if (!result?.ready) {
        const retryAfterMs = Math.max(100, Number(result?.retry_after_ms) || 1000);
        await this.sleepImpl(retryAfterMs);
      } else {
        this.#acceptAssignment(result, { requestStarted, expectedGeneration: null });
      }
    } while (!result?.ready && !this.closing);
    if (this.closing) throw new RotaSlotContractError("Rota Slot Adapter closed during Claim");
    this.started = true;
    this.#scheduleRenew();
    return this.status();
  }

  async executeJob(job, { prepare, executeAttempt } = {}) {
    if (!this.started || !this.assignment || !this.slotReady || this.closing) {
      throw new RotaSlotDeferredError("slot_not_ready");
    }
    if (this.activeJob) throw new RotaSlotDeferredError("local_capacity");
    if (typeof prepare !== "function" || typeof executeAttempt !== "function") {
      throw new TypeError("prepare and executeAttempt callbacks are required");
    }
    this.activeJob = true;
    let resolveActiveJob;
    const activeJobDone = new Promise((resolve) => { resolveActiveJob = resolve; });
    this.activeJobCompletion = { promise: activeJobDone, resolve: resolveActiveJob };
    try {
      const prepared = await prepare();
      if (prepared?.kind === "skip") return prepared.result;
      if (prepared?.kind === "defer") {
        throw new RotaSlotDeferredError(prepared.reason, {
          retryAfterMs: Math.max(1, Date.parse(prepared.retryAt) - Date.now()),
        });
      }
      this.#validatePrepared(prepared);

      const executionId = jobExecutionId(job);
      let routeSwitches = 0;
      let resumeMode = prepared.initialResumeMode ?? "initial";
      let beginTaskLeaseConflictRecovered = false;
      while (!this.closing) {
        this.#requireLeaseSafety();
        const frozen = this.assignment;
        let task;
        try {
          task = await this.#beginTask({ prepared, executionId, frozen });
        } catch (error) {
          if (!isLeaseConflict(error)) throw error;
          if (beginTaskLeaseConflictRecovered) {
            this.#fenceSlot("LEASE_CONFLICT_UNRESOLVED", error, { preserveLeaseSafety: true });
            const reclaimed = await this.#reclaimAssignment(frozen);
            if (reclaimed && this.slotReady) continue;
            throw new RotaSlotDeferredError("lease_reclaiming");
          }
          beginTaskLeaseConflictRecovered = true;
          await this.#recoverBeginTaskLeaseConflict(frozen, error);
          continue;
        }
        this.activeTask = task;
        const controller = new AbortController();
        this.activeAttemptController = controller;
        const reusableRuntime = this.idleRuntime;
        this.idleRuntime = null;
        let runtime;
        try {
          runtime = await this.identityRuntime.acquire({
            assignment: frozen,
            policy: this.policy,
            proxyUrl: proxyUrl({
              baseUrl: this.proxyBaseUrl,
              proxyUser: frozen.proxy_user,
              password: this.proxyPassword,
            }),
            task,
            prepared,
            abortSignal: controller.signal,
            reusableRuntime,
          });
        } catch (error) {
          await this.#completeTask({
            prepared,
            frozen,
            task,
            outcome: "failed",
            durationMs: 0,
            businessComplete: false,
            observationIDs: [],
            activeManagedRequests: 0,
          });
          this.activeTask = null;
          this.activeAttemptController = null;
          throw error;
        }
        this.activeRuntime = runtime;
        const startedAt = this.monotonicNow();
        let attemptResult;
        let thrown = null;
        const attemptContext = Object.freeze({
          businessRunId: prepared.businessRunId,
          jobExecutionId: executionId,
          number: task.attempt_number,
          resumeMode,
          abortSignal: controller.signal,
          routeGeneration: frozen.route_generation,
          getBudget: () => this.client.businessRunBudget(prepared.businessRunId),
        });
        try {
          const invoke = () => executeAttempt(prepared, attemptContext);
          attemptResult = typeof runtime?.execute === "function"
            ? await runtime.execute({ job, prepared, attempt: attemptContext }, invoke)
            : await invoke();
          if (this.closing && controller.signal.aborted) {
            attemptResult = { kind: "cancelled", error: controller.signal.reason };
          }
        } catch (error) {
          thrown = error;
          attemptResult = this.closing && controller.signal.aborted
            ? { kind: "cancelled", error: controller.signal.reason ?? error }
            : { kind: "unexpected_failure", error };
        }

        const quiesced = await this.identityRuntime.quiesce(runtime, controller.signal);
        const observationIDs = [];
        if (attemptResult.kind === "retryable_network_failure") {
          if (!attemptResult.checkpointPersisted || !RETRYABLE_OBSERVATIONS.has(attemptResult.observation)) {
            throw new RotaSlotContractError("retryable network failure lacks a durable checkpoint or valid observation");
          }
          observationIDs.push(await this.#observe({
            prepared,
            frozen,
            task,
            result: attemptResult,
          }));
        }
        const outcome = attemptResult.kind === "unexpected_failure"
          ? "failed"
          : completionOutcome(attemptResult);
        const completion = await this.#completeTask({
          prepared,
          frozen,
          task,
          outcome,
          durationMs: Math.max(0, Math.round(this.monotonicNow() - startedAt)),
          businessComplete: businessComplete(attemptResult),
          observationIDs,
          activeManagedRequests: Number(quiesced?.active_managed_requests ?? 0),
        });
        this.activeTask = null;
        this.activeAttemptController = null;

        if (attemptResult.kind === "managed_work_complete" || attemptResult.kind === "business_terminal") {
          await this.identityRuntime.checkpoint(runtime, attemptResult);
          this.activeRuntime = null;
          this.idleRuntime = runtime;
          if (attemptResult.kind === "business_terminal") return attemptResult.result ?? null;
          return attemptResult.result;
        }
        if (attemptResult.kind === "unexpected_failure") {
          await this.identityRuntime.retire(runtime, frozen);
          this.activeRuntime = null;
          throw thrown;
        }
        if (attemptResult.kind === "cancelled") {
          await this.identityRuntime.retire(runtime, frozen);
          this.activeRuntime = null;
          throw attemptResult.error ?? new RotaSlotDeferredError("adapter_closing");
        }

        await this.identityRuntime.retire(runtime, frozen);
        this.activeRuntime = null;
        routeSwitches += 1;
        await this.#waitForNewRoute(completion, frozen);
        if (routeSwitches > this.maxRouteSwitchesPerExecution) {
          throw new RotaExecutionBudgetExhaustedError();
        }
        resumeMode = "network_attempt_resume";
      }
      throw new RotaSlotDeferredError("adapter_closing");
    } finally {
      this.activeJob = false;
      if (this.activeJobCompletion?.promise === activeJobDone) {
        this.activeJobCompletion = null;
      }
      resolveActiveJob();
    }
  }

  close() {
    if (!this.closePromise) this.closePromise = this.#close();
    return this.closePromise;
  }

  async #close() {
    this.closing = true;
    if (this.renewTimer) this.clearTimeoutImpl(this.renewTimer);
    this.renewTimer = null;
    const activeJob = this.activeJobCompletion?.promise ?? null;
    if (this.activeAttemptController && !this.activeAttemptController.signal.aborted) {
      this.activeAttemptController.abort(new RotaSlotDeferredError("adapter_closing"));
    }
    if (activeJob) await activeJob;
    if (this.completionRecoveryPromise) {
      await this.completionRecoveryPromise.catch(() => {});
    }
    if (this.reclaimPromise) await this.reclaimPromise.catch(() => {});
    let releaseSafe = this.activeTask === null;
    if (this.pendingCompletion) {
      try {
        releaseSafe = await this.#recoverPendingCompletion(
          this.pendingCompletion,
          this.assignment,
        );
      } catch {
        releaseSafe = false;
      }
    }
    this.lane.beginClose();
    if (this.activeRuntime) {
      const runtime = this.activeRuntime;
      const frozen = this.assignment;
      await this.identityRuntime.quiesce(runtime, new AbortController().signal);
      await this.identityRuntime.retire(runtime, frozen);
      this.activeRuntime = null;
    }
    this.activeAttemptController = null;
    if (this.idleRuntime) {
      await this.identityRuntime.retire(this.idleRuntime, this.assignment);
      this.idleRuntime = null;
    }
    if (releaseSafe && this.assignment?.lease_id) {
      const frozen = this.assignment;
      await this.lane.enqueue("release", () => this.client.release({
        release_request_id: this.randomUUID(),
        slot_name: frozen.slot_name,
        worker_id: this.workerId,
        worker_instance_id: this.workerInstanceId,
        lease_id: frozen.lease_id,
        known_route_generation: frozen.route_generation,
        reason: "worker_shutdown",
      }), { allowClosing: true });
    }
    this.assignment = null;
    this.slotReady = false;
    this.controlState = null;
    await this.lane.drain();
  }

  status() {
    return Object.freeze({
      started: this.started,
      closing: this.closing,
      active_job: this.activeJob,
      active_task_id: this.activeTask?.task_id ?? null,
      assignment: this.assignment ? {
        ready: this.slotReady,
        control_state: this.controlState,
        workload_scope: this.assignment.workload_scope,
        role: this.assignment.role,
        slot_name: this.assignment.slot_name,
        lease_id: this.assignment.lease_id,
        route_generation: this.assignment.route_generation,
        credential_generation: this.assignment.credential_generation,
        network_identity_key: this.assignment.network_identity_key,
        profile_epoch: this.assignment.profile_epoch,
        identity_policy_id: this.assignment.identity_policy_id,
        identity_policy_version: this.assignment.identity_policy_version,
        identity_policy_hash: this.assignment.identity_policy_hash,
      } : null,
    });
  }

  #validatePrepared(prepared) {
    if (prepared?.kind !== "ready") throw new RotaSlotContractError("prepare returned an invalid result");
    requiredString(prepared.businessRunId, "prepared.businessRunId");
    requiredString(prepared.workloadKind, "prepared.workloadKind");
    if (prepared.identityPolicyId !== this.policy.id
        || Number(prepared.identityPolicyVersion) !== this.policy.version
        || prepared.identityPolicyHash !== this.policy.hash) {
      throw new RotaSlotDeferredError("policy_unavailable");
    }
  }

  async #beginTask({ prepared, executionId, frozen }) {
    try {
      return await this.lane.enqueue("begin", () => this.client.beginTask({
        slot_name: frozen.slot_name,
        worker_id: this.workerId,
        worker_instance_id: this.workerInstanceId,
        lease_id: frozen.lease_id,
        route_generation: frozen.route_generation,
        attempt_request_id: this.randomUUID(),
        business_run_id: prepared.businessRunId,
        job_execution_id: executionId,
        task_kind: prepared.workloadKind,
      }));
    } catch (error) {
      if (["EXECUTION_ROUTE_BUDGET", "EXECUTION_ROUTE_BUDGET_EXHAUSTED"].includes(error?.code)) {
        throw new RotaExecutionBudgetExhaustedError(error);
      }
      if (["BUSINESS_RUN_BUDGET", "BUSINESS_RUN_BUDGET_EXHAUSTED"].includes(error?.code)) {
        throw new RotaBusinessRunBudgetExhaustedError(error);
      }
      if (error?.code === "ROUTE_NOT_READY") {
        throw new RotaSlotDeferredError("route_not_ready");
      }
      throw error;
    }
  }

  async #recoverBeginTaskLeaseConflict(frozen, conflict) {
    if (this.activeTask || this.pendingCompletion) {
      throw new RotaSlotContractError("BeginTask Lease conflict occurred after a Task became active");
    }
    this.#fenceSlot("LEASE_CONFLICT", conflict, { preserveLeaseSafety: true });

    let renewed;
    try {
      renewed = await this.#renewAssignment(frozen);
    } catch (error) {
      if (isLeaseGone(error)) {
        const reclaimed = await this.#reclaimAssignment(frozen);
        if (reclaimed && this.slotReady) return;
        throw new RotaSlotDeferredError("lease_reclaiming");
      }
      if (isLeaseConflict(error)) {
        const reclaimed = await this.#reclaimAssignment(frozen);
        if (reclaimed && this.slotReady) return;
        throw new RotaSlotDeferredError("lease_reclaiming");
      }
      if (error?.retryable === true) {
        throw new RotaSlotDeferredError("lease_conflict_recovery");
      }
      throw error;
    }

    const current = this.assignment;
    if (current?.slot_name !== frozen.slot_name || current?.lease_id !== frozen.lease_id) {
      if (this.slotReady) return;
      throw new RotaSlotDeferredError("lease_reclaiming");
    }
    if (renewed?.ready === true && this.slotReady) {
      return;
    }
    if (renewed?.ready === true) {
      this.#fenceSlot("LEASE_CONFLICT_UNRESOLVED", conflict, { preserveLeaseSafety: true });
      throw new RotaSlotDeferredError("lease_conflict_recovery");
    }
    throw new RotaSlotDeferredError("lease_conflict_recovery", {
      retryAfterMs: Math.max(1000, Number(renewed?.retry_after_ms) || 1000),
    });
  }

  async #observe({ prepared, frozen, task, result }) {
    const observationID = `${task.task_id}:${this.randomUUID()}`;
    const observed = await this.lane.enqueue("observe", () => this.client.observe({
      slot_name: frozen.slot_name,
      worker_id: this.workerId,
      worker_instance_id: this.workerInstanceId,
      lease_id: frozen.lease_id,
      route_generation: frozen.route_generation,
      task_id: task.task_id,
      business_run_id: prepared.businessRunId,
      observation_id: observationID,
      kind: result.observation,
      source: requiredString(result.source ?? result.failedStage, "observation.source").slice(0, 255),
      occurred_at: new Date().toISOString(),
      payload: { failed_stage: String(result.failedStage ?? "").slice(0, 120) },
    }));
    if (observed.observation_id !== observationID || observed.task_id !== task.task_id) {
      throw new RotaSlotContractError("Rota returned a mismatched Observation receipt");
    }
    return observationID;
  }

  async #completeTask({
    prepared,
    frozen,
    task,
    outcome,
    durationMs,
    businessComplete: completed,
    observationIDs,
    activeManagedRequests,
  }) {
    if (activeManagedRequests !== 0) {
      throw new RotaSlotContractError("identity runtime did not quiesce all managed requests");
    }
    const request = Object.freeze({
      completion_request_id: this.randomUUID(),
      slot_name: frozen.slot_name,
      worker_id: this.workerId,
      worker_instance_id: this.workerInstanceId,
      lease_id: frozen.lease_id,
      route_generation: frozen.route_generation,
      task_id: task.task_id,
      business_run_id: prepared.businessRunId,
      outcome,
      duration_ms: durationMs,
      business_complete: completed,
      observation_ids: [...new Set(observationIDs)].sort(),
      attempt_quiesced: true,
      active_managed_requests: 0,
    });
    this.pendingCompletion = request;
    this.completionUncertain = false;
    try {
      const completed = await this.#sendCompletionRequest(request);
      if (this.pendingCompletion === request) this.pendingCompletion = null;
      this.completionUncertain = false;
      return completed;
    } catch (error) {
      if (isLeaseGone(error)) {
        this.completionUncertain = false;
        this.#fenceSlot("LEASE_GONE", error);
        void this.#reclaimAssignment(frozen).catch(() => {});
      } else {
        this.completionUncertain = true;
        this.#fenceSlot("COMPLETE_UNCERTAIN", error);
      }
      throw error;
    }
  }

  async #sendCompletionRequest(request) {
    let lastError = null;
    for (let attempt = 1; attempt <= this.maxCompletionAttempts; attempt += 1) {
      try {
        const receipt = await this.lane.enqueue("complete", () => this.client.completeTask(request));
        return this.#validateCompletionReceipt(request, receipt);
      } catch (error) {
        lastError = error;
        if (error?.retryable !== true || attempt >= this.maxCompletionAttempts) throw error;
        await this.sleepImpl(this.completionRetryDelayMs * attempt);
      }
    }
    throw lastError;
  }

  #validateCompletionReceipt(request, receipt) {
    if (receipt?.ok !== true || receipt?.task_completed !== true
        || receipt.completion_request_id !== request.completion_request_id
        || receipt.task_id !== request.task_id
        || receipt.slot_name !== request.slot_name
        || receipt.lease_id !== request.lease_id
        || Number(receipt.completed_task_route_generation) !== request.route_generation) {
      throw new RotaSlotContractError("CompleteTask returned a mismatched completion receipt");
    }
    const readyState = receipt.control_state === READY_KEEP_ROUTE
      || receipt.control_state === READY_NEW_ROUTE;
    const waitingState = receipt.control_state === PENDING_NEW_ROUTE
      || receipt.control_state === PAUSED_NO_RESERVE;
    if ((!readyState && !waitingState) || Boolean(receipt.ready) !== readyState) {
      throw new RotaSlotContractError("CompleteTask returned an inconsistent control state");
    }
    return receipt;
  }

  #scheduleCompletionRecovery(frozen) {
    if (this.closing || !this.completionUncertain || !this.pendingCompletion) return null;
    if (this.completionRecoveryPromise) return this.completionRecoveryPromise;
    const request = this.pendingCompletion;
    const recovery = this.#recoverPendingCompletion(request, frozen).catch(async (error) => {
      if (this.pendingCompletion !== request) return false;
      if (isLeaseGone(error)) {
        this.completionUncertain = false;
        this.#fenceSlot("LEASE_GONE", error);
        await this.#reclaimAssignment(frozen);
      } else if (this.assignment?.slot_name === frozen.slot_name
          && this.assignment?.lease_id === frozen.lease_id) {
        this.completionUncertain = true;
        this.#fenceSlot("COMPLETE_UNCERTAIN", error, { preserveLeaseSafety: true });
      }
      return false;
    });
    const shared = recovery.finally(() => {
      if (this.completionRecoveryPromise === shared) this.completionRecoveryPromise = null;
    });
    this.completionRecoveryPromise = shared;
    return shared;
  }

  async #recoverPendingCompletion(request, frozen) {
    const activeJob = this.activeJobCompletion?.promise ?? null;
    if (activeJob) await activeJob;
    if (this.pendingCompletion !== request || !frozen
        || this.assignment?.slot_name !== frozen.slot_name
        || this.assignment?.lease_id !== frozen.lease_id) return false;

    const receipt = await this.#sendCompletionRequest(request);
    if (this.pendingCompletion !== request) return false;
    const runtime = this.activeRuntime;
    if (runtime) await this.identityRuntime.retire(runtime, frozen);
    if (this.activeRuntime === runtime) this.activeRuntime = null;
    this.activeAttemptController = null;
    this.activeTask = null;
    this.pendingCompletion = null;
    this.completionUncertain = false;

    if (this.assignment?.slot_name === frozen.slot_name
        && this.assignment?.lease_id === frozen.lease_id) {
      const canReopen = receipt.control_state === READY_KEEP_ROUTE
        && receipt.ready === true
        && Number(this.assignment.route_generation) === request.route_generation
        && this.monotonicNow() < this.leaseSafeUntil;
      this.slotReady = canReopen;
      this.controlState = receipt.control_state;
    }
    return true;
  }

  async #waitForNewRoute(completion, frozen) {
    if (completion?.slot_name !== frozen.slot_name || completion?.lease_id !== frozen.lease_id) {
      throw new RotaSlotContractError("CompleteTask returned a mismatched Lease fence");
    }
    if (![PENDING_NEW_ROUTE, PAUSED_NO_RESERVE, READY_NEW_ROUTE].includes(completion.control_state)) {
      throw new RotaSlotContractError(`failed Attempt did not enter a new Route state: ${completion.control_state}`);
    }
    const deadline = this.monotonicNow() + this.routeReadyWaitMs;
    let delayMs = Math.max(1, Number(completion.retry_after_ms) || 100);
    while (!this.closing && this.monotonicNow() <= deadline) {
      if (this.slotReady
          && this.assignment?.slot_name === frozen.slot_name
          && this.assignment?.lease_id === frozen.lease_id
          && this.assignment.route_generation > frozen.route_generation) {
        return;
      }
      await this.sleepImpl(delayMs);
      const renewed = await this.#renewAssignment(frozen);
      if (renewed?.ready) {
        if (this.assignment.route_generation < frozen.route_generation + 1) {
          throw new RotaSlotContractError("Rota did not advance the Route generation");
        }
        return;
      }
      delayMs = Math.min(1000, Math.max(100, delayMs * 2));
      if (renewed?.control_state === PAUSED_NO_RESERVE && this.monotonicNow() >= deadline) break;
    }
    throw new RotaSlotDeferredError(
      completion.control_state === PAUSED_NO_RESERVE ? "no_reserve" : "route_not_ready",
      { retryAfterMs: Math.max(1000, Number(completion.retry_after_ms) || 1000) },
    );
  }

  #acceptAssignment(value, { requestStarted, expectedGeneration }) {
    if (!value?.ready || value.protocol_version !== 2) {
      throw new RotaSlotContractError("Rota returned an incomplete or unsupported Assignment");
    }
    const generation = positiveInteger(value.route_generation, "assignment.route_generation");
    const remainingMs = positiveInteger(value.lease_remaining_ms, "assignment.lease_remaining_ms");
    const profileEpoch = nonNegativeInteger(value.profile_epoch, "assignment.profile_epoch");
    const fields = [
      "slot_name", "worker_id", "worker_instance_id", "lease_id", "proxy_user",
      "network_identity_key", "identity_policy_id", "identity_policy_hash",
      "workload_scope", "role",
    ];
    for (const field of fields) requiredString(value[field], `assignment.${field}`);
    if (value.worker_id !== this.workerId || value.worker_instance_id !== this.workerInstanceId
        || value.role !== this.role || value.workload_scope !== this.resolvedPolicy.workload_scope
        || value.identity_policy_id !== this.policy.id
        || Number(value.identity_policy_version) !== this.policy.version
        || value.identity_policy_hash !== this.policy.hash) {
      throw new RotaSlotContractError("Rota Assignment conflicts with the Worker identity policy");
    }
    if (this.assignment) {
      if (value.slot_name !== this.assignment.slot_name || value.lease_id !== this.assignment.lease_id) {
        throw new RotaSlotContractError("Rota changed Slot or Lease during a live Worker Lease");
      }
      if (generation < this.assignment.route_generation) {
        return false;
      }
    }
    if (expectedGeneration !== null && generation < expectedGeneration) {
      throw new RotaSlotContractError("Rota did not advance the Route generation");
    }
    this.assignment = cloneAssignment({
      ...value,
      route_generation: generation,
      profile_epoch: profileEpoch,
    });
    this.slotReady = true;
    this.controlState = value.control_state;
    this.leaseSafeUntil = requestStarted + Math.max(0, remainingMs - this.leaseSafetyMarginMs);
    return true;
  }

  #fenceSlot(
    controlState,
    reason = new RotaSlotDeferredError("slot_not_ready"),
    { preserveLeaseSafety = false } = {},
  ) {
    this.slotReady = false;
    this.controlState = controlState;
    if (!preserveLeaseSafety) this.leaseSafeUntil = 0;
    if (this.activeAttemptController && !this.activeAttemptController.signal.aborted) {
      this.activeAttemptController.abort(reason);
    }
  }

  #requireLeaseSafety() {
    if (!this.assignment || this.monotonicNow() >= this.leaseSafeUntil) {
      throw new RotaSlotDeferredError("lease_safety_window");
    }
  }

  #renewAssignment(frozen) {
    if (this.renewPromise) return this.renewPromise;
    const requestStarted = this.monotonicNow();
    const request = Object.freeze({
      renew_request_id: this.randomUUID(),
      slot_name: frozen.slot_name,
      worker_id: this.workerId,
      worker_instance_id: this.workerInstanceId,
      lease_id: frozen.lease_id,
      known_route_generation: frozen.route_generation,
    });
    const command = this.lane.enqueue("renew", async () => {
      const renewed = await this.client.renew(request);
      if (this.assignment?.slot_name !== frozen.slot_name
          || this.assignment?.lease_id !== frozen.lease_id) return renewed;
      if (!renewed?.ready) {
        this.#fenceSlot(
          renewed?.control_state || "RENEW_NOT_READY",
          new RotaSlotDeferredError("renew_not_ready", {
            retryAfterMs: Math.max(1000, Number(renewed?.retry_after_ms) || 1000),
          }),
        );
        return renewed;
      }
      const nextGeneration = Number(renewed.route_generation);
      if (this.idleRuntime && Number.isSafeInteger(nextGeneration)
          && nextGeneration > Number(this.assignment?.route_generation ?? 0)) {
        const staleRuntime = this.idleRuntime;
        this.idleRuntime = null;
        await this.identityRuntime.retire(staleRuntime, frozen);
      }
      this.#acceptAssignment(renewed, {
        requestStarted,
        expectedGeneration: frozen.route_generation,
      });
      if (this.completionUncertain) {
        this.#fenceSlot(
          "COMPLETE_UNCERTAIN",
          new RotaSlotDeferredError("completion_uncertain"),
          { preserveLeaseSafety: true },
        );
      }
      return renewed;
    });
    const recovered = command.then((renewed) => {
      if (this.completionUncertain && this.pendingCompletion) {
        const recovery = this.#scheduleCompletionRecovery(frozen);
        if (recovery) void recovery.catch(() => {});
      }
      return renewed;
    });
    const shared = recovered.finally(() => {
      if (this.renewPromise === shared) this.renewPromise = null;
    });
    this.renewPromise = shared;
    return shared;
  }

  #reclaimAssignment(frozen) {
    if (this.reclaimPromise) return this.reclaimPromise;
    const reclaim = this.#reclaimGoneAssignment(frozen);
    const shared = reclaim.finally(() => {
      if (this.reclaimPromise === shared) this.reclaimPromise = null;
    });
    this.reclaimPromise = shared;
    return shared;
  }

  async #reclaimGoneAssignment(frozen) {
    const mustWaitForActiveTask = this.activeTask !== null
      || this.activeRuntime !== null
      || this.pendingCompletion !== null;
    const activeJob = mustWaitForActiveTask
      ? this.activeJobCompletion?.promise ?? null
      : null;
    if (activeJob) await activeJob;
    if (this.closing
        || this.assignment?.slot_name !== frozen.slot_name
        || this.assignment?.lease_id !== frozen.lease_id) return false;

    if (this.activeAttemptController && !this.activeAttemptController.signal.aborted) {
      this.activeAttemptController.abort(new RotaSlotDeferredError("lease_gone"));
    }
    const runtimes = [...new Set([this.activeRuntime, this.idleRuntime].filter(Boolean))];
    for (const runtime of runtimes) {
      await this.identityRuntime.retire(runtime, frozen);
    }
    this.activeRuntime = null;
    this.idleRuntime = null;
    this.activeAttemptController = null;
    this.pendingCompletion = null;
    this.completionUncertain = false;
    this.activeTask = null;
    this.assignment = null;
    this.slotReady = false;
    this.controlState = "RECLAIMING";
    this.leaseSafeUntil = 0;

    while (!this.closing) {
      const requestStarted = this.monotonicNow();
      let claimed;
      try {
        claimed = await this.lane.enqueue("reclaim", () => this.client.claim({
          claim_request_id: this.randomUUID(),
          protocol_version: 2,
          role: this.role,
          worker_id: this.workerId,
          worker_instance_id: this.workerInstanceId,
          identity_policy_id: this.policy.id,
          identity_policy_version: this.policy.version,
        }));
      } catch (error) {
        if (error?.retryable !== true) throw error;
        await this.sleepImpl(1000);
        continue;
      }
      if (!claimed?.ready) {
        await this.sleepImpl(Math.max(100, Number(claimed?.retry_after_ms) || 1000));
        continue;
      }
      this.#acceptAssignment(claimed, { requestStarted, expectedGeneration: null });
      this.#scheduleRenew();
      return true;
    }
    return false;
  }

  #scheduleRenew() {
    if (this.closing || !this.started || this.renewTimer) return;
    this.renewTimer = this.setTimeoutImpl(() => {
      this.renewTimer = null;
      if (this.closing || !this.assignment) return;
      const frozen = this.assignment;
      let retryable = true;
      void this.#renewAssignment(frozen).catch(async (error) => {
        if (isLeaseGone(error) || isLeaseConflict(error)) {
          retryable = false;
          if (this.assignment?.slot_name === frozen.slot_name
              && this.assignment?.lease_id === frozen.lease_id) {
            this.#fenceSlot(isLeaseGone(error) ? "LEASE_GONE" : "LEASE_CONFLICT", error);
            await this.#reclaimAssignment(frozen);
          }
          return;
        }
        retryable = error?.retryable === true;
        if (this.assignment?.slot_name === frozen.slot_name
            && this.assignment?.lease_id === frozen.lease_id) {
          this.#fenceSlot("RENEW_FAILED", error);
        }
      }).finally(() => {
        if (retryable) this.#scheduleRenew();
      });
    }, this.renewIntervalMs);
    this.renewTimer.unref?.();
  }
}
