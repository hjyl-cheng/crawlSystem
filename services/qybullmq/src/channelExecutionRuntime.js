import { browserProfileStore } from "./browserProfileStore.js";
import {
  assertChannelExecutionIdentity,
  ChannelExecutionMetrics,
  ProxyIdentityChangedError,
  runWithChannelExecution,
} from "./channelExecutionContext.js";
import { fingerprintGateway } from "./fingerprintGateway.js";
import { sameProxyAssignment } from "./proxyAssignment.js";
import { runWithProxyIdentity } from "./proxyIdentity.js";
import {
  acquirePersistentYtDlp,
  closePersistentYtDlp,
  persistentYtDlpState,
  releasePersistentYtDlp,
} from "./ytdlpSession.js";
import {
  acquireYoutubeJs,
  closeYoutubeJs,
  releaseYoutubeJs,
  youtubeJsState,
} from "./youtubeJs.js";
import { decideYoutubeFailure } from "./youtubeFailurePolicy.js";

function cleanSessionRelease(value) {
  if (!value) return { summary: value, cookieState: undefined };
  const { cookie_state: cookieState, ...summary } = value;
  return { summary, cookieState };
}

function executionSummary({ attemptId, proxy, profileGroup }) {
  return {
    attempt_id: attemptId,
    proxy_id: proxy.proxy_id == null ? null : Number(proxy.proxy_id),
    proxy_user: proxy.proxy_user || proxy.slot_name,
    slot_name: proxy.slot_name,
    lease_id: proxy.lease_id ?? null,
    route_generation: proxy.route_generation ?? null,
    network_identity_key: proxy.network_identity_key ?? null,
    profile_epoch: proxy.profile_epoch ?? null,
    identity_policy_id: proxy.identity_policy_id ?? null,
    proxy_address_hash: proxy.proxy_address_hash ?? null,
    profile_group_id: profileGroup.profile_group_id,
    profile_revision: profileGroup.profile_revision,
    youtubejs_profile_id: profileGroup.clients.youtubejs_chrome?.profile_id ?? null,
    ytdlp_profile_id: profileGroup.clients.ytdlp_safari?.profile_id ?? null,
  };
}

function errorFromEvidence(evidence) {
  const error = new Error(evidence.error_message || evidence.body || "YouTube request failed");
  if (evidence.error_name) error.name = evidence.error_name;
  if (evidence.error_code) error.code = evidence.error_code;
  return error;
}

function failureDecisions(metrics, attemptError = null) {
  const evidence = [...(metrics.failure_evidence || [])];
  if (attemptError) {
    evidence.push({
      error: attemptError,
      error_name: attemptError?.name || null,
      error_code: attemptError?.code || attemptError?.cause?.code || null,
      error_message: String(attemptError?.message || attemptError),
      status: attemptError?.youtube_failure_evidence?.status ?? null,
      body: attemptError?.youtube_failure_evidence?.body ?? "",
      source: attemptError?.youtube_failure_evidence?.source ?? "channel_attempt",
      target_url: attemptError?.youtube_failure_evidence?.target_url ?? null,
      client: attemptError?.youtube_failure_evidence?.client ?? null,
    });
  }
  const seen = new Set();
  const output = [];
  for (const item of evidence) {
    const sourceError = item.error || errorFromEvidence(item);
    const disposition = decideYoutubeFailure({
      error: sourceError,
      status: item.status,
      body: item.body,
      source: item.source,
    });
    const key = [disposition.kind, item.source, item.status, item.error_message].join(":");
    if (seen.has(key)) continue;
    seen.add(key);
    output.push({
      ...disposition,
      evidence: {
        source: item.source || null,
        target_url: item.target_url || null,
        client: item.client || null,
        status: item.status ?? disposition.status ?? null,
        body: item.body || "",
        error_message: item.error_message || null,
      },
    });
  }
  return output;
}

export class ChannelExecutionRuntime {
  constructor({
    profileStore = null,
    gateway = null,
    acquireYtDlp = acquirePersistentYtDlp,
    releaseYtDlp = releasePersistentYtDlp,
    acquireYoutube = acquireYoutubeJs,
    releaseYoutube = releaseYoutubeJs,
  } = {}) {
    this.profileStore = profileStore;
    this.gateway = gateway;
    this.acquireYtDlp = acquireYtDlp;
    this.releaseYtDlp = releaseYtDlp;
    this.acquireYoutube = acquireYoutube;
    this.releaseYoutube = releaseYoutube;
    this.activeAttemptId = null;
  }

  store() {
    if (!this.profileStore) this.profileStore = browserProfileStore();
    return this.profileStore;
  }

  transport() {
    if (!this.gateway) this.gateway = fingerprintGateway();
    return this.gateway;
  }

  async run({
    job,
    proxy,
    getProxySnapshot,
    proxyUrl,
    workerId,
    language,
    country,
    timezone,
    task = null,
    prepared = null,
    abortSignal = null,
    managedRequestTracker = null,
  }, callback) {
    if (this.activeAttemptId) throw new Error(`channel execution runtime is already active: ${this.activeAttemptId}`);
    this.activeAttemptId = `preparing:${workerId}:${job?.id ?? "unknown"}`;
    let attemptId = null;
    let store = null;
    let profileGroup = null;
    const channelId = String(job.data?.channel_id || "unknown");
    try {
      const profileEpoch = Number(proxy?.profile_epoch);
      const hasV2Identity = Boolean(
        proxy?.lease_id
        && proxy?.route_generation
        && proxy?.network_identity_key
        && proxy?.identity_policy_id,
      ) && Number.isSafeInteger(profileEpoch) && profileEpoch >= 0;
      const hasLegacyIdentity = Boolean(proxy?.proxy_id && proxy?.proxy_address_hash);
      if ((!hasV2Identity && !hasLegacyIdentity) || !proxy?.slot_name || !proxyUrl) {
        throw new Error("channel execution requires a complete Rota identity, slot and proxy URL");
      }
      const currentProxy = getProxySnapshot();
      if (!sameProxyAssignment(proxy, currentProxy)) {
        throw new ProxyIdentityChangedError(proxy, currentProxy);
      }

      store = this.store();
      profileGroup = await store.loadOrCreate({
        proxyId: proxy.proxy_id,
        proxyAddressHash: proxy.proxy_address_hash,
        identityPolicyId: proxy.identity_policy_id,
        identityPolicyVersion: proxy.identity_policy_version,
        networkIdentityKey: proxy.network_identity_key,
        profileEpoch: proxy.profile_epoch,
        language,
        country,
        timezone,
      });
      attemptId = await store.beginAttempt({
        channelId,
        runId: prepared?.businessRunId ?? job.data?.run_id ?? null,
        queueName: job.queueName,
        jobId: job.id,
        jobAttempt: job.attemptsMade,
        workerId,
        proxy,
        profileGroup,
        task,
        prepared,
      });
      this.activeAttemptId = attemptId;
    } catch (error) {
      this.activeAttemptId = null;
      throw error;
    }

    const summary = executionSummary({ attemptId, proxy, profileGroup });
    const gateway = this.transport();
    let result;
    let error = null;
    let hasError = false;
    let ytdlpLease = null;
    let youtubeLease = null;
    let ytdlpRelease = null;
    let youtubeRelease = null;
    let cleanupError = null;
    let identityChanged = false;
    let attemptAborted = false;
    const startedAt = Date.now();
    const metrics = new ChannelExecutionMetrics();
    let metricsSnapshot = null;
    let decisions = [];

    try {
      await gateway.prepare({ proxyUrl, profileGroup });
      result = await runWithProxyIdentity({
        ...proxy,
        proxy_url: proxyUrl,
        abort_signal: abortSignal,
        managed_request_tracker: managedRequestTracker,
      }, () => runWithChannelExecution({
        attempt_id: attemptId,
        proxy,
        get_proxy_snapshot: getProxySnapshot,
        profile_group: profileGroup,
        fingerprint_gateway: gateway,
        metrics,
        abort_signal: abortSignal,
      }, async () => {
        assertChannelExecutionIdentity();
        ytdlpLease = await this.acquireYtDlp(channelId, language, {
          profile: profileGroup.clients.ytdlp_safari,
          proxyUrl,
        });
        if (!ytdlpLease?.enabled) throw new Error(`yt-dlp fingerprint session unavailable: ${ytdlpLease?.error || "disabled"}`);
        youtubeLease = await this.acquireYoutube(channelId, {
          profile: profileGroup.clients.youtubejs_chrome,
          proxyUrl,
        });
        if (!youtubeLease?.enabled) throw new Error(`YouTube.js fingerprint session unavailable: ${youtubeLease?.error || "disabled"}`);
        const output = await callback();
        if (abortSignal?.aborted) throw abortSignal.reason ?? new Error("channel execution aborted");
        assertChannelExecutionIdentity();
        return output;
      }));
    } catch (caught) {
      error = caught;
      hasError = true;
    } finally {
      const attemptCancelled = () => identityChanged || Boolean(abortSignal?.aborted);
      const refreshAttemptState = () => {
        const finalProxy = getProxySnapshot();
        const changed = !sameProxyAssignment(proxy, finalProxy)
          || error instanceof ProxyIdentityChangedError;
        if (changed) {
          identityChanged = true;
          if (!hasError) {
            error = new ProxyIdentityChangedError(proxy, finalProxy);
            hasError = true;
          }
        }
        if (abortSignal?.aborted) {
          error = abortSignal.reason;
          hasError = true;
        }
      };
      refreshAttemptState();
      try {
        youtubeRelease = await this.releaseYoutube();
      } catch (caught) {
        cleanupError = cleanupError || caught;
      }
      refreshAttemptState();
      const ytdlpReleaseStartedCancelled = attemptCancelled();
      try {
        ytdlpRelease = await this.releaseYtDlp({
          cancelled: ytdlpReleaseStartedCancelled,
          reason: error,
          signal: abortSignal,
        });
      } catch (caught) {
        cleanupError = cleanupError || caught;
      }
      refreshAttemptState();
      let ytdlpCancellationApplied = ytdlpReleaseStartedCancelled;
      const terminateReleasedYtDlpIfNeeded = async () => {
        if (ytdlpCancellationApplied || !attemptCancelled()) return;
        ytdlpCancellationApplied = true;
        try {
          await this.releaseYtDlp({ cancelled: true, reason: error, signal: abortSignal });
        } catch (caught) {
          cleanupError = cleanupError || caught;
        }
        refreshAttemptState();
      };
      await terminateReleasedYtDlpIfNeeded();
      const ytdlp = cleanSessionRelease(ytdlpRelease);
      ytdlpRelease = ytdlp.summary;
      if (!attemptCancelled() && !hasError) {
        try {
          const youtubeCookies = await gateway.snapshot(profileGroup.clients.youtubejs_chrome);
          refreshAttemptState();
          await terminateReleasedYtDlpIfNeeded();
          if (!attemptCancelled() && !hasError) {
            await store.checkpointCookies(profileGroup.profile_group_id, {
              youtubejs_chrome: youtubeCookies,
              ytdlp_safari: ytdlp.cookieState,
            });
          }
        } catch (caught) {
          cleanupError = cleanupError || caught;
        }
      }
      refreshAttemptState();
      await terminateReleasedYtDlpIfNeeded();
      if (!attemptCancelled() && !hasError && cleanupError) {
        error = cleanupError;
        hasError = true;
      }
      attemptAborted = attemptCancelled();
      metricsSnapshot = metrics.snapshot();
      decisions = failureDecisions(metricsSnapshot, attemptAborted ? null : error);
      try {
        await store.finishAttempt(attemptId, {
          status: attemptAborted ? "aborted" : hasError ? "failed" : "success",
          identityChanged,
          error,
          result: {
            duration_ms: Date.now() - startedAt,
            youtube_requests: metricsSnapshot,
            failure_decisions: decisions,
            ytdlp_session: ytdlpLease ? { ...ytdlpLease, release: ytdlpRelease } : null,
            youtubejs_session: youtubeLease ? { ...youtubeLease, release: youtubeRelease } : null,
          },
        });
      } catch (caught) {
        if (!attemptCancelled() && !hasError) {
          error = caught;
          hasError = true;
        }
      }
      this.activeAttemptId = null;
    }

    if (hasError) {
      if (error && ["object", "function"].includes(typeof error)) {
        try {
          error.channel_execution_attempt = {
            attempt_id: attemptId,
            youtube_requests: metricsSnapshot,
            failure_decisions: decisions,
          };
        } catch {
          // Cancellation reason identity takes precedence over diagnostic decoration.
        }
      }
      throw error;
    }
    return {
      result,
      execution: summary,
      youtube_requests: metricsSnapshot,
      failure_decisions: decisions,
      sessions: {
        ytdlp: ytdlpLease ? { ...ytdlpLease, release: ytdlpRelease, state: persistentYtDlpState() } : null,
        youtubejs: youtubeLease ? { ...youtubeLease, release: youtubeRelease, state: youtubeJsState() } : null,
      },
    };
  }

  async close() {
    await Promise.allSettled([
      closeYoutubeJs(),
      closePersistentYtDlp(),
      this.gateway?.close?.(),
    ]);
  }
}

let defaultRuntime = null;

export function channelExecutionRuntime() {
  if (!defaultRuntime) defaultRuntime = new ChannelExecutionRuntime();
  return defaultRuntime;
}
