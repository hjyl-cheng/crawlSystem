import { isBusinessRunBudgetExhausted } from "./businessRunBudgetRecovery.js";
import { classifyTerminalChannelError } from "./channelLifecycle.js";
import { youtubeErrorText } from "./detailPolicy.js";
import { isParserContractError, parserContractDetails } from "./localizedParsing.js";
import {
  channelCandidateFailureDisposition,
  clearChannelCandidateJobAttempt,
  completeChannelCandidateJobAttempt,
  recordChannelCandidateSystemFailure,
  resolveMigrationSystemRetryItems,
  retryableSystemFailureDecision,
  isStaleExecutionFailure,
  settleChannelCandidateJobFailure,
} from "./managedWorkerJob.js";
import { decideYoutubeFailure } from "./youtubeFailurePolicy.js";
import {
  persistChannelCandidateParserContractFailure,
  StaleChannelCandidateAttemptError,
} from "./channelCandidateAttemptMutations.js";
import { activeChannelCandidateAttemptFence } from "./channelCandidateAttemptFence.js";
import { isAboutOnlyPublicationGapRepair } from "./publicationGapRepairExecution.js";

function requiredFunction(value, name) {
  if (typeof value !== "function") throw new TypeError(`${name} is required`);
  return value;
}

function terminalFailure({
  businessRunBudgetTerminal,
  permanentFailure,
  terminalChannel,
  attemptsMade,
  maxAttempts,
}) {
  return Boolean(businessRunBudgetTerminal
    || permanentFailure
    || terminalChannel !== null
    || attemptsMade >= maxAttempts);
}

export function describeChannelCandidateWorkerFailure(error) {
  const systemFailureDecision = retryableSystemFailureDecision(error);
  const systemFailure = systemFailureDecision?.evidence ?? null;
  const failureDecision = systemFailureDecision
    ?? error?.youtube_failure_decision
    ?? decideYoutubeFailure({ error });
  return Object.freeze({
    message: youtubeErrorText(error),
    parserFailure: isParserContractError(error),
    terminalChannel: classifyTerminalChannelError(error),
    businessRunBudgetTerminal: isBusinessRunBudgetExhausted(error),
    systemFailure,
    parserDetails: parserContractDetails(error),
    failureDecision,
    permanentFailure: !systemFailure && failureDecision.retry_mode === "none",
  });
}

export async function completeChannelCandidateWorkerJob(query, job, {
  withTransaction = null,
  finishMigrationRetryIntent = null,
} = {}) {
  requiredFunction(query, "query");
  const retryIntentId = String(job?.data?.retry_intent_id ?? "").trim();
  const complete = async (transactionQuery) => {
    const completed = await completeChannelCandidateJobAttempt(transactionQuery, job);
    if (!retryIntentId) return completed;
    if (!completed.cleared) {
      return Object.freeze({ ...completed, intentFinished: false });
    }
    const intentFinished = await requiredFunction(
      finishMigrationRetryIntent,
      "finishMigrationRetryIntent",
    )(transactionQuery, job, { outcome: "finished" });
    if (!intentFinished) {
      const error = new Error(
        `Migration Retry Intent attempt Fence rejected completed Job: ${job?.id}`,
      );
      error.code = "MIGRATION_RETRY_INTENT_FENCE_STALE";
      throw error;
    }
    return Object.freeze({ ...completed, intentFinished: true });
  };
  if (!retryIntentId) return complete(query);
  requiredFunction(withTransaction, "withTransaction");
  return withTransaction((client) => complete(client.query.bind(client)));
}

function currentAttemptJob(job) {
  return {
    id: job?.id,
    queueName: job?.queueName,
    data: job?.data,
    opts: job?.opts,
    attemptsMade: Number(job?.attemptsMade ?? 0) + 1,
    attemptsStarted: job?.attemptsStarted,
  };
}

function durabilityError(error, persistenceError) {
  const wrapped = new AggregateError(
    [error, persistenceError],
    `Channel Candidate system failure was not durably recorded: ${persistenceError.message}`,
  );
  wrapped.name = "ChannelCandidateSystemFailurePersistenceError";
  return wrapped;
}

export async function runChannelCandidateWorkerJobWithDurableSettlement({
  query,
  withTransaction = null,
  finishMigrationRetryIntent = null,
  job,
  execute,
} = {}) {
  requiredFunction(query, "query");
  requiredFunction(execute, "execute");
  const attemptJob = currentAttemptJob(job);
  try {
    const result = await execute();
    const completed = await completeChannelCandidateWorkerJob(query, attemptJob, {
      withTransaction,
      finishMigrationRetryIntent,
    });
    if (!completed.cleared) {
      const error = new Error(`Candidate attempt fence rejected completed Job: ${job?.id}`);
      error.code = "CANDIDATE_ATTEMPT_FENCE_STALE";
      throw error;
    }
    return result;
  } catch (error) {
    const failure = describeChannelCandidateWorkerFailure(error);
    if (!failure.systemFailure) throw error;
    const maxAttempts = Math.max(1, Number(job?.opts?.attempts ?? 1));
    let settlement;
    try {
      settlement = await recordChannelCandidateSystemFailure(query, attemptJob, {
        message: failure.message,
        error,
        systemFailureTerminal: isStaleExecutionFailure(error) || attemptJob.attemptsMade >= maxAttempts,
      });
    } catch (persistenceError) {
      throw durabilityError(error, persistenceError);
    }
    if (!settlement.recorded) {
      // A newer owner must remain untouched, including by failure settlement.
      if (isStaleExecutionFailure(error)) throw error;
      throw durabilityError(
        error,
        new Error(`Candidate attempt fence rejected failed Job: ${job?.id}`),
      );
    }
    throw error;
  }
}

export async function failChannelCandidateWorkerJob({
  query,
  withTransaction,
  job,
  error,
  failure = describeChannelCandidateWorkerFailure(error),
  refreshDispatchCandidateCounts,
  signalReadyDiscoveryPageQualifications,
  finishMigrationRetryIntent,
} = {}) {
  requiredFunction(query, "query");
  requiredFunction(withTransaction, "withTransaction");
  requiredFunction(refreshDispatchCandidateCounts, "refreshDispatchCandidateCounts");
  requiredFunction(
    signalReadyDiscoveryPageQualifications,
    "signalReadyDiscoveryPageQualifications",
  );
  requiredFunction(finishMigrationRetryIntent, "finishMigrationRetryIntent");
  const maxAttempts = Math.max(1, Number(job?.opts?.attempts ?? 1));
  const attemptsMade = Number(job?.attemptsMade ?? 0);
  const disposition = channelCandidateFailureDisposition({
    error,
    terminalChannel: failure.terminalChannel,
    permanentFailure: failure.permanentFailure,
    attemptsMade,
    maxAttempts,
  });
  const isTerminal = terminalFailure({
    ...failure,
    attemptsMade,
    maxAttempts,
  });
  const shouldFailRun = !failure.terminalChannel
    && !isAboutOnlyPublicationGapRepair(job?.data)
    && !failure.businessRunBudgetTerminal
    && !failure.systemFailure
    && (failure.permanentFailure || attemptsMade >= maxAttempts)
    && String(job?.data?.run_id ?? "").trim() !== "";
  const { settlement, resolved, runFailureRecorded } = await withTransaction(async (client) => {
    const transactionQuery = client.query.bind(client);
    let settled;
    if (failure.parserDetails) {
      try {
        await persistChannelCandidateParserContractFailure(
          transactionQuery,
          activeChannelCandidateAttemptFence(job),
          {
            message: failure.message,
            details: failure.parserDetails,
          },
        );
        const fenceCleared = await clearChannelCandidateJobAttempt(transactionQuery, job);
        if (!fenceCleared) {
          throw new Error(`Candidate parser failure Fence could not be cleared: ${job?.id}`);
        }
        settled = { recorded: true, fenceCleared: true };
      } catch (candidateError) {
        if (!(candidateError instanceof StaleChannelCandidateAttemptError)) throw candidateError;
        settled = { recorded: false, fenceCleared: false };
      }
    } else {
      settled = await settleChannelCandidateJobFailure(transactionQuery, job, {
        disposition,
        message: failure.message,
        error,
        systemFailureTerminal: failure.systemFailure != null
          && (isStaleExecutionFailure(error) || attemptsMade >= maxAttempts),
        snapshotPatch: {},
      });
    }
    const ownsSettledCandidate = settled.recorded || settled.fenceCleared;
    if (job?.data?.retry_intent_id
        && !failure.systemFailure
        && isTerminal
        && ownsSettledCandidate) {
      const intentFinished = await finishMigrationRetryIntent(transactionQuery, job, {
        outcome: "failed",
        error,
      });
      if (!intentFinished) {
        const intentError = new Error(
          `Migration Retry Intent attempt Fence rejected failed Job: ${job?.id}`,
        );
        intentError.code = "MIGRATION_RETRY_INTENT_FENCE_STALE";
        throw intentError;
      }
    }
    const resolvedCount = !failure.systemFailure && isTerminal && ownsSettledCandidate
      ? await resolveMigrationSystemRetryItems(transactionQuery, job, {
        resolution: "retry_job_terminal_business_failure",
      })
      : 0;
    let runFailureCount = 0;
    if (shouldFailRun && ownsSettledCandidate) {
      const failedRun = await transactionQuery(
        `UPDATE crawler.channel_runs
         SET status='failed',detail_status='failed',error_message=$2,
             result_json=result_json || $3::jsonb,
             finished_at=now(),updated_at=now()
         WHERE run_id=$1 AND candidate_id=$4`,
        [
          String(job.data.run_id),
          failure.message,
          JSON.stringify(failure.parserDetails
            ? { parser_contract_error: failure.parserDetails }
            : {}),
          Number(job.data.candidate_id),
        ],
      );
      runFailureCount = Number(failedRun.rowCount ?? 0);
    }
    return {
      settlement: settled,
      resolved: resolvedCount,
      runFailureRecorded: runFailureCount === 1,
    };
  });
  if (job?.data?.dispatch_batch_id) {
    await refreshDispatchCandidateCounts(String(job.data.dispatch_batch_id));
  }
  await signalReadyDiscoveryPageQualifications({
    candidateId: Number(job?.data?.candidate_id),
  });
  return Object.freeze({
    disposition,
    settlement,
    resolved,
    runFailureRecorded,
    terminal: isTerminal,
    attemptsMade,
    maxAttempts,
  });
}
