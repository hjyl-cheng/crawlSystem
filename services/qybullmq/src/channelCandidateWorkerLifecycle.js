import { isBusinessRunBudgetExhausted } from "./businessRunBudgetRecovery.js";
import { classifyTerminalChannelError } from "./channelLifecycle.js";
import { youtubeErrorText } from "./detailPolicy.js";
import { isParserContractError, parserContractDetails } from "./localizedParsing.js";
import {
  channelCandidateFailureDisposition,
  completeChannelCandidateJobAttempt,
  recordChannelCandidateSystemFailure,
  resolveMigrationSystemRetryItems,
  retryableSystemFailureDecision,
  settleChannelCandidateJobFailure,
} from "./managedWorkerJob.js";
import { decideYoutubeFailure } from "./youtubeFailurePolicy.js";

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

export async function completeChannelCandidateWorkerJob(query, job) {
  requiredFunction(query, "query");
  return completeChannelCandidateJobAttempt(query, job);
}

function currentAttemptJob(job) {
  return {
    id: job?.id,
    queueName: job?.queueName,
    data: job?.data,
    opts: job?.opts,
    attemptsMade: Number(job?.attemptsMade ?? 0) + 1,
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
  job,
  execute,
} = {}) {
  requiredFunction(query, "query");
  requiredFunction(execute, "execute");
  const attemptJob = currentAttemptJob(job);
  try {
    const result = await execute();
    const completed = await completeChannelCandidateJobAttempt(query, attemptJob);
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
        systemFailureTerminal: attemptJob.attemptsMade >= maxAttempts,
      });
    } catch (persistenceError) {
      throw durabilityError(error, persistenceError);
    }
    if (!settlement.recorded) {
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
  const { settlement, resolved } = await withTransaction(async (client) => {
    const transactionQuery = client.query.bind(client);
    const settled = await settleChannelCandidateJobFailure(transactionQuery, job, {
      disposition,
      message: failure.message,
      error,
      systemFailureTerminal: failure.systemFailure != null && attemptsMade >= maxAttempts,
      snapshotPatch: failure.parserDetails
        ? { parser_contract_error: failure.parserDetails }
        : {},
    });
    const resolvedCount = !failure.systemFailure && isTerminal
      ? await resolveMigrationSystemRetryItems(transactionQuery, job, {
        resolution: "retry_job_terminal_business_failure",
      })
      : 0;
    return { settlement: settled, resolved: resolvedCount };
  });
  if (job?.data?.dispatch_batch_id) {
    await refreshDispatchCandidateCounts(String(job.data.dispatch_batch_id));
  }
  await signalReadyDiscoveryPageQualifications({
    candidateId: Number(job?.data?.candidate_id),
  });
  if (job?.data?.retry_intent_id && !failure.systemFailure && isTerminal) {
    await finishMigrationRetryIntent(query, job, {
      outcome: "failed",
      error,
    });
  }
  return Object.freeze({
    disposition,
    settlement,
    resolved,
    terminal: isTerminal,
    attemptsMade,
    maxAttempts,
  });
}
