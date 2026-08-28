function positiveInteger(value, field) {
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed <= 0) {
    throw new TypeError(`${field} must be a positive integer`);
  }
  return parsed;
}

function requiredText(value, field) {
  const normalized = String(value ?? "").trim();
  if (!normalized) throw new TypeError(`${field} is required`);
  return normalized;
}

export function normalizeChannelCandidateAttemptFence(value, {
  candidateId = null,
} = {}) {
  const normalized = Object.freeze({
    candidateId: positiveInteger(value?.candidateId, "candidateAttemptFence.candidateId"),
    dispatchGeneration: positiveInteger(
      value?.dispatchGeneration,
      "candidateAttemptFence.dispatchGeneration",
    ),
    jobId: requiredText(value?.jobId, "candidateAttemptFence.jobId"),
    bullmqAttempt: positiveInteger(
      value?.bullmqAttempt,
      "candidateAttemptFence.bullmqAttempt",
    ),
  });
  if (candidateId != null && normalized.candidateId !== positiveInteger(candidateId, "candidateId")) {
    throw new TypeError("candidateAttemptFence.candidateId must match candidateId");
  }
  return normalized;
}

function channelCandidateAttemptFence(job, bullmqAttempt) {
  return normalizeChannelCandidateAttemptFence({
    candidateId: job?.data?.candidate_id,
    dispatchGeneration: job?.data?.dispatch_generation,
    jobId: job?.id,
    bullmqAttempt,
  });
}

export function activeChannelCandidateAttemptFence(job) {
  return channelCandidateAttemptFence(job, Number(job?.attemptsMade ?? 0) + 1);
}

export function failedChannelCandidateAttemptFence(job) {
  return channelCandidateAttemptFence(job, job?.attemptsMade);
}
