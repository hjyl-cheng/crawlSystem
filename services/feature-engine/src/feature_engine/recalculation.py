from __future__ import annotations


# Serializes the "no unresolved run" check with Scheduler plan creation.
RECALCULATION_COORDINATION_LOCK_ID = 1_616_000_101

# Only one worker may execute a resumable recalculation at a time. There can be
# only one unresolved Run, so a global lock is both sufficient and explicit.
RECALCULATION_RESUME_LOCK_ID = 1_616_000_102

# Failed and partial runs are resumable and remain unresolved until they either
# succeed or an operator explicitly cancels them.
UNRESOLVED_RECALCULATION_STATUSES = ("pending", "running", "partial", "failed")
