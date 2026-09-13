# Unified Worker management

The server card now has one Worker management entry and a quick pause action.
Server details, editing and deletion live in the overflow menu. Uninitialized
nodes retain initialization/environment preparation actions; a ready empty node
offers its first Worker deployment. The existing center fleet exposes intake
control only.

The management dialog separates verified installed count from admitted count.
It shows the effect of expansion, installation memory requirements, live intake
and drain status, and a collapsible latest deployment record. Running and failed
deployments expand the record; successful deployments fold it. Polling preserves
edited inputs. Saving intake stays in the same dialog so users can observe drain.

## Deployment and optional intake synchronization

The existing `deploy-workers` endpoint accepts optional `count`, `syncIntake`
and `expectedAllowedCount`. The store freezes the validated count and updates
the saved plan in the same transaction that starts the operation. Legacy calls
using a separately saved plan remain supported. This reuses the original SSH,
container recipe and center registration; collection and queue processing are
unchanged.

When explicitly selected, intake synchronization runs in the server-side
deployment operation, after every intended Worker has connected. Closing the
browser does not cancel it. The original intake count is checked before starting
and is passed as an optimistic guard to the center when enabling Workers. A newer
manual intake setting wins. Synchronization failure is recorded separately from
a successfully completed deployment, with an instruction to verify intake.
Progress and outcomes are kept in the existing registry JSON; no new table is
required. Passwords remain transient.

Container reduction still requires the existing retirement workflow and is not
enabled by this dialog. Reducing admitted count remains available, including
during a failed expansion. The existing per-node 32-instance deployment bound
and installer memory check are unchanged. No global limit was added.

## Validation and deployment

- PostgreSQL integration covers atomic count submission, failed expansion keeping
  the previous installed/admitted counts, retry and successful synchronization,
  and a concurrent manual reduction that must not be overwritten. It checks that
  synchronization is attempted only after all connections are verified and that
  the stored result survives recreating the store.
- Dashboard registration/runtime/intake route tests pass.
- A real browser against isolated mock nodes covers 5→20 expansion, preserving
  edits across polling, background progress, failed expansion with intake pause,
  success history folding, center intake, first deployment, outside-menu clicks
  and mobile scrolling/overflow. No production node expansion was triggered.

Only Dashboard was replaced with `qy-allpachong/dashboard:worker-manager-20260911`.
The previous process exited with code 0 and its stopped container/configuration
remain available for rollback. Runtime compose and the intake-count override
reference the new image. Read-only production verification found the unified
dialog and new client, no legacy Worker dialogs, local deployed/admitted 20 and
remote deployed/admitted 5. Both queues remained unpaused and Full Crawl retained
40 active tasks. The remote node's prior failed expansion to 20 was not retried.
