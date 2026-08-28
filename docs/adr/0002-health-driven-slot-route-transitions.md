---
status: accepted
---

# Health-driven Slot Route transitions

A conclusive Health Incident can make a Proxy ineligible while a Worker still owns its Slot. We will keep an Execution-Locked Slot on its current Route until the active Attempt quiesces and completes; a Health Incident is not a substitute for that Task's Worker Observation and does not create a Task `pending_action`. `BeginTask` remains the final eligibility gate, so the ineligible Route cannot start another Task.

A Lease-Owned Idle Slot uses a same-Lease Idle Route Transition. Rota selects an eligible reserve, rotates credentials, advances the Route generation, prepares the data plane, and then exposes the new Route through Renew. If no reserve exists, Rota clears the unusable Route and returns `PAUSED_NO_RESERVE` while retaining the Lease; a later reconciliation resumes the same transition when capacity returns. Repeated health notifications and reconciliation runs must not advance either generation more than once for the same transition.

The Worker must quiesce and retire any idle Runtime for the older Route before accepting a higher generation. Rota never silently changes an active Task's endpoint. Health notifications are post-commit hints that request reconciliation; persisted Proxy health state and periodic reconciliation remain authoritative if a notification is lost.

## Route Activation Publication

An Idle Route Transition publishes a prepared data-plane credential in this order:

1. PostgreSQL persists `pending_new_route` and a fenced activation Claim.
2. `BeginActivation(previousClaim, claim)` performs a monotonic data-plane CAS and prepares the credential.
3. `CommitActivation(claim)` makes that data-plane Token immune to conditional retirement.
4. PostgreSQL Finalize exposes the Route as ready.

Claim load is not a lease renewal. Only a successful Begin or Commit may extend the Claim, and both renewal and Finalize require the same Claim to remain unexpired together with the Slot, live Lease, Proxy, and Route-generation Fence. An expired Claim cannot be revived. A takeover Claim persists its predecessor so stale managers cannot overwrite or retire the newer activation. Finalize uncertainty is resolved by authoritative read or idempotent retry; a committed data-plane Token is never conditionally retired.

Before reconciliation or managed proxy connections are enabled, Rota rebuilds the activation registry from the complete database Route Fence. A ready live Route is reconstructed as committed; a pending unready Route is reconstructed as activating and blocked. Registry reconstruction failure is fail-closed. The in-memory registry requires single data-plane instance ownership; multiple data-plane replicas require shared persistence or reliable synchronization.

The legacy `/api/v1/proxy-control/swap` protocol is removed because it bypassed the active-Task and activation fences. Every Route change now uses the same Completion or Idle Route Transition state machine.

## Considered Options

Replacing the Lease and requiring Release/Claim was rejected for idle transitions because it turns a routine Route change into ownership loss and cannot directly preserve `PAUSED_NO_RESERVE` on the owned Slot. Automatically aborting active Tasks was rejected because the current Completion contract authorizes rotation with Task Observations; inventing a second implicit authorization path would make completion fencing ambiguous.
