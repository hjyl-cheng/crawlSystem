---
status: accepted
---

# Health-driven Slot Route transitions

Background Health Incidents apply to Proxies outside a live Worker Lease, including Proxies merely preassigned to an unleased Slot. While a Proxy is on a live Lease, its Worker's Task Observations are the health authority. An Observation may require a Route replacement, but an Execution-Locked Slot stays on its current Route until the active Attempt quiesces and completes; background probing neither changes the leased Proxy's lifecycle nor creates a Task `pending_action`.

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

## Active and Warm Reserve

For unbound inventory, `active` is a renewable health grant rather than a permanent historical label. One request through the Proxy to `https://www.youtube.com/results?search_query=<random 1-6 digit number>` is authoritative: HTTP 200, a final YouTube host, valid `ytInitialData`, and no challenge or consent page create the grant and schedule its next recheck. The complete request has a 15-second ceiling. The Base probe and server-direct control probe are not run; their legacy evidence fields do not gate lifecycle decisions. The periodic claimant changes an overdue unbound Proxy to `idle` before probing it, so it cannot be allocated while validation is in flight or inconclusive. Legacy HTTP status fields are not eligibility evidence.

A Warm Reserve is an `active` Proxy that is not assigned to a Slot, is outside cooldown, does not require revalidation, and has a future recheck deadline backed by structured successful evidence. Proxy Control capacity reports this set directly. Only a Proxy protected by a live Slot and Lease-history Fence is excluded from background claims; a mere preassignment is still revalidated. If a Task Observation requires a Route change, Completion first fences the old Route and then changes its Proxy to recoverable `failed` state with a cooldown and scheduled recheck. The Route change never archives the Proxy; only the existing conclusive long-failure policy may do that later.

## Considered Options

Replacing the Lease and requiring Release/Claim was rejected for idle transitions because it turns a routine Route change into ownership loss and cannot directly preserve `PAUSED_NO_RESERVE` on the owned Slot. Automatically aborting active Tasks was rejected because the current Completion contract authorizes rotation with Task Observations; inventing a second implicit authorization path would make completion fencing ambiguous.
