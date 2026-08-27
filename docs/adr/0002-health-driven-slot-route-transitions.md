---
status: accepted
---

# Health-driven Slot Route transitions

A conclusive Health Incident can make a Proxy ineligible while a Worker still owns its Slot. We will keep an Execution-Locked Slot on its current Route until the active Attempt quiesces and completes; a Health Incident is not a substitute for that Task's Worker Observation and does not create a Task `pending_action`. `BeginTask` remains the final eligibility gate, so the ineligible Route cannot start another Task.

A Lease-Owned Idle Slot uses a same-Lease Idle Route Transition. Rota selects an eligible reserve, rotates credentials, advances the Route generation, prepares the data plane, and then exposes the new Route through Renew. If no reserve exists, Rota clears the unusable Route and returns `PAUSED_NO_RESERVE` while retaining the Lease; a later reconciliation resumes the same transition when capacity returns. Repeated health notifications and reconciliation runs must not advance either generation more than once for the same transition.

The Worker must quiesce and retire any idle Runtime for the older Route before accepting a higher generation. Rota never silently changes an active Task's endpoint. Health notifications are post-commit hints that request reconciliation; persisted Proxy health state and periodic reconciliation remain authoritative if a notification is lost.

## Considered Options

Replacing the Lease and requiring Release/Claim was rejected for idle transitions because it turns a routine Route change into ownership loss and cannot directly preserve `PAUSED_NO_RESERVE` on the owned Slot. Automatically aborting active Tasks was rejected because the current Completion contract authorizes rotation with Task Observations; inventing a second implicit authorization path would make completion fencing ambiguous.
