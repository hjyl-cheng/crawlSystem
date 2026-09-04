# Proxy-Controlled Crawling

This context describes how crawler Workers hold managed network identities and how Rota changes those identities without crossing an in-flight request boundary.

## Language

**Execution-Locked Slot**:
A Slot with an active Task whose Route remains fixed until the current Attempt has quiesced and completed.
_Avoid_: Locked Lease, busy Slot

**Lease-Owned Idle Slot**:
A Slot owned by a live Worker Lease with no active Task. Its Route may change through an Idle Route Transition.
_Avoid_: Locked Slot, free Slot

**Health Incident**:
Persisted, conclusive background health evidence for a Proxy outside a live Worker Lease. It can change that Proxy's lifecycle without a Task Observation, but never governs a live leased Route.
_Avoid_: Observation, Worker failure

**Verified Active Proxy**:
An unleased Proxy with a current successful YouTube search health grant, or a Proxy on a live Worker Lease under Task Observation authority. An unleased grant ends when scheduled revalidation becomes due.
_Avoid_: Legacy healthy Proxy, HTTP-200 Proxy

**Warm Reserve**:
A Verified Active Proxy that is not assigned to a Slot, is not cooling down, and is still inside its scheduled recheck interval. This is the only inventory that may be assigned as a new Route.
_Avoid_: Active count, unassigned inventory

**Quarantined Proxy**:
A `failed` Proxy that was removed from a Slot after a route-changing Task Observation. It remains recoverable, waits through its cooldown, and returns to `active` only after the authoritative YouTube probe passes. It is not archived by the route change itself.
_Avoid_: Archived Proxy, stale Route

**Task Observation**:
Evidence reported by a Worker for one active Task and one Route Fence.
_Avoid_: Health Incident, generic error log

**Idle Route Transition**:
The controlled replacement or pause of a Lease-Owned Idle Slot after its current Proxy becomes ineligible.
_Avoid_: background swap, silent rebind

**Route Fence**:
The Slot, Lease, and Route generation identity that authorizes one managed network Route. A newer generation makes the older Route stale.
_Avoid_: Assignment version alone, proxy binding
