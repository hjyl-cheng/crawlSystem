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
Persisted, conclusive health evidence that can make a Proxy ineligible independently of a Task Observation.
_Avoid_: Observation, Worker failure

**Task Observation**:
Evidence reported by a Worker for one active Task and one Route Fence.
_Avoid_: Health Incident, generic error log

**Idle Route Transition**:
The controlled replacement or pause of a Lease-Owned Idle Slot after its current Proxy becomes ineligible.
_Avoid_: background swap, silent rebind

**Route Fence**:
The Slot, Lease, and Route generation identity that authorizes one managed network Route. A newer generation makes the older Route stale.
_Avoid_: Assignment version alone, proxy binding
