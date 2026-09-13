# Initialized unused-node deletion

The page previously permitted only metadata-only registrations to be deleted. A successful SSH/Beszel initialization permanently blocked deletion even when the saved Worker count was only a plan and no deployment existed.

The Dashboard now permits initialized execution nodes with no deployment record to enter a guarded cleanup flow. A durable registry reservation blocks edits, initialization and deployment during deletion, including after partial cleanup failures. Version checks and operation IDs reject stale requests. No schema migration is required.

Before cleanup, the Dashboard checks that the center has no node identity, deployment, worker connection or assigned task for that node, then verifies the pinned SSH connection and remote state. Both remote runtime locks must be available. Containers (including stopped containers), deployment files, spool/secrets contents, unverified worker-capable processes and a different node identity block removal. The remote script rechecks before stopping the owned Beszel service and removing its configuration and the unused runtime identity. The center's matching monitoring record and registration tokens are removed, followed by the node registry entry. Failures retain the registry and support retry.

Docker, SSH access/keys, installed binaries and collected data are retained. Registered/deployed nodes still require a separate decommission workflow; this change does not implement worker draining. The optional sudo password is transient. Existing migration and incremental collection paths are unchanged.

Validation:

- Node 22 Dashboard suite: 69 passed, 3 environment-dependent tests skipped, 0 failed. Tests sharing the original node registry test database ran sequentially.
- Final focused deletion tests: 7 passed, 0 skipped, including real SSH/key login/sudo in an isolated fixture. Checks covered retained spool data, stopped containers, Docker unavailability, node identity mismatch and repeat cleanup.
- Isolated PostgreSQL test covered center-task rejection, durable partial failure, competing deployment, cleanup retry and re-adding the same address with a new UUID.
- Browser exercise covered opening the card menu, deletion preflight, transient password submission/clearing and automatic dialog closure.
- Read-only check of 增量节点01 (43.172.83.170) passed center and remote checks. Its registration was not deleted; the user will test deletion/re-addition from the page.

Deployment is Dashboard-only, image `qy-allpachong/dashboard:node-deletion-20260911`. The prior Dashboard compose file is saved as `runtime/remote-center-production/dashboard.node-deletion.rollback.compose.json` (local ignored operational configuration). No Worker/queue restart is required.

## Follow-up: Beszel fingerprint identifiers

The first production deletion attempts stopped at center monitoring cleanup. Beszel 0.19.0 returned fingerprint ID `z880rv9ve` (9 characters); the initial implementation incorrectly required 15 characters, conflating fingerprint IDs with our system ID format. Remote cleanup had completed, but the registry was correctly retained for retry. The earlier mocked monitor test used a 15-character ID and failed to cover this real response.

Fingerprint validation now accepts bounded lowercase alphanumeric IDs without requiring a fixed length, while retaining exact system ownership checks and rejecting unsafe path characters. The deletion dialog reports the previous failed step. A regression using the real short ID failed before the fix and passes after it. An isolated, actual Beszel 0.19.0 integration test verifies generated short IDs, removal, repeated cleanup, re-registration and preservation of a second system. All 7 focused tests passed with no skips. Updated Dashboard image: `qy-allpachong/dashboard:node-deletion-beszel-20260911`. The real node remains registered for the user's page-driven retry.

## Node details presentation

The node details dialog still rendered the original static “not deployed” badge and “deployment/runtime records not connected” note after successful deployment. It now reads the same persisted deployment status as the node card and shows both planned and applied counts. Opening details no longer suspends polling; the open dialog is refreshed after monitoring reads, without reopening it. A browser check covered deployed counts, a subsequent deployment transition, monitoring changes while details remained open, and zero write requests. Deployment image: `qy-allpachong/dashboard:node-details-20260911`. This presentation change does not enable remote execution or restart Workers.
