# Server node execution controls — 2026-09-11

## Behavior

The server card menu closes on an outside click, Escape, or opening another card menu. Cards and node details expose **开始接任务** and **暂停接任务**, with connected, ready, active, and idle counts refreshed alongside monitoring.

Deployment leaves new remote Worker slots paused. Starting a node changes its desired intake through the authenticated central deployment API. The center validates the deployment, exact slot count, node state, and live connections. Registry version and expected desired state reject stale conflicting requests. Node credentials cannot authorize this operation.

Pausing changes `activation_requested`, retaining execution admission until the current processor finishes. The supervisor gracefully closes the BullMQ consumer, then releases its route and activation. The UI shows **正在收尾…** while this is pending. A pause during route allocation cannot be overwritten by a late activation. New slots added during scale-out remain paused until explicitly enabled.

Remote and local consumers use the existing incremental BullMQ queue, Redis database 0, and `bull` prefix. No separate Plan generation or migration queue was introduced. Existing execution fences and recovery remain in force. No new schema is needed.

## Deployment

- Center image: `qy-allpachong/remote-node-center:node-control-20260911`.
- Dashboard image: `qy-allpachong/dashboard:node-control-20260911`.
- Center uses `REMOTE_NODE_EXECUTION_ENABLED=true` and `REMOTE_NODE_EXECUTION_ADMISSION=dashboard`; only registered deployments with explicitly requested activation qualify. Existing supervisor capacity limit remains 32 slots.
- Center joined the existing Rota control network. The private nginx proxy gained the exact authenticated deployment execution route and was gracefully reloaded. Public proxy configuration and Rota processes were unchanged.
- Existing node `54a7cdd3-eb9c-4713-8d2f-21f4a5279de0` had its three desired activation flags set to false before enabling the supervisor, after checking that the Workers were never activated and had no remote tasks.
- Only the center and Dashboard containers were recreated. All 132 other containers in the deployment baseline retained their IDs and start times and were running after deployment.

## Validation

- Four PostgreSQL/Redis integration tests passed: deployment administration, activation, supervisor, and node execution control. The latter covers shared queue consumption, graceful pause, restart persistence, resume, unavailable Workers, deployment mismatch, and pause during route startup.
- Center entry integration test passed with dashboard admission and no static node allowlist.
- Eighteen remote handoff regression tests passed, including process recovery, country routing, and API wait handling.
- Dashboard suite: 66 passed, 8 environment-dependent skips, no failures.
- Browser fixture passed outside-click/Escape, menu switching, start/pause controls, node-scoped requests, detail controls, and mobile overflow checks.
- Live Dashboard GET execution status and end-to-end POST pause passed through the private proxy to the center. All three Workers were connected, requested=false, enabled=false, active=0, with zero remote tasks. No production start command was sent: the user will initiate collection.
- Live Dashboard served the updated script. An unauthenticated public asset request correctly redirected to the login page; it was not used as evidence of an authenticated browser session.

Production configuration backups and fixed image selections are kept in the ignored private runtime directory. No credentials are included here. Changes are not committed by this task.
