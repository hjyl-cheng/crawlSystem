# Worker capacity memory budget

The user approved replacing the sum of container limits with a capacity budget
based on observed incremental collection usage:

`required MiB = total Worker count × 256 + 1536`

The 1.5 GiB reserve covers the operating system, other services and variation.
The existing per-container `mem_limit: 768m` remains an independent protection
ceiling. This change applies to the currently supported incremental deployment
recipe; it does not open Query deployment or change collection/queue behavior.

## Evidence and interpretation

Read-only measurements on node 43.172.83.170 found MemTotal 7806160 KiB (7.44
GiB). Nine idle Workers used approximately 649 MiB altogether. The five older
Workers that had processed collection tasks had lifetime cgroup peaks of
184.22–194.86 MiB, no OOM kills and no restarts. The newer four Workers had only
startup/idle observations. The 256 MiB figure is a planning allowance based on
these samples, not a guarantee that every future task remains below it.

On this node, 20 Workers require a 6.5 GiB budget; 23 require 7.25 GiB; 24 require
7.5 GiB and exceed the measured total. These are memory admission calculations,
not measured sustainable concurrency or throughput limits.

## Implementation and checks

The browser preview, read-only SSH preflight and remote Python installer all use
the new budget. The installer independently enforces it even if the browser
estimate is stale. No table, Worker image or task-processing change is needed.

Regression tests first failed against the old checks, then passed for 20/23 and
rejected 24. The Python test reaches the actual `deploy(..., 'files')` path with
the real generated container recipe, stopping before any node filesystem write.
It also verifies that generated containers retain their 768 MiB limits. Browser
tests cover the same capacity boundaries and existing add/start/pause behavior.

Dashboard image: `qy-allpachong/dashboard:worker-memory-budget-20260911`.
Deployment uses the existing Dashboard configuration with a retained stopped
container and private configuration backup for rollback. Current runtime compose
and the checked-in intake override reference this image.

Production verification passed: the new read-only SSH check accepted 20 and 23
and rejected 24 on the real node. Served browser code and the installed Python
script both contained the new budget, with the 768 MiB container limit intact.
Dashboard's previous process exited 0. No Worker was added: center remained
20 deployed / 0 admitted, remote remained 9 deployed / 9 admitted. Both queues
were unpaused and Full Crawl had 40 active tasks.
