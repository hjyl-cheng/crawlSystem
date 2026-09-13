# NATS / JetStream transport experiment

This isolated experiment evaluates replacing remote-node HTTP command polling and result uploads with the official NATS server and JavaScript client. It is **not deployed**, and it is not a replacement scheduler. BullMQ remains the owner of channel jobs; the existing incremental runner remains the owner of collection policy.

Run with Docker Compose and Node.js (the existing qybullmq dependencies must also be installed):

```bash
cd services/remote-node/experiments/nats-transport
npm ci
npm test
```

The runner creates uniquely named test containers, a scratch `remote_node_ingestion_test` database, and a JetStream volume. It binds host ports to loopback, uses test-only credentials, caps both services at 0.5 CPU and 256 MiB, and removes its containers/network/volume in `finally`. It does not load production environment files. Results are written to `results.json` only after every assertion passes. The NATS image is pinned by digest; JS dependencies are locked separately from production dependencies.

## What is actually exercised

- Two authenticated node connections carry 100 concurrent logical workers. This measures multiplexed work, **not 100 machines or independent Node.js processes**.
- Commands remain associated with the original task, command ID, and execution generation. JetStream uses separate node-specific durable pull consumers.
- The result consumer invokes the actual `RemoteChannelPlanStore.receive` and `RemoteNodeStore` against their real PostgreSQL schema. Synthetic payloads go through the existing gzip result protocol.
- A further 1,000 distinct command results exercise new SQL writes with 100 concurrent publishers and eight concurrent result consumers. Heartbeat request/reply also invokes the actual SQL lease-renewal method while results are processed.
- A result is acknowledged to JetStream only after the existing receipt transaction commits. Closing the consumer connection between COMMIT and ACK causes redelivery to a replacement consumer; the original writer must accept it idempotently.
- Duplicate publication, stale generation, conflicting payload, and cross-node subject access are exercised explicitly.
- Stopping consumption leaves 40 durable pending results. SIGKILL of the NATS process followed by restart checks recovery of those messages and official-client reconnection. An unconfirmed upload is retained in the existing local result spool and replayed.
- Stream overflow uses `DiscardNew`: a full queue rejects new messages instead of silently evicting already accepted results.

## Interpretation and limits

Read `results.json` for measurements. Publish ACK latency is broker receipt latency; it is **not** database completion latency. SQL receipt throughput is **not** channels/hour and excludes YouTube, parsing, Agent, publication, and full incremental business processing. These are loopback tests on a server also running production services; they do not establish WAN/TLS latency, maximum scale, or superiority to an equally configured HTTP benchmark.

This test reuses the production receipt and generation checks. It does not run the complete business fence/Clock lifecycle through NATS, or switch actual workers. JetStream ACK is not BullMQ job completion. NATS message-ID deduplication has a finite window and cannot replace SQL idempotency.

The broker is a single replica. SIGKILL tests a process failure; it does not test machine power loss, kernel page-cache loss, disk loss, or a multi-server quorum. Production durability requires an explicit replication/fsync/storage decision. Core NATS heartbeats are transient; only a successful guarded SQL renewal extends the execution lease. Persistent command delivery must always be rechecked against current execution ownership before collection.

## Proposed production seam

Keep the existing runner, BullMQ channel ownership, Rota route ownership, and writer. Introduce an optional transport adapter at command delivery/result receipt, rather than changing collection strategies:

1. Commit commands using the existing SQL transaction. Deliver them to JetStream through a durable relay/outbox with stable message IDs. Publishing SQL + JetStream directly as two independent writes is insufficient.
2. A node consumes only its permitted subjects, checks current execution ownership, uses its original collector and local result spool, and publishes results with a stable identity. Broker publication acknowledgement permits spool cleanup but does not complete the channel.
3. A bounded pool of center result consumers invokes the original receipt writer, commits, then acknowledges the message. Preserve the result/generation fence and quarantine genuine conflicts with durable evidence.
4. Run heartbeat/lease handling independently of slow result consumers. Keep official-client reconnection with backoff/jitter and enforce spool/stream limits; reduce intake when downstream processing cannot keep up.
5. Provision per-node credentials and TLS, bind permissions to the registered node, and expose backlog/oldest-message/ACK-latency/error metrics. Remove access when retiring a node.
6. Verify the full original Plan lifecycle, public-network disconnection, and coexistence with the old transport before enabling any production node. A transport switch must have exactly one owner per execution.

Official components: [nats-server](https://github.com/nats-io/nats-server), [nats.js](https://github.com/nats-io/nats.js), [JetStream consumers](https://docs.nats.io/nats-concepts/jetstream/consumers), [JetStream message deduplication](https://docs.nats.io/using-nats/developer/develop_jetstream/model_deep_dive).
