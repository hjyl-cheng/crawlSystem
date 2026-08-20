# Qybullmq Roles

Every row below uses `services/qybullmq/Dockerfile` and the same image digest.

| Compose service | Entry point | Queue or responsibility | Rota role |
| --- | --- | --- | --- |
| `qybullmq-api` | `src/server.js` | API and Bull Board | none |
| `controller` | `src/controller.js` | backpressure, repair, batching | control API only |
| `worker-query-quality` | `src/worker.js` | `youtube-query-quality` | `query_quality` |
| `worker-discover` | `src/worker.js` | `youtube-discover-page` | `discover` |
| `worker-channel` | `src/worker.js` | `youtube-channel-crawl` | `channel` |
| `worker-incremental` | `src/worker.js` | `youtube-channel-incremental` | `channel` |
| `worker-data-api` | `src/worker.js` | `youtube-data-api-batch` | none |
| `worker-agent` | `src/worker.js` | full and incremental local Agent | none |
| `worker-finalize` | `src/worker.js` | `youtube-finalize` | none |
| `feature-relay` | `src/runFeatureRecalcRelay.js` | Feature HTTP relay | none |
| `crawler-outbox-publisher` | `src/runCrawlerOutboxPublisher.js` | crawler event outbox | none |
| `publication-publisher` | `src/runPublicationPublisher.js` | publication transport | none |
| `business-publication-ingress` | `src/runBusinessPublicationIngress.js` | authenticated ingress | none |
| `business-publication-reconciler` | `src/runBusinessPublicationReconciler.js` | ordered reconciliation | none |
| `business-publication-projector` | `src/runBusinessPublicationProjector.js` | business projection | none |

Recommended starting scale is one replica per service. Increase only network
workers for which Rota has ready capacity:

```bash
QYBULLMQ_IMAGE_TAG=<immutable-tag> ./scripts/compose.sh production up -d \
  --scale worker-channel=20 \
  --scale worker-incremental=5
```

Do not add `container_name` to scalable workers. With no explicit
`PROXY_WORKER_ID`, each replica uses its unique container hostname as the Rota
lease identity.
