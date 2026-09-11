# Connection verification image. This image does not claim channel tasks.
# Build from repository root; Go relay is built from the same source revision.
ARG GO_IMAGE=golang:1.25.3-bookworm
ARG NODE_IMAGE=node:20-bookworm-slim
FROM ${GO_IMAGE} AS relay
WORKDIR /src
ENV GOMAXPROCS=2 GOMEMLIMIT=384MiB
COPY services/rota/core/go.mod services/rota/core/go.sum ./
RUN go mod download
COPY services/rota/core .
RUN CGO_ENABLED=0 go build -p 2 -trimpath -ldflags="-s -w" -o /out/node-forward ./cmd/node-forward

FROM ${NODE_IMAGE}
WORKDIR /app
COPY services/remote-node/package.json ./
COPY --from=relay /out/node-forward /usr/local/bin/node-forward
COPY services/qybullmq/src/remoteNodes/protocol.js services/qybullmq/src/remoteNodes/client.js services/qybullmq/src/remoteNodes/localRotaClient.js services/qybullmq/src/remoteNodes/workerConfig.js services/qybullmq/src/remoteNodes/workerConnection.js services/qybullmq/src/remoteNodes/nodeConnectionRuntime.js ./src/remoteNodes/
COPY services/qybullmq/scripts/runRemoteNodeConnection.mjs ./scripts/
ARG QY_VCS_REF=uncommitted
LABEL org.opencontainers.image.title="qy-remote-node-connection" \
      org.opencontainers.image.revision="${QY_VCS_REF}" \
      qy.remote.mode="connect_only"
USER 1000:1000
HEALTHCHECK --interval=10s --timeout=5s --start-period=30s --retries=3 CMD ["node", "scripts/runRemoteNodeConnection.mjs", "--healthcheck"]
CMD ["node", "scripts/runRemoteNodeConnection.mjs"]
