FROM node:20-bookworm-slim

WORKDIR /app

ENV NODE_ENV=production \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    ROTA_IDENTITY_POLICY_CATALOG_FILE=/app/config/identity-policy-catalog.json \
    TZ=UTC

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
      ca-certificates \
      curl \
      python3 \
      python3-pip \
    && rm -rf /var/lib/apt/lists/*

COPY services/qybullmq/package.json services/qybullmq/package-lock.json ./
RUN npm ci --omit=dev

COPY services/qybullmq/requirements.txt /tmp/qybullmq-requirements.txt
COPY services/local-agent/pyproject.toml /opt/qy-channel-profile/source/
COPY services/local-agent/src /opt/qy-channel-profile/source/src
RUN pip3 install --break-system-packages --no-cache-dir \
      -r /tmp/qybullmq-requirements.txt \
      "/opt/qy-channel-profile/source[runtime]"

COPY services/qybullmq/src ./src
COPY services/qybullmq/scripts ./scripts
COPY services/local-agent/models /opt/qy-channel-profile/model-bundle
COPY services/rota/core/internal/proxycontrol/identity_policy_catalog.generated.json /app/config/identity-policy-catalog.json

ARG QY_BUILD_VERSION=development
ARG QY_VCS_REF=uncommitted
ARG QY_BUILD_DATE=unknown
ARG QY_SOURCE_URL=local:pachongsys
LABEL org.opencontainers.image.title="qy-qybullmq" \
      org.opencontainers.image.version="${QY_BUILD_VERSION}" \
      org.opencontainers.image.revision="${QY_VCS_REF}" \
      org.opencontainers.image.created="${QY_BUILD_DATE}" \
      org.opencontainers.image.source="${QY_SOURCE_URL}"

EXPOSE 3000

CMD ["node", "src/server.js"]
