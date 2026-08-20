FROM python:3.12-slim

WORKDIR /app

COPY pyproject.toml ./
COPY src ./src
RUN pip install --no-cache-dir ".[postgres]"

COPY sql ./sql
COPY deployment ./deployment

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    TZ=UTC

ARG QY_BUILD_VERSION=development
ARG QY_VCS_REF=uncommitted
ARG QY_BUILD_DATE=unknown
ARG QY_SOURCE_URL=local:pachongsys
LABEL org.opencontainers.image.title="qy-feature-engine" \
      org.opencontainers.image.version="${QY_BUILD_VERSION}" \
      org.opencontainers.image.revision="${QY_VCS_REF}" \
      org.opencontainers.image.created="${QY_BUILD_DATE}" \
      org.opencontainers.image.source="${QY_SOURCE_URL}"

USER 65532:65532

EXPOSE 8090

CMD ["feature-ingest"]
