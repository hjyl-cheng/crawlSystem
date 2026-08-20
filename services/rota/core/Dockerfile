# Build stage
FROM golang:1.25.3-alpine AS builder

# Install build dependencies
RUN apk add --no-cache git ca-certificates tzdata

ARG DBIP_VERSION=2026-08
ARG DBIP_SHA1=2ca01eac8c6d8f375a2d615529519be75c580441

# Set working directory
WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# DB-IP Country Lite is distributed under CC BY 4.0.
RUN mkdir -p /app/geoip && \
    wget --no-verbose -O /tmp/dbip-country-lite.mmdb.gz \
      "https://download.db-ip.com/free/dbip-country-lite-${DBIP_VERSION}.mmdb.gz" && \
    gzip -dc /tmp/dbip-country-lite.mmdb.gz > /app/geoip/dbip-country-lite.mmdb && \
    echo "${DBIP_SHA1}  /app/geoip/dbip-country-lite.mmdb" | sha1sum -c -

# Copy source code
COPY . .

# Build the application
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags='-w -s -extldflags "-static"' \
    -a -installsuffix cgo \
    -o /app/bin/server \
    ./cmd/server/main.go && \
    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags='-w -s -extldflags "-static"' \
    -a -installsuffix cgo \
    -o /app/bin/migrate \
    ./cmd/migrate/main.go

# Final stage
FROM alpine:latest

# Install runtime dependencies
RUN apk --no-cache add ca-certificates tzdata

# Create non-root user
RUN addgroup -g 1000 appuser && \
    adduser -D -u 1000 -G appuser appuser

WORKDIR /app

# Copy binary from builder
COPY --from=builder /app/bin/server /app/server
COPY --from=builder /app/bin/migrate /app/migrate
COPY --from=builder /app/geoip/dbip-country-lite.mmdb /app/geoip/dbip-country-lite.mmdb
COPY --from=builder /app/THIRD_PARTY_NOTICES.md /app/THIRD_PARTY_NOTICES.md

# Change ownership
RUN mkdir -p /app/geoip/managed && chown -R appuser:appuser /app

ARG QY_BUILD_VERSION=development
ARG QY_VCS_REF=uncommitted
ARG QY_BUILD_DATE=unknown
ARG QY_SOURCE_URL=local:pachongsys
LABEL org.opencontainers.image.title="qy-rota-core" \
      org.opencontainers.image.version="${QY_BUILD_VERSION}" \
      org.opencontainers.image.revision="${QY_VCS_REF}" \
      org.opencontainers.image.created="${QY_BUILD_DATE}" \
      org.opencontainers.image.source="${QY_SOURCE_URL}"

# Switch to non-root user
USER appuser

# Expose ports
EXPOSE 8000 8001

ENV GEOIP_DATABASE_PATH=/app/geoip/dbip-country-lite.mmdb

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:8001/health || exit 1

# Run the application
ENTRYPOINT ["/app/server"]
