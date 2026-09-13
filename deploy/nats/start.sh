#!/bin/sh
# Official server handles transport, persistence, reconnects and live auth reload.
# This small launcher only notices the center's atomically replaced ACL file.
set -eu
until [ -s /run/nats-auth/users.conf ]; do sleep 1; done
nats-server -c /etc/nats/server.conf &
server_pid=$!
trap 'kill -TERM "$server_pid" 2>/dev/null || true; wait "$server_pid" || true; exit 0' TERM INT
cert_file=${NATS_CERT_FILE:-/run/nats-tls/fullchain.pem}
key_file=${NATS_KEY_FILE:-/run/nats-tls/privkey.pem}
previous=$(sha256sum /run/nats-auth/users.conf "$cert_file" "$key_file")
while kill -0 "$server_pid" 2>/dev/null; do
  sleep 2 & wait $! || true
  current=$(sha256sum /run/nats-auth/users.conf "$cert_file" "$key_file")
  if [ "$current" != "$previous" ]; then
    if nats-server -t -c /etc/nats/server.conf >/dev/null 2>&1; then
      kill -HUP "$server_pid"
      previous=$current
    fi
  fi
done
wait "$server_pid"
