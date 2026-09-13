#!/bin/sh
# Fixed, bounded cleanup of an initialized node with no deployments or data.
# Docker, SSH access, installed binaries and collected data are preserved.
set -eu
node_id=$1
mode=$2
case "$mode" in check|cleanup) ;; *) exit 2;; esac
fail() { printf 'QY_DELETE_BLOCK=%s\n' "$1"; exit 0; }
exec 9>/run/lock/qy-node-runtime.lock
flock -n 9 || fail operation
exec 8>/run/lock/qy-node-initialize.lock
flock -n 8 || fail operation
if [ -e /etc/qy-node/runtime/node-id ]; then
  [ "$(cat /etc/qy-node/runtime/node-id)" = "$node_id" ] || fail identity
fi
for directory in /etc/qy-node/runtime/deployments /var/lib/qy-node/spool /var/lib/qy-node/runtime/spool /etc/qy-node/runtime/secrets; do
  if [ -e "$directory" ]; then
    [ -d "$directory" ] || fail deployment
    contents=$(find "$directory" -mindepth 1 -print -quit) || fail deployment
    [ -z "$contents" ] || fail deployment
  fi
done
# No registered deployments should exist. Fail closed for ANY container,
# including stopped containers which could restart, rather than infer ownership.
if [ -S /var/run/docker.sock ] && ! command -v docker >/dev/null 2>&1; then fail docker; fi
if command -v docker >/dev/null 2>&1; then
  containers=$(timeout 15 docker --host unix:///var/run/docker.sock ps -aq) || fail docker
  [ -z "$containers" ] || fail containers
fi
# Workers installed through this page run in Docker. Also reject recognizable
# unmanaged worker processes, so manual installations cannot be silently ignored.
processes=$(ps -eo comm=) || fail processes
if printf '%s\n' "$processes" | grep -E '^[[:space:]]*(node|nodejs|python|python3)[[:space:]]*$' >/dev/null; then
  fail processes
fi
unit=/etc/systemd/system/qy-beszel-agent.service
if [ -e "$unit" ]; then
  grep -qxF 'ExecStart=/opt/qy-node/beszel-agent' "$unit" || fail monitoring
  grep -qxF 'EnvironmentFile=/etc/qy-node/beszel.env' "$unit" || fail monitoring
elif systemctl is-active --quiet qy-beszel-agent.service; then
  fail monitoring
fi
if [ "$mode" = cleanup ]; then
  if [ -e "$unit" ]; then
    systemctl disable --now qy-beszel-agent.service >/dev/null 2>&1 || fail monitoring
    if systemctl is-active --quiet qy-beszel-agent.service; then fail monitoring; fi
    rm -- "$unit"
    systemctl daemon-reload || fail monitoring
  fi
  rm -f -- /etc/qy-node/beszel.env /etc/qy-node/runtime/node-id
fi
printf 'QY_DELETE_OK\n'
