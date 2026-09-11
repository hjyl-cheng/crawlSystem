#!/bin/sh
# QY remote execution environment, revision 1. No job dispatch or crawler setup.
# Invoke as root: bootstrap.sh check|docker|layout|verify NODE_UUID
set -eu
umask 077
export DEBIAN_FRONTEND=noninteractive
unset DOCKER_HOST DOCKER_CONTEXT DOCKER_TLS_VERIFY DOCKER_CERT_PATH
step=${1:-}
node_id=${2:-}
case "$step" in check|docker|layout|verify) ;; *) exit 64 ;; esac
printf '%s' "$node_id" | LC_ALL=C grep -Eq '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' || exit 64
[ "$(id -u)" = 0 ] || exit 77
command -v flock >/dev/null
exec 9>/run/lock/qy-node-runtime.lock
flock -n 9 || exit 75
fail() { printf 'QY_RUNTIME_ERROR=%s\n' "$1"; exit 1; }
arch=$(uname -m)
case "$arch" in x86_64|aarch64) ;; *) fail unsupported_arch ;; esac
. /etc/os-release
case "$ID:$VERSION_ID" in ubuntu:22.04|ubuntu:24.04|debian:12|debian:13) ;; *) fail unsupported_os ;; esac
[ -d /run/systemd/system ] || fail systemd_required
command -v systemctl >/dev/null || fail systemd_required
# Keep a remote-side owner marker as well as the center registry. A mistaken
# duplicate registration must not prepare the same host under a different node.
if [ -e /etc/qy-node/runtime/node-id ]; then
  [ "$(cat /etc/qy-node/runtime/node-id)" = "$node_id" ] || fail node_identity_conflict
fi
if [ "$step" != check ]; then
  [ -f /etc/qy-node/runtime/node-id ] || fail preflight_required
fi
engine() { docker --host unix:///var/run/docker.sock "$@"; }
repository() {
  # Reuse an existing official source; never replace its key or configuration.
  if ! grep -qs 'download.docker.com/linux/' /etc/apt/sources.list /etc/apt/sources.list.d/*; then
    apt-get -o DPkg::Lock::Timeout=120 update >/dev/null 2>&1 || fail apt_update
    apt-get -o DPkg::Lock::Timeout=120 install -y --no-remove --no-install-recommends ca-certificates curl gnupg >/dev/null 2>&1 || fail apt_prerequisites
    staging=$(mktemp -d)
    trap 'rm -rf -- "$staging"' EXIT
    trap 'exit 130' HUP INT TERM
    curl --fail --silent --show-error --proto '=https' --tlsv1.2 --connect-timeout 15 --max-time 90 \
      "https://download.docker.com/linux/$ID/gpg" -o "$staging/docker.asc" 2>/dev/null || fail repository_key_download
    fingerprint=$(gpg --batch --show-keys --with-colons "$staging/docker.asc" 2>/dev/null | awk -F: '$1=="fpr" {print $10; exit}')
    [ "$fingerprint" = 9DC858229FC7DD38854AE2D88D81803C0EBFCD88 ] || fail repository_key_invalid
    install -d -m 755 /etc/apt/keyrings
    install -m 644 "$staging/docker.asc" /etc/apt/keyrings/qy-node-docker.asc
    apt_arch=$(dpkg --print-architecture)
    case "$apt_arch" in amd64|arm64) ;; *) fail unsupported_arch ;; esac
    case "${VERSION_CODENAME:-}" in jammy|noble|bookworm|trixie) ;; *) fail unsupported_os ;; esac
    printf 'deb [arch=%s signed-by=/etc/apt/keyrings/qy-node-docker.asc] https://download.docker.com/linux/%s %s stable\n' \
      "$apt_arch" "$ID" "$VERSION_CODENAME" > /etc/apt/sources.list.d/qy-node-docker.list
    chmod 644 /etc/apt/sources.list.d/qy-node-docker.list
    rm -rf -- "$staging"
    trap - EXIT HUP INT TERM
  fi
  apt-get -o DPkg::Lock::Timeout=120 update >/dev/null 2>&1 || fail apt_update
}
case "$step" in
  check)
    command -v apt-get >/dev/null || fail unsupported_os
    install -d -m 700 /etc/qy-node/runtime
    if [ ! -f /etc/qy-node/runtime/node-id ]; then
      printf '%s\n' "$node_id" > /etc/qy-node/runtime/node-id
    fi
    ;;
  docker)
    if ! command -v python3 >/dev/null; then
      apt-get -o DPkg::Lock::Timeout=120 update >/dev/null 2>&1 || fail apt_update
      apt-get -o DPkg::Lock::Timeout=120 install -y --no-remove --no-install-recommends python3 >/dev/null 2>&1 || fail apt_prerequisites
    fi
    if ! command -v docker >/dev/null; then
      # Do not silently replace a different container runtime installation.
      if command -v dockerd >/dev/null || command -v containerd >/dev/null || command -v podman >/dev/null; then
        fail existing_runtime_conflict
      fi
      repository
      apt-get -o DPkg::Lock::Timeout=120 install -y --no-remove --no-install-recommends \
        docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin >/dev/null 2>&1 || fail docker_install
    fi
    if ! engine compose version --short >/dev/null 2>&1; then
      repository
      apt-get -o DPkg::Lock::Timeout=120 install -y --no-remove --no-install-recommends docker-compose-plugin >/dev/null 2>&1 || fail compose_install
    fi
    if ! engine info >/dev/null 2>&1; then
      systemctl start docker >/dev/null 2>&1 || fail docker_start
    fi
    engine info >/dev/null 2>&1 || fail docker_unavailable
    systemctl enable docker >/dev/null 2>&1 || fail docker_enable
    ;;
  layout)
    install -d -m 755 /opt/qy-node/runtime
    install -d -m 700 /var/lib/qy-node/runtime /var/lib/qy-node/runtime/spool /etc/qy-node/runtime/secrets
    # Preparing directories must not start containers, open ports, distribute
    # database credentials or touch existing Docker daemon/network settings.
    ;;
  verify)
    engine info >/dev/null 2>&1 || fail docker_unavailable
    docker_version=$(engine version --format '{{.Server.Version}}')
    compose_version=$(engine compose version --short)
    case "$compose_version" in v2.*|2.*|v5.*|5.*) ;; *) fail compose_version ;; esac
    printf '%s\n%s\n' "$docker_version" "$compose_version" | LC_ALL=C grep -Eq '[^a-zA-Z0-9.+_-]' && fail version_format
    for directory in /var/lib/qy-node/runtime/spool /etc/qy-node/runtime/secrets; do
      [ -d "$directory" ] && [ "$(stat -c '%a:%u' "$directory")" = 700:0 ] || fail directory_permissions
    done
    printf '{"revision":1,"nodeId":"%s","arch":"%s","os":"%s","dockerVersion":"%s","composeVersion":"%s"}\n' \
      "$node_id" "$arch" "$ID" "$docker_version" "$compose_version"
    ;;
esac
