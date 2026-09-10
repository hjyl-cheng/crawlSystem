FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends openssh-server sudo systemd ca-certificates curl procps util-linux && rm -rf /var/lib/apt/lists/*
RUN useradd -m -s /bin/bash ubuntu && usermod -aG sudo ubuntu && mkdir -p /run/sshd /run/systemd/system && ssh-keygen -A
# This isolated fixture exercises real SSH, sudo, files, agent and Hub. The
# service-manager shim avoids granting a test container host cgroup privileges.
COPY systemctl-fixture /usr/local/bin/systemctl
COPY entrypoint /entrypoint
RUN chmod 755 /usr/local/bin/systemctl /entrypoint
EXPOSE 22
ENTRYPOINT ["/entrypoint"]
