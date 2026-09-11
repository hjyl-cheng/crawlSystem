# Build the sibling node-onboarding fixture first; no host Docker socket needed.
ARG SSH_FIXTURE_IMAGE=qy-node-onboarding-sshd-test:latest
FROM ${SSH_FIXTURE_IMAGE}
RUN /usr/bin/apt-get update && /usr/bin/apt-get install -y --no-install-recommends python3 && rm -rf /var/lib/apt/lists/*
COPY mock-command /fixture/mock-command
RUN mkdir -p /test-state && printf 'qy-node-runtime-fixture-v1' > /test-state/fixture-only && chmod 755 /fixture/mock-command && \
    for program in apt-get systemctl curl gpg; do ln -sf /fixture/mock-command /usr/local/bin/$program; done
