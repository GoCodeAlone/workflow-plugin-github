FROM mcr.microsoft.com/dotnet/runtime-deps:8.0-jammy

ARG TARGETARCH
ARG ACTIONS_RUNNER_VERSION=2.333.1
ARG RUNNER_JOB_BINARY=dist/github-actions-runner-job-linux-${TARGETARCH}

RUN apt-get update \
  && apt-get install -y --no-install-recommends ca-certificates curl git tar gzip bash \
  && rm -rf /var/lib/apt/lists/*

RUN set -eux; \
  runner_arch="${TARGETARCH}"; \
  if [ "$runner_arch" = "amd64" ]; then runner_arch="x64"; fi; \
  mkdir -p /opt/actions-runner; \
  curl -fsSL \
    "https://github.com/actions/runner/releases/download/v${ACTIONS_RUNNER_VERSION}/actions-runner-linux-${runner_arch}-${ACTIONS_RUNNER_VERSION}.tar.gz" \
    -o /opt/actions-runner/actions-runner.tar.gz; \
  chmod 0644 /opt/actions-runner/actions-runner.tar.gz; \
  useradd -m -s /bin/bash runner

COPY --chmod=0755 ${RUNNER_JOB_BINARY} /usr/local/bin/github-actions-runner-job
COPY --chmod=0755 cmd/github-actions-runner-job/entrypoint.sh /usr/local/bin/github-actions-runner-job-entrypoint

USER runner
WORKDIR /home/runner
ENV GITHUB_ACTIONS_RUNNER_ARCHIVE=/opt/actions-runner/actions-runner.tar.gz
ENV GITHUB_ACTIONS_RUNNER_DIR=/home/runner/actions-runner
ENTRYPOINT ["/usr/local/bin/github-actions-runner-job-entrypoint"]
