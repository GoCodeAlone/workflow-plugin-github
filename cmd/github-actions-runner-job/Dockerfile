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
  case "$runner_arch" in \
    x64) runner_sha256="18f8f68ed1892854ff2ab1bab4fcaa2f5abeedc98093b6cb13638991725cab74" ;; \
    arm64) runner_sha256="69ac7e5692f877189e7dddf4a1bb16cbbd6425568cd69a0359895fac48b9ad3b" ;; \
    *) echo "unsupported actions runner arch: $runner_arch" >&2; exit 1 ;; \
  esac; \
  mkdir -p /opt/actions-runner; \
  curl --fail --show-error --location --retry 5 --retry-all-errors --retry-delay 2 --connect-timeout 15 \
    "https://github.com/actions/runner/releases/download/v${ACTIONS_RUNNER_VERSION}/actions-runner-linux-${runner_arch}-${ACTIONS_RUNNER_VERSION}.tar.gz" \
    -o /opt/actions-runner/actions-runner.tar.gz; \
  echo "${runner_sha256}  /opt/actions-runner/actions-runner.tar.gz" | sha256sum -c -; \
  tar -tzf /opt/actions-runner/actions-runner.tar.gz ./config.sh ./run.sh >/dev/null; \
  chmod 0644 /opt/actions-runner/actions-runner.tar.gz; \
  useradd -m -s /bin/bash runner

COPY --chmod=0755 ${RUNNER_JOB_BINARY} /usr/local/bin/github-actions-runner-job
COPY --chmod=0755 cmd/github-actions-runner-job/entrypoint.sh /usr/local/bin/github-actions-runner-job-entrypoint

USER runner
WORKDIR /home/runner
ENV GITHUB_ACTIONS_RUNNER_ARCHIVE=/opt/actions-runner/actions-runner.tar.gz
ENV GITHUB_ACTIONS_RUNNER_DIR=/home/runner/actions-runner
ENTRYPOINT ["/usr/local/bin/github-actions-runner-job-entrypoint"]
