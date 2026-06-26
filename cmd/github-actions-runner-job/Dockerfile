ARG ACTIONS_RUNNER_IMAGE=ghcr.io/actions/actions-runner:2.333.1
FROM ${ACTIONS_RUNNER_IMAGE}

ARG TARGETARCH
ARG RUNNER_JOB_BINARY=dist/github-actions-runner-job-linux-${TARGETARCH}

USER root
COPY --chmod=0755 ${RUNNER_JOB_BINARY} /usr/local/bin/github-actions-runner-job

USER runner
WORKDIR /home/runner
ENV GITHUB_ACTIONS_RUNNER_DIR=/home/runner
ENTRYPOINT ["/usr/local/bin/github-actions-runner-job"]
