#!/usr/bin/env bash
set -euo pipefail

archive="${GITHUB_ACTIONS_RUNNER_ARCHIVE:-/opt/actions-runner/actions-runner.tar.gz}"
runner_dir="${GITHUB_ACTIONS_RUNNER_DIR:-/home/runner/actions-runner}"

if [ ! -x "$runner_dir/config.sh" ]; then
  mkdir -p "$runner_dir"
  tar -xzf "$archive" -C "$runner_dir"
fi

exec /usr/local/bin/github-actions-runner-job "$@"
