# workflow-plugin-github

GitHub integration plugin for the [workflow engine](https://github.com/GoCodeAlone/workflow). Provides GitHub webhook handling and GitHub Actions workflow management.

## Capabilities

### Module: `git.webhook`

Receives GitHub webhook events, validates HMAC-SHA256 signatures, normalizes payloads to a common `GitEvent` schema, and publishes to a configured message broker topic.

```yaml
modules:
  - name: github-webhooks
    type: git.webhook
    config:
      provider: github
      secret: "${GITHUB_WEBHOOK_SECRET}"
      events: [push, pull_request, release]
      topic: "git.events"
```

The module registers an HTTP handler at `/webhooks/github`. Configure your GitHub repository webhook to point to `https://<host>/webhooks/github`.

### Module: `github.runner_provider`

Provides the GitHub-owned side of the workflow-compute runner provider boundary.
It mints repository-scoped GitHub Actions runner registration tokens and removes
runners without exposing GitHub API credentials to workflow-compute.

```yaml
modules:
  - name: github-runners
    type: github.runner_provider
    config:
      token: "${GITHUB_TOKEN}"
      provider_token: "${GITHUB_RUNNER_PROVIDER_TOKEN}"
      repositories: ["GoCodeAlone/workflow-compute"]
```

For local proof runs, the repo also builds `github-runner-provider`, a small
HTTP provider service:

```sh
GITHUB_TOKEN=... \
GITHUB_RUNNER_PROVIDER_TOKEN=... \
GITHUB_RUNNER_PROVIDER_REPOSITORIES=GoCodeAlone/workflow-compute \
  bin/github-runner-provider 127.0.0.1:8090
```

workflow-compute should point at that service with
`COMPUTE_GITHUB_RUNNER_PROVIDER_URL` and
`COMPUTE_GITHUB_RUNNER_PROVIDER_TOKEN`; it should not receive `GITHUB_TOKEN`.

### Step: `step.gh_action_trigger`

Triggers a GitHub Actions workflow via `workflow_dispatch`.

```yaml
- type: step.gh_action_trigger
  config:
    owner: "GoCodeAlone"
    repo: "workflow"
    workflow: "ci.yml"
    ref: "main"
    inputs:
      environment: "staging"
    token: "${GITHUB_TOKEN}"
```

### Step: `step.gh_action_status`

Checks or polls the status of a GitHub Actions workflow run.

```yaml
- type: step.gh_action_status
  config:
    owner: "GoCodeAlone"
    repo: "workflow"
    run_id: 123456
    token: "${GITHUB_TOKEN}"
    wait: true
    poll_interval: "10s"
    timeout: "30m"
```

### Step: `step.gh_create_check`

Creates a GitHub Check Run (commit status check).

```yaml
- type: step.gh_create_check
  config:
    owner: "GoCodeAlone"
    repo: "workflow"
    sha: "abc123"
    name: "workflow-ci"
    status: "completed"
    conclusion: "success"
    title: "CI Pipeline"
    summary: "All tests passed"
    token: "${GITHUB_TOKEN}"
```

## Building

```sh
make build
# binary at bin/workflow-plugin-github
```

## Testing

```sh
make test
```

## Installation

```sh
make install DESTDIR=/path/to/workflow
```

## License

MIT
