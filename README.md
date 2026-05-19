# workflow-plugin-github

> ⚠️ **Experimental** — This plugin compiles and passes its unit tests but has not been validated in any active GoCodeAlone-internal production deployment. Use with caution. Please [open an issue](https://github.com/GoCodeAlone/workflow-plugin-github/issues/new) if you adopt it so we can promote it to **verified** status.

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

### Step: `step.gh_compute_gateway`

Submits a GitHub-origin workload to workflow-compute's protected gateway. The
compute server remains in the execution path; raw agents are not registered as
GitHub runners. When `write_check` is enabled, the check target must match the
repository and SHA verified by the compute server.

```yaml
- type: step.gh_compute_gateway
  config:
    server_url: "${COMPUTE_SERVER_URL}"
    token: "${COMPUTE_GITHUB_TOKEN}"
    repository: "GoCodeAlone/workflow-compute"
    oidc_token: "{{.github_oidc_token}}"
    workflow_run_id: "{{.run_id}}"
    workflow_job_id: "{{.job_id}}"
    workflow_job_name: "build"
    ref: "{{.ref}}"
    sha: "{{.sha}}"
    org_id: "org-1"
    pool_id: "pool-1"
    policy_id: "policy-1"
    command_args: ["go", "test", "./..."]
    wait: true
    write_check: true
    check_owner: "GoCodeAlone"
    check_repo: "workflow-compute"
    check_sha: "{{.sha}}"
    check_name: "workflow-compute"
    check_token: "${GITHUB_TOKEN}"
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
