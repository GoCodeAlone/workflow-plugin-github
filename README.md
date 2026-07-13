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
It mints general repository- or organization-scoped registration tokens,
preflights organization runner labels/groups, creates exact ephemeral JIT runner
configurations, and removes runners without exposing GitHub API credentials to
workflow-compute.

```yaml
modules:
  - name: github-runners
    type: github.runner_provider
    config:
      token: "${GITHUB_TOKEN}"
      provider_token: "${GITHUB_RUNNER_PROVIDER_TOKEN}"
      repositories: ["GoCodeAlone/workflow-compute"]
      organizations: ["GoCodeAlone"]
      runner_groups: ["workflow-compute-stg"]
      state_dir: "/var/lib/workflow-github-runner-provider"
```

For local proof runs, the repo also builds `github-runner-provider`, a small
HTTP provider service:

```sh
GITHUB_TOKEN=... \
GITHUB_RUNNER_PROVIDER_TOKEN=... \
GITHUB_RUNNER_PROVIDER_REPOSITORIES=GoCodeAlone/workflow-compute \
GITHUB_RUNNER_PROVIDER_ORGANIZATIONS=GoCodeAlone \
GITHUB_RUNNER_PROVIDER_RUNNER_GROUPS=workflow-compute-stg \
GITHUB_RUNNER_PROVIDER_STATE_DIR=/var/lib/workflow-github-runner-provider \
  bin/github-runner-provider 127.0.0.1:8090
```

For a provider endpoint reachable outside host loopback, configure TLS with both
`GITHUB_RUNNER_PROVIDER_TLS_CERT_FILE` and
`GITHUB_RUNNER_PROVIDER_TLS_KEY_FILE`. The runner job continues to reject
plaintext bearer transport except for literal loopback IP endpoints and never
follows provider redirects. A private certificate authority can be supplied to
the runner job as base64-encoded PEM in
`COMPUTE_GITHUB_RUNNER_PROVIDER_CA_CERT_B64`; certificate verification and
hostname verification remain enabled.

workflow-compute should point at that service with
`COMPUTE_GITHUB_RUNNER_PROVIDER_URL` and
`COMPUTE_GITHUB_RUNNER_PROVIDER_TOKEN`; it should not receive `GITHUB_TOKEN`.
The `ephemeral_runner_job` operation uses the authenticated JIT endpoint, starts
the bundled listener with `run.sh --jitconfig`, and unregisters only the exact
provider-owned JIT runner ID recorded in the ownership journal. Workload outputs
are returned through the declared `github-workload-outputs.tar.gz` provider
artifact rather than arbitrary names.

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

### Step: `step.gh_upstream_release_monitor`

Checks the latest release tag for an upstream GitHub repository and reports
whether it differs from the tag your application has pinned. Public repositories
can be checked without a token; private repositories or higher-rate checks can
provide `token`.

```yaml
- name: check_upstream
  type: step.gh_upstream_release_monitor
  config:
    upstream_owner: "{{ .upstream_owner }}"
    upstream_repo: "{{ .upstream_repo }}"
    pinned_tag: "{{ .pinned_tag }}"
    token: "${GITHUB_TOKEN}"
```

The step is intentionally read-only. Compose `update_available` with
`step.conditional`, repo-owned update workflows, `step.gh_action_trigger`,
`step.gh_action_status`, `step.gh_pr_create`, and `step.gh_pr_merge` when an
application should re-pin an upstream dependency.

Workflow-compute workloads should be routed through a workflow-compute provider
or through GitHub's normal self-hosted runner/webhook surfaces. This plugin does
not expose workflow-compute gateway client steps or generic check-run creation
steps.

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
