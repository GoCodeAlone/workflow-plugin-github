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

#### Retained Linux provider

The release archive includes `github-runner-provider` for a user-scoped Linux
installation alongside a retained workflow-compute agent. The host must have a
lingering user systemd manager, rootless Podman, and an agent bundle that
supports supervisor maintenance and signed package verification. The strict
non-secret config is generated for the registered worker and must use absolute
paths under that user's home.

Enable the user manager once with administrative access, then verify the rest
as the retained agent user. Installation rejects UID 0, disabled linger, a
missing user manager, and a non-rootless Podman runtime.

```sh
sudo loginctl enable-linger "$USER"
systemctl --user show-environment >/dev/null
test "$(loginctl show-user "$(id -u)" --property Linger --value)" = yes
test "$(podman info --format '{{.Host.Security.Rootless}}')" = true
```

The archive ships
`schemas/github-runner-retained-config.schema.json` and the runtime-validated
`examples/github-runner-retained-config.json`. Place the config in an
owner-only regular file under the retained user's home. Replace
`/home/wfcompute` with that exact home, and set the worker/profile IDs, agent
unit, supervisor paths, provider marker path, organization/repository, runner
group/labels, workflow, and full 40-character commit `ref` from the retained
agent registration and provider campaign. The following constraints are also
enforced by the runtime decoder:

- `install_root` is exactly
  `$HOME/.workflow-compute/github-runner-provider`.
- `provider_url` uses HTTPS on port `18090`; its host equals
  `stable_container`, and `candidate_container` is different.
- `component_id` identifies the provider component in the agent supervisor
  config, while `provider_marker_path` identifies that component's signed
  current-update marker.
- `podman_path`, `systemctl_path`, and `loginctl_path` identify canonical
  executable paths outside provider-managed state. Install rejects symlinks,
  non-regular files, untrusted ownership, group/world-writable executables, and
  lifecycle recovery re-attests their recorded digests before mutation.
- Every configured user path stays below the same home and has no symlinked
  existing component. The config contains no credentials.

Run the one-time install or an idempotent reinstall with credentials in the
process environment, never in the config or command arguments:

```sh
GITHUB_RUNNER_PROVIDER_GITHUB_TOKEN="${GITHUB_TOKEN}" \
GITHUB_RUNNER_PROVIDER_TOKEN="${PROVIDER_TOKEN}" \
  github-runner-provider retained install -config <absolute-config-path>
```

After installation, autonomous refresh is driven by the retained agent's
signed package marker and a recurring user-systemd timer. A workflow is not
needed for routine provider updates. Check the redacted local state with:

```sh
github-runner-provider retained status -config <absolute-config-path>
```

For credential rotation, re-run `retained install` with the same config and the
replacement environment values. The transaction preserves the worker identity,
provider state, and retained-agent registration. A credential reinstall also
rotates the private CA and server key. Between reinstalls, refresh renews the
provider server certificate before its final 30 days while retaining the CA and
server key.

Interrupted current-format transactions recover automatically on the next
lifecycle command. Only when an error explicitly reports an unbound legacy
provider transaction should an operator use the exact transaction ID printed
by that error:

```sh
github-runner-provider retained recover -config <absolute-config-path> \
  -confirm <exact-legacy-provider-transaction-id>
```

Uninstall is deliberately separate from install/update orchestration. The
default retains provider state and credentials so a later reinstall can recover
ownership safely:

```sh
github-runner-provider retained uninstall -config <absolute-config-path>
```

Only remove retained state and credentials after update/reconnect evidence is
complete:

```sh
github-runner-provider retained uninstall -config <absolute-config-path> --purge
```

GitHub workflow output is orchestration evidence only. Acceptance requires a
job dispatched by workflow-compute STG to the registered agent and validation
through the STG task, proof, log, and artifact APIs.

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
