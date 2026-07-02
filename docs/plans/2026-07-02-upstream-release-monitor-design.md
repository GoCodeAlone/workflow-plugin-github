# GitHub Upstream Release Monitor Design

**Status:** Approved
**Date:** 2026-07-02
**Related:** decisions/0001-upstream-release-monitor-boundary.md

## Context

The Signal work now has repeated repo-owned drift workflows:

- `libsignal-go` monitors `signalapp/libsignal` releases and runs its own pin update.
- `libsignal-service-go` monitors Signal service upstream material and runs its own fetch/update scripts.
- `encrypted-spaces-go` monitors upstream Encrypted Spaces changes.

That repeated shape is no longer Signal-specific. Workflow should expose the reusable GitHub primitive that lets applications detect upstream release drift, then compose existing PR/action/merge steps with target-repo update scripts.

## Decision

Add a first-class `step.gh_upstream_release_monitor` to `workflow-plugin-github`.

The step is read-only. It fetches the latest release for a configured upstream GitHub repository, compares that tag to a configured pinned tag, and emits structured drift metadata. It does not clone repositories, run update commands, create branches, create PRs, enable automerge, or know any Signal-specific rules.

## Requirements

- `R1`: Expose a Workflow step named `step.gh_upstream_release_monitor`.
- `R2`: Require `upstream_owner`, `upstream_repo`, and `pinned_tag`.
- `R3`: Allow an optional `token` for authenticated GitHub API reads, while supporting public unauthenticated reads.
- `R4`: Resolve template expressions for the owner, repo, and pinned tag at runtime.
- `R5`: Emit `upstream_owner`, `upstream_repo`, `pinned_tag`, `latest_tag`, `update_available`, `release_id`, `release_url`, and `published_at`.
- `R6`: Return a pipeline-stopping error result when GitHub lookup fails.
- `R7`: Register strict proto config/input/output contracts and keep `plugin.json`, `plugin.contracts.json`, and runtime step types in sync.
- `R8`: Document composition with existing `step.gh_pr_create`, `step.gh_action_trigger`, `step.gh_action_status`, and `step.gh_pr_merge` for repo-specific re-pin automation.

## Non-Goals

- No official Signal account registration, live send/receive, linked-device automation, or Signal service interaction.
- No automatic branch mutation, PR creation, or automerge inside this new step.
- No generic shell execution primitive.
- No non-GitHub release source support in this PR.

## Acceptance

- Unit tests cover update/no-update/error behavior and missing required config.
- Contract tests prove the new step has strict proto descriptors.
- Schema tests prove the manifest advertises the step and its fields.
- `go test ./...` passes with `GOWORK=off GOPRIVATE=github.com/GoCodeAlone/*`.
