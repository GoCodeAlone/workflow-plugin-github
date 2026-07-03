# Retro: Upstream Release Monitor

**PR:** #45 — Add upstream release monitor step
**Merged:** 2026-07-02
**Branch:** `feat/upstream-release-monitor`
**Design:** docs/plans/2026-07-02-upstream-release-monitor-design.md
**Plan:** docs/plans/2026-07-02-upstream-release-monitor.md
**Related ADRs:** decisions/0001-upstream-release-monitor-boundary.md

## Adversarial-review findings, scored

No committed adversarial-review reports were present for this feature. Findings are reconstructed from downstream review and CI evidence only.

| Phase | Finding | Severity | Outcome |
|---|---|---|---|
| design | No committed design review report | n/a | Inconclusive |
| plan | No committed plan review report | n/a | Inconclusive |

## Gate misses

| Issue | Gate that missed | Why it slipped | Fix idea |
|---|---|---|---|
| GitHub release lookup had no default timeout when the pipeline context lacked a deadline | requesting-code-review | Local review checked boundary wiring but missed network-stall behavior; Copilot caught it post-PR. | Include network deadline/timeout in the standard checklist for new API clients. |
| Plan had duplicate contradictory status fields (`Locked` top-level, `Draft` in Scope Manifest) | scope-lock / alignment-check | The lock helper hashed the manifest but did not flag the stale manifest status line. | Scope checks should reject a manifest `Status: Draft` when a lock sidecar exists. |

No CI failures occurred. PR checks were green before merge and after the review-fix commit.

## Missed skill activations

The activation log was unavailable at `.claude/autodev-state/in-progress.jsonl` in the canonical repository root, so this table records observed artifacts rather than hook logs.

| Gate | Fired? | Notes |
|---|---|---|
| brainstorming | yes | Design boundary was derived before implementation. |
| adversarial-design-review (design) | unverified | No committed report. |
| writing-plans | yes | Locked plan exists. |
| adversarial-design-review (plan) | unverified | No committed report. |
| alignment-check | yes | Scope manifest check passed before lock. |
| scope-lock | yes | Lock sidecar existed during execution, was regenerated after review fix, and was removed after the plan was marked complete. |
| subagent-driven-development | no | Main thread executed implementation directly. |
| finishing Step 1e (doc-reconciliation) | unverified | PR touched docs/examples; no explicit `Doc-reconciliation:` line was present in the PR body. |
| pr-monitoring | yes | CI and Copilot threads were monitored, fixed, resolved, and rechecked. |

## What worked

- The read-only boundary held: the new step reports drift but leaves repo mutation to existing Workflow primitives and target-repo scripts.
- Strict contract tests caught manifest/proto/runtime sync requirements before PR creation.
- Copilot review found two actionable issues before merge, and both fixes were small and verifiable.
- Release automation produced `v1.0.25`, published assets, pushed the runner-job image, and dispatched registry sync.

## What didn't

- The local pre-PR review missed the API timeout edge case.
- The lock process allowed an internally contradictory plan status until external review caught it.
- The retro could not use an activation log, so skill-firing evidence is incomplete.

## Plugin-level follow-ups

- Consider adding a scope-lock check that rejects `Status: Draft` inside a manifest when a `.scope-lock` file exists.
- Consider making default network timeouts an explicit review checklist item for new GitHub API clients.

## Project guidance updates

| Guidance file | Change | Reason |
|---|---|---|
| `(none)` | no change | No durable project-wide guidance file exists, and the lessons are gate/plugin-process follow-ups rather than GitHub-plugin design constraints. |
