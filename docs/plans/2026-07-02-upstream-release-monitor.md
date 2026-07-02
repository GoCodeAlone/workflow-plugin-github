# GitHub Upstream Release Monitor Plan

**Status:** Locked 2026-07-02T21:56:54Z
**Date:** 2026-07-02
**Design:** docs/plans/2026-07-02-upstream-release-monitor-design.md

## Scope Manifest

**PR Count:** 1
**Tasks:** 4
**Estimated Lines of Change:** ~550

**Out of scope:**
- Signal service live traffic, account registration, linked devices, or official app automation.
- A mutating one-step updater that runs repo scripts, opens PRs, or enables automerge.
- Non-GitHub release sources.

**PR Grouping:**

| PR # | Title | Tasks | Branch |
|------|-------|-------|--------|
| 1 | Add upstream release monitor step | Task 1, Task 2, Task 3, Task 4 | feat/upstream-release-monitor |

**Status:** Draft

### Task 1: Record Boundary

Add design and ADR documentation that captures why this belongs in `workflow-plugin-github` as a read-only primitive instead of another Signal-specific package.

Verification: design, plan, and ADR are present and self-consistent.

### Task 2: Add Failing Runtime Tests

Add tests for `step.gh_upstream_release_monitor` covering drift detected, no drift, API failure, and required config validation.

Verification: focused new tests fail before implementation because the step constructor/runtime does not exist.

### Task 3: Implement Step And Contracts

Implement the step, register it in runtime step creation, add proto config/input/output messages, regenerate Go bindings, and update strict contract registry.

Verification: focused runtime and contract tests pass.

### Task 4: Update Manifest, Docs, And Full Verification

Update `plugin.json`, `plugin.contracts.json`, README, and an example Workflow config showing composition with existing GitHub steps. Run full plugin verification.

Verification: `GOWORK=off GOPRIVATE=github.com/GoCodeAlone/* go test ./...` passes.
