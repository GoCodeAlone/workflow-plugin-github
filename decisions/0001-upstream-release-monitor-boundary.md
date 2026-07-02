# 0001. Keep Release Drift Read-Only

**Status:** Accepted
**Date:** 2026-07-02
**Decision-makers:** Workflow maintainers
**Related:** docs/plans/2026-07-02-upstream-release-monitor-design.md

## Context

Signal-related repos now repeat the same upstream-release drift pattern, but each repo has different update scripts, compatibility tests, branch rules, and release gates. A Signal-specific updater would duplicate GitHub plumbing, while a fully mutating generic updater would need to execute repo-specific code and own automerge policy.

## Decision

Add a read-only GitHub release monitor step. It reports whether an upstream latest release differs from a pinned tag. Repository mutation remains composed from existing Workflow primitives and repo-owned scripts.

Rejected: a Signal-only monitor because the pattern applies beyond Signal. Rejected: a one-step automerge updater because it would hide repo-specific update and safety policy inside a generic plugin.

## Consequences

Workflow apps can build reusable upstream drift pipelines without hard-coding Signal semantics. Repos still own their update commands, tests, and automerge criteria. A later non-GitHub provider can add a sibling primitive without changing this boundary.
