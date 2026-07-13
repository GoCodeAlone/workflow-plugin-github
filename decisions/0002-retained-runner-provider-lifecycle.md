# 0002. Keep retained runner-provider lifecycle plugin-owned

**Status:** Accepted
**Date:** 2026-07-13
**Related:** `docs/plans/2026-07-13-retained-runner-provider-lifecycle-design.md`

## Context

The GitHub runner provider needs a long-lived credential-bearing local service,
but retained-agent updates must not depend on a workflow-compute GitHub Actions
installer. Moving only the provider binary into this plugin would leave service
installation, update, and rollback behavior app-owned.

## Decision

Ship the retained Linux lifecycle adapter as subcommands of the existing
`github-runner-provider` release binary. One manual user-scope install creates
systemd/Podman wiring. Subsequent provider updates are driven by signed
workflow-compute package markers and cryptographically verified with the
provider-neutral compute-agent command before plugin-owned candidate preflight
and activation.

Do not place GitHub API logic, provider credentials, provider service units, or
provider update orchestration in workflow-compute. Do not put STG API tokens on
the retained host for lifecycle observation.

## Consequences

- Provider lifecycle and rollback release with the provider implementation.
- Workflow-compute remains responsible only for generic package delivery,
  maintenance fencing, provenance verification, dispatch, proof, and artifacts.
- Linux user-systemd/Podman is the first adapter. A generic managed-sidecar
  contract remains deferred until another provider supplies evidence that the
  lifecycle is reusable rather than merely similar.
- The stable launcher must preserve backward compatibility with versioned local
  lifecycle config/state across provider updates.
