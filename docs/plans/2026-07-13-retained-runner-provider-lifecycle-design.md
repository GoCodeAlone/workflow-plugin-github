# Retained GitHub Runner Provider Lifecycle Design

**Status:** Approved as a fix-forward refinement of the workspace GitHub Provider dogfood design
**Date:** 2026-07-13
**Project:** Workflow-Compute slimming closure
**Related:** workspace `docs/plans/2026-06-26-github-provider-dogfood-agents-design.md`, Task 4 and Task 8 of its implementation plan

## Problem

The GitHub runner provider keeps its GitHub credential outside ephemeral job
containers, so a retained agent needs a local provider service. The discarded
workflow-compute implementation installed and refreshed that service from a
repository-owned GitHub workflow. That shape made provider updates depend on
GitHub Actions, left provider lifecycle in the private app repo, and required a
static STG read token on the host.

The retained Linux proof needs one user-scope install, autonomous updates from
signed workflow-compute package campaigns, candidate validation, rollback, and
an independently invokable uninstall path.

## Global Design Guidance

Source: workspace `docs/design-guidance.md`.

| guidance | design response |
|---|---|
| Workflow/plugin ecosystem is the substrate | GitHub-specific lifecycle ships in this plugin; workflow-compute supplies only generic signed package delivery and maintenance/provenance commands. |
| Reuse over rebuild | Reuse `github-runner-provider`, retained supervisor packages, user systemd, and Podman. |
| Primary language Go; strict boundaries | Lifecycle state/config/evidence use typed Go structs and strict decoders; no shell/JQ state machine or new `map[string]any` boundary. |
| Secrets never logged | Credentials enter through environment, are written only to mode-0600 files, and are excluded from status/evidence. |
| Multi-component validation | Release package, retained host, systemd/Podman, STG campaign, provider API, GitHub runner job, and STG proof/artifact APIs are all exercised. |

## Approaches

| approach | trade-off | decision |
|---|---|---|
| App-owned GitHub workflow installer | Existing code is available, but updates depend on GitHub and provider logic remains in workflow-compute. | Rejected. |
| Plugin-owned retained lifecycle with marker-triggered refresh | Smallest provider-owned path; one install, then server-mediated updates; Linux/systemd-specific adapter is explicit. | Selected. |
| Generic supervisor-managed sidecar framework | Strong long-term reuse, but expands core contracts/process supervision before one real provider proves the lifecycle requirements. | Deferred until a second provider needs the same mechanism. |

## Architecture

`github-runner-provider` gains commands that do not require provider service
environment parsing:

- `version`: side-effect-free package probe.
- `probe`: authenticated TLS readiness and semantic GitHub preflight from
  inside a candidate/stable provider container.
- `retained install`: one-time user-scope install/reinstall transaction.
- `retained refresh`: verify the current signed provider package, stage a
  candidate container, preflight it, atomically activate it, and restart only
  the provider service.
- `retained serve-active`: validate durable active state and exec Podman for
  the selected immutable image ID.
- `retained status`: emit redacted machine-readable service/package state.
- `retained uninstall`: remove user-scope provider wiring under the same
  maintenance fence; purge of provider state is explicit.

The install transaction copies the reviewed provider binary to a stable
launcher path, writes provider and agent environment files, creates TLS
material, writes user-systemd provider/refresh/path units plus the retained
agent drop-in, activates the current signed package, and restarts the agent.
The installer uses `compute-agent supervisor-maintenance` and the local agent
status file; it never reads STG leases and receives no STG API token.

The path unit watches the exact supervisor current-package marker. A marker
change runs `retained refresh`. A bounded user timer runs the same idempotent
refresh at boot and periodically, so a coalesced path event, disabled user
session, or server/agent restart still catches up. Refresh is serialized by a
user-owned OS lock and uses `compute-agent supervisor-update verify` to
cryptographically bind worker, directive, artifact, path, and digest before
copying bytes.

Refresh writes a crash-durable transaction journal before mutation, builds a
digest-unique scratch image, starts a candidate with cloned provider state, and
runs authenticated readiness plus GitHub preflight from a separate probe
container on the same `--network bridge` used by provider workloads. This proves
container-name DNS and TLS from the workload side of the boundary, not merely
from inside the provider container. Only then does refresh fsync and atomically
replace durable active state, restart the stable provider, verify it again from
the separate probe container, mark the journal committed, and retain the prior
active image/state as rollback material. Candidate or stable activation failure
restores the prior active record and service. Startup recovery finishes or
rolls back an interrupted journal before any new refresh.

The agent receives only provider URL, provider API token, and CA certificate.
The GitHub token remains in the provider container environment and is never
forwarded to the ephemeral runner-job container.

## Integration Matrix

| integration | classification | proof |
|---|---|---|
| `github-runner-provider` release binary | runtime-integrated | release archive runs `version`; digest equals promoted STG artifact. |
| `compute-agent` maintenance/update verification | runtime-integrated | real retained install verifies the current marker and drains/restarts the same worker identity. |
| user systemd + rootless Podman | runtime-integrated, Linux | service/path/refresh units active; candidate and stable authenticated probes pass. |
| GitHub org runner API | runtime-integrated | semantic preflight and one ephemeral job lifecycle succeed. |
| STG package campaign | runtime-integrated | a later plugin version promotes and refreshes without a GitHub install workflow. |
| STG task/proof/artifact APIs | runtime-integrated | accepted provider task and canonical artifact refs are retrieved from STG. |
| macOS/Windows retained provider lifecycle | deferred | runner-job payload is currently Linux-only; this slice proves retained Linux before adding launchd/Windows adapters. |

## Security Review

- Install paths must be absolute, under the invoking user's home, owned by that
  user, and symlink-free. Generated files use atomic replacement and restrictive
  modes.
- The executing installer binary must hash to the verified promoted artifact;
  direct or stale release binaries cannot establish an unrelated package.
- Package verification is delegated to the compute-agent cryptographic reader;
  this plugin does not duplicate signature-shape checks.
- Provider and agent tokens are rejected when empty or containing line breaks.
  Commands and evidence never include credential values.
- Candidate provider state is a regular-file-only clone. Candidate failure or
  interrupted activation preserves prior active state and service.
- Refresh uses a single-owner lock and crash-durable prepare/activate/commit
  journal. The current and immediately previous immutable image IDs are retained;
  cleanup never removes rollback material referenced by active recovery state.
- Rootless Podman runs with a read-only root, dropped capabilities, no-new-
  privileges, explicit state/TLS mounts, and no socket mount. Ephemeral workload
  containers receive only the provider API credential.
- The provider API remains authenticated over a plugin-generated private CA;
  readiness and semantic preflight require the provider token.
- Uninstall retains state and credentials unless explicit purge is requested.

## Infrastructure Impact

- Creates user-owned files below `~/.workflow-compute/github-runner-provider`
  and user-systemd units/drop-ins below `~/.config/systemd/user`.
- Builds local rootless Podman images and runs one provider container.
- Adds no cloud resources, database migrations, public ports, or production
  deployment. The first runtime proof is STG only.
- Initial install needs a self-hosted workflow on the retained Linux host. Once
  installed, package refreshes are triggered by STG campaigns and local marker
  observation, not GitHub workflows.
- Credential rotation is an idempotent reinstall operation. It rewrites secret
  files under maintenance, re-preflights provider/agent wiring, and preserves
  worker identity and provider journal state.

## Multi-Component Validation

1. Unit tests use fake command execution and filesystem roots to prove strict
   config, transaction ordering, secret exclusion, candidate failure rollback,
   crash-journal recovery, path+timer unit content, status, and uninstall
   behavior.
2. Build Linux amd64/arm64 provider binaries and run `version` without provider
   credentials.
3. Launch a real local provider process with TLS and an HTTP fake for the GitHub
   boundary; run the real `probe` command.
4. Release the plugin and publish an executable, probe-capable provider package
   through STG.
5. Run one retained Linux install workflow, then publish a second package
   campaign and prove path/timer refresh plus reconnect occurs without another
   install workflow. Restart the user service manager or retained agent between
   promotion and observation to prove catch-up after a missed immediate event.
6. Dispatch the real ephemeral GitHub runner workload from STG. Validate worker,
   proof, logs, and artifact refs through STG; GitHub output alone is not proof.
7. Exercise the separate manual uninstall workflow only after update/reconnect
   evidence is retained.

## Assumptions

| id | assumption | failure response |
|---|---|---|
| A1 | Retained Linux runs user systemd with lingering and rootless Podman. | Install preflight fails before mutation and emits a redacted diagnostic. |
| A2 | Podman `--network bridge` provides name resolution between provider and workload-shaped containers. | Every candidate/stable activation runs the real probe from a separate bridge container and fails closed before rollout if name resolution or TLS fails. |
| A3 | Current-package marker replacement is observable by a systemd path unit. | A bounded boot/periodic timer reconciles current versus active digest even when path observation is missed. |
| A4 | Existing provider state consists only of regular files/directories. | Refresh rejects unsupported entries and preserves the active service. |
| A5 | Provider launcher schema remains backward-compatible across plugin updates. | Version the lifecycle config/state and fail closed before activation. |

## Self-Challenge

1. A generic managed-sidecar framework may eventually be cleaner. It is not
   justified until another provider demonstrates identical lifecycle needs.
2. A path unit can miss or coalesce events. The timer is required, refresh is
   idempotent, and status compares active/current digests; STG rollout proof must
   show actual version transition, not merely an active unit.
3. Same-user host compromise can read provider files despite containerization.
   The security boundary is untrusted workload versus trusted retained agent;
   hardware-backed host isolation is explicitly outside this slimming slice.

## Rollback

- Candidate failure retains the previous active image/state and running service.
  Startup recovery consumes the fsynced transaction journal after interruption;
  the current and prior immutable image IDs are never removed while referenced.
- A bad accepted release can be rolled back by a signed STG campaign to a prior
  plugin version; the same refresh path preflights it before activation.
- `retained uninstall` removes provider units and the agent drop-in while
  preserving worker identity and, by default, provider state.
- The dogfood workflow can return to the existing non-provider runner labels;
  no production deployment is part of this design.
