### Adversarial Review Report

**Phase:** design
**Artifact:** `docs/plans/2026-07-13-retained-runner-provider-lifecycle-design.md`
**Status:** PASS

**Findings (Important):**

- `D1` [Missing failure modes] `Architecture`: a systemd path event alone did
  not guarantee catch-up after an inactive user session, coalesced event, or
  reboot. Recommendation: add an idempotent boot/periodic reconciliation timer.
  _Resolution: design now requires path plus bounded timer and a missed-event
  runtime proof._
- `D2` [Declared integration proof] `Architecture` / assumption A2: probing from
  inside the provider container did not prove the ephemeral workload container
  could resolve and authenticate the provider across Podman bridge networking.
  Recommendation: probe from a separate workload-shaped container on the same
  network. _Resolution: candidate and stable activation now require the separate
  bridge-container probe._
- `D3` [Rollback story] `Architecture` / `Rollback`: atomic active-state replace
  did not define restart-mid-activation recovery or preservation of the prior
  Podman image. Recommendation: use a crash-durable journal and retain referenced
  current/prior immutable image IDs. _Resolution: design now requires
  prepare/activate/commit journal recovery and image retention._

**Findings (Minor):**

- `D4` [Infrastructure impact] Credential rotation was implicit in reinstall
  but not named. Recommendation: state the rotation transaction and identity/
  state preservation. _Resolution: added to Infrastructure Impact._

**Bug-class scan transcript:**

| Class | Result | Note |
|---|---|---|
| Project-guidance conflicts | Clean | Plugin ownership, Go implementation, secret handling, and real proof follow workspace guidance. |
| Assumptions under attack | Finding | A2/A3 were converted from rollout-time guesses into activation-time probes and timer reconciliation. |
| Repo-precedent conflicts | Clean | Existing provider binary/package and retained user-systemd/Podman patterns are reused without app-owned lifecycle. |
| Artifact-class precedent | Clean | Lifecycle remains a provider release command; scenario code will only orchestrate real STG proof. |
| YAGNI violations | Clean | Generic managed-sidecar framework remains explicitly deferred. |
| Missing failure modes | Finding | D1 and D3 address missed events and interruption during activation. |
| Security / privacy | Clean | GitHub credential remains provider-only; host receives no STG read token; package verification is delegated to compute-agent. |
| Infrastructure impact | Finding | D4 makes credential rotation and persistent user units/images explicit. |
| Multi-component validation | Clean | Release, STG campaign, host service, GitHub API/job, and STG artifacts are all runtime-proven. |
| Declared integration proof | Finding | D2 adds the missing workload-side network/DNS/TLS proof. |
| Contributed UI rendering proof | Clean | No UI contribution is declared. |
| Rollback story | Finding | D3 adds durable recovery and rollback image retention. |
| Simpler alternative | Clean | App-owned script and generic supervisor framework are compared and rejected/deferred with reasons. |
| User-intent drift | Clean | Initial workflow install is separated from autonomous STG-driven updates and manual uninstall. |
| Existence / runtime-validity | Clean | Existing release binary, provider endpoints, compute-agent commands, package markers, systemd, and Podman are named and must be runtime-probed before rollout. |

**Options the author may not have considered:**

1. A generic supervisor sidecar manager would remove systemd-specific provider
   lifecycle, but it expands workflow-compute before a second provider proves a
   reusable contract. Defer with evidence.
2. A timer-only reconciler is simpler than path plus timer, but increases update
   latency. Path plus bounded timer keeps immediate updates and recovery.

**Verdict reasoning:** The initial design had three tangible reliability/proof
gaps. They are resolved in the artifact without expanding provider ownership or
weakening the credential boundary. Remaining platform adapters are explicitly
deferred, so the revised Linux lifecycle design passes.
