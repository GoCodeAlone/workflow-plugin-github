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

Refresh builds a digest-unique scratch image, writes a crash-durable transaction
journal, stops the stable provider to quiesce ownership writes, clones that
authoritative state, starts a candidate, and runs authenticated readiness plus
GitHub preflight from a separate probe container on the DNS-enabled
`wfcompute-github-provider` rootless network. The
installer makes that network the agent's default through a private
`containers.conf`, so generic workload `--network bridge` requests join the same
network without adding GitHub logic to workflow-compute. This proves
container-name DNS and TLS from the workload side of the boundary, not merely
from inside the provider container. Only then does refresh stop the candidate
and stable service, journal the state-promoting boundary, rename the original
provider state to a rollback directory, rename the candidate state into the
stable mount path, and fsync both parents. It then replaces durable active image
state, restarts the stable provider, verifies it from the separate probe
container, and commits. The old state directory is removed only after the
stable probe commits; the previous immutable image remains in durable active
metadata. Candidate or stable activation failure restores the prior state
directory, active record, and service. Startup recovery deterministically
handles prepared, state-promoting, state-promoted, activated, and committed
journals before any new refresh.

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
  user, and symlink-free. The purgeable root is fixed to the dedicated
  `~/.workflow-compute/github-runner-provider` subtree. Generated files use
  atomic replacement and restrictive modes.
- The executing installer binary must hash to the verified promoted artifact;
  direct or stale release binaries cannot establish an unrelated package.
- Package verification is delegated to the compute-agent cryptographic reader;
  this plugin does not duplicate signature-shape checks.
- Provider and agent tokens are rejected when empty or containing line breaks.
  Commands and evidence never include credential values.
- Candidate provider state is a regular-file-only clone. Candidate failure or
  interrupted activation preserves prior active state and service.
- Install, refresh, and uninstall use one lifecycle-wide OS lock held from
  before maintenance mutation through maintenance release. The lock inode is a
  sibling of the purgeable install root so explicit purge cannot unlink it
  while held. Refresh uses a
  crash-durable prepare/state-promote/activate/commit journal. The current and
  immediately previous immutable image IDs are retained; uncommitted provider
  state and managed-file backups are never deleted after a failed restore.
- Rootless Podman runs with a read-only root, dropped capabilities, no-new-
  privileges, explicit state/TLS mounts, and no socket mount. Ephemeral workload
  containers receive only the provider API credential.
- The provider API remains authenticated over a plugin-generated private CA;
  readiness and semantic preflight require the provider token.
- Uninstall retains state and credentials unless explicit purge is requested.

## Infrastructure Impact

- Creates user-owned files below `~/.workflow-compute/github-runner-provider`
  plus a sibling lifecycle lock, and user-systemd units/drop-ins below
  `~/.config/systemd/user`.
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
| A2 | A dedicated rootless bridge with DNS can be the provider network and the agent's default generic bridge. | Install creates and validates `wfcompute-github-provider`; candidate/stable probes fail closed on DNS/TLS, and the agent receives only `CONTAINERS_CONF`. |
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

### Backport 2026-07-13: Canonical Podman Image IDs

Cause: Podman 5.8 `.Id` returned 64 lowercase hex characters, while durable
state validation required `sha256:<hex>`.
Change: accept only exact bare or `sha256:` SHA-256 forms at the Podman boundary;
store and compare the canonical prefixed digest in refresh and `serve-active`.
Scope: no manifest change.
Evidence: rootless Podman runtime install rejected
`d30ca04b79ef9de02c7dffd5f953561b5c437b6314c38810d6e05b9a3f581bf1`
before candidate activation; the stable service later rejected the equivalent
bare ID until the same canonicalizer guarded `serve-active`.

### Backport 2026-07-13: DNS-Enabled Provider Network

Cause: rootless Podman's default `bridge` had `dns_enabled=false`; a separate
workload-shaped probe could not resolve the candidate container.
Change: create and validate `wfcompute-github-provider` as a non-internal DNS
bridge; provider/probe containers join it explicitly; agent-local
`containers.conf` maps generic bridge workloads to it. No ports or sockets.
Scope: no manifest change.
Evidence: default bridge → `no such host`; named bridge → candidate DNS, TLS,
semantic GitHub preflight, stable activation, and marker refresh passed.

### Backport 2026-07-13: Scratch Image Trust Roots

Cause: the scratch provider image had no OS CA pool, so GitHub API preflight
failed with `x509: certificate signed by unknown authority`.
Change: the provider command imports Go's maintained fallback X.509 roots;
system roots remain preferred when available.
Scope: no manifest change.
Evidence: forced-empty-root subprocess failed before import and passed after;
real candidate preflight against `api.github.com` passed from scratch.

### Backport 2026-07-13: Rootless Podman User Units

Cause: `PrivateTmp=true` and, before Podman's pause process existed,
`NoNewPrivileges=true` blocked `newuidmap`; stable activation exited 125.
Change: omit those directives from user services that launch rootless Podman.
Provider and probe containers remain read-only, capability-dropped, and
`no-new-privileges`.
Scope: no manifest change.
Evidence: isolated user-systemd runtime failed namespace setup with each
directive and completed install after their removal.

### Backport 2026-07-13: Path Unit Value Escaping

Cause: `PathChanged="/absolute/path"` preserved the quote as path data; systemd
rejected it as non-absolute and refused the watch unit.
Change: render path-unit values without generic `ExecStart` quoting; encode
unsafe bytes with `\\xNN` and double `%` specifiers.
Scope: no manifest change.
Evidence: systemd journal reported `Path unit lacks path setting`; corrected
unit became active and marker creation invoked the real refresh oneshot.

### Backport 2026-07-14: Lifecycle Transaction Hardening

Cause: adversarial review found that install/uninstall acquired the mutation
lock after entering maintenance, candidate state was probed but never promoted,
failed managed-file restores deleted their backups, and same-digest refresh
reported success without checking the stable service.
Change: hold one lock across the complete maintenance transaction; promote and
recover provider state through explicit journal phases; preserve backups until
all restores succeed and propagate cleanup errors; inspect and probe the stable
provider on idempotent timer/path refreshes. A failed state restore leaves the
provider stopped and the journal intact.
Scope: no manifest change.
Evidence: contention performs no maintenance/agent mutation; simulated crashes
at every journal phase recover the expected state generation; stable-probe and
restore failures recover or fail closed; inactive same-digest service is
rejected.

### Backport 2026-07-14: Observable Systemd Activation

Cause: combined path/timer activation could partially mutate one unit before
returning an error, while the rollback model tracked only the combined call.
Change: activate path and timer independently; on failure inspect exact
`ActiveState` and `UnitFileState`, roll back observed mutations, and remain
conservative when state cannot be inspected. Bound refresh startup to 15
minutes so candidate build/probe/promotion is not killed by systemd's default.
Scope: no manifest change.
Evidence: stateful partial-enable tests leave no enabled watch unit; confirmed
pre-mutation failures do not issue spurious disable operations; timeout unit
rendering is regression-tested.

### Backport 2026-07-14: Cross-Transaction Rollback

Cause: a provider refresh could commit migrated state before later installer
steps restarted and re-observed the retained agent; outer rollback then restored
old image metadata without restoring its matching provider state. Review also
found that cloning before provider quiescence could lose a final ownership
journal write.
Change: stop the stable provider before the authoritative clone; retain a
deferred committed refresh journal during install; let the outer failure path
restore image metadata plus provider state; finalize the state rollback target
only after provider, watch units, and fenced agent restart succeed. Journal
phase variables advance only after the corresponding durable write.
Scope: no manifest change.
Evidence: a quiesce-time write appears in candidate state; failed commit-journal
writes roll back the last durable phase; a post-refresh agent restart failure
restores the prior provider-state generation.

### Backport 2026-07-14: Exact Host-State Restoration

Cause: rollback inferred prior service activation from unit-file existence and
purge unlinked the held lifecycle lock inode.
Change: snapshot `UnitFileState` and `ActiveState` for each pre-existing managed
unit and restore enablement/activity independently; place the lock outside the
purged tree. Recovery stops candidate and stable containers before replacing
mounted state directories.
Scope: no manifest change.
Evidence: disabled units remain disabled after failed reinstall; active units
return active; a contender cannot acquire a replacement lock while purge holds
the original; permission-gated recovery proves process stop precedes filesystem
restore.

### Backport 2026-07-14: Commit Recovery And Durable Cleanup

Cause: adversarial review found that a process exit after a deferred provider
commit required manual journal repair, post-commit backup cleanup errors skipped
maintenance release, runtime-only systemd enablement became persistent, and
nested cloned state directories were not individually fsynced.
Change: a repeated install finalizes a previously probed deferred commit before
replaying the idempotent outer transaction; committed install/uninstall cleanup
always attempts maintenance release; runtime enablement is restored with
`enable --runtime`, while linked/transient activity states that cannot be
reconstructed are rejected before managed-unit mutation; cloned directories are
synced bottom-up through the first existing parent. Purge accepts only the
dedicated provider root.
Scope: no manifest change.
Evidence: process-restart fixtures consume the committed journal and rollback
directory; permission-gated backup cleanup still emits maintenance end; exact
systemd-state and bottom-up sync tests fail under fix reversion and pass after
restoration; shared/custom purge roots are rejected.

### Backport 2026-07-14: Unified Identity-Bound Lifecycle Transaction

Cause: the final code-review loop showed that separate installer and provider
journals still left autonomous-refresh fences unrecoverable, accepted only one
nested provider phase, and recovered using retry-time worker identity.
Change: replace the install-only outer record with one sibling lifecycle
operation journal shared by install, uninstall, autonomous refresh, and a
constrained legacy `refresh_recovery`. The journal separates immutable identity
(`worker_id`, `profile_id`, plugin/component, transaction id) from recovery
transport. Recovery transport records the strict non-secret config plus the
compute-agent executable digest, supervisor-config digest, and agent unit's
loaded fragment path/digest, ordered `DropInPaths` plus digests, effective
`ExecStart`, and relevant environment-file paths/digests. Before stop/start or
maintenance commands, recovery re-attests those regular files and verifies
sanitized local status still names the recorded worker. After this lifecycle
changes the managed drop-in, it captures the expected effective unit signature
after daemon-reload and before start; `ready` recovery accepts only that
journaled post-change signature. Before the first unit-file mutation, the
journal also stores deterministic intended bytes/digests for every managed unit
and drop-in (these contain paths only, no secret values). During `fenced`, each
managed path may independently match its recorded pre-state (including absence)
or intended state; this permits every partial sequential-write vector while
rejecting any third value. Loaded effective state may match only the complete
pre/intended signature because daemon-reload occurs after all writes. Rollback
restores every path to pre-state, runs daemon-reload, and requires the complete
normalized pre-signature. `ready`/`releasing` commit recovery requires every
path to match intended state and accepts loaded pre/intended signatures,
reloading only the exact pre-loaded case before requiring intended. This covers
crashes during sequential writes/restores, before/after daemon-reload, and
post-signature persistence. Retry-time config never becomes recovery authority.

The state machine has seven durable phases:

- `intent`: a canonical owner-only sibling transaction directory, bound to the
  transaction id, is fsynced before any copies or maintenance. No mutation has
  started; recovery removes that directory.
- `adopting`: only `refresh_recovery` uses this phase. It durably binds exactly
  one legacy inner transaction's hash, candidate digest, and verified identity
  before maintenance; recovery may establish/drain the exact fence but cannot
  fabricate or restore a managed-file/systemd baseline.
- `fencing`: exact maintenance begin may be in flight. Recovery idempotently
  establishes and drains that exact fence, performs no provider/managed-file
  mutation, and durably writes `ready{outcome:rollback}` before maintenance end;
  release and transaction-directory cleanup use the forward-only terminal path.
  Once begin is durable and
  sanitized status is lease/task-free, install/uninstall copy snapshots into
  the sibling directory, append each completed `0600` snapshot atomically,
  capture recovery attestations/unit state, and re-read them immediately before
  advancing; a crash during copying remains `fencing` and deletes all copies.
- `fenced`: exact maintenance is durable and sanitized status is unavailable
  with no task/lease. Only this phase permits stopping the recorded agent or
  mutating provider/files/systemd. Recovery re-establishes the exact fence,
  conservatively stops the re-attested agent, rolls back provider/files/units,
  starts and observes that agent, confirms the inner journal is durably absent,
  and writes `ready{outcome:rollback}` before any maintenance end. All release
  then proceeds through forward-only `ready`/`releasing`.
- `ready`: a typed terminal outcome (`commit` or `rollback`) is durable after
  provider/files/units restoration or activation and agent observation while
  fenced. Commit-ready with `provider_effect:changed` requires a matching
  deferred committed inner journal; commit-ready `unchanged` requires an absent
  inner plus bound unchanged provenance; uninstall commit-ready requires
  `not_applicable` and an absent inner. Rollback-ready requires the inner
  transaction durably absent after rollback.
  Recovery inspects maintenance: exact active fences are ended; inactive state
  advances forward; conflicting identity fails closed. It never re-fences,
  stops the agent, or mutates provider state.
- `releasing`: the same typed terminal outcome is retained while exact
  maintenance end may have succeeded. Recovery applies the
  same forward-only rule as `ready` using two sources: maintenance status
  classifies exact-active, inactive, or conflicting identity; only for
  exact-active does sanitized local status classify active task/lease versus
  drained. Exact-active plus task/lease waits boundedly and retains `releasing`
  on timeout; exact-active plus drained ends the exact fence; inactive advances
  without stopping or re-fencing; conflicting identity fails closed. It then
  advances to `committed`. A workload accepted after a successful end is never
  interrupted by recovery.
- `committed`: maintenance is released. Recovery only finalizes deferred
  provider state for the commit outcome, requested purge/preservation,
  snapshots, and journal. Rollback outcome performs no provider finalization,
  ignores any requested purge, preserves the restored pre-state, and removes
  only snapshots/transaction evidence after audit drains.

Install/uninstall snapshot and attest under the drained `fencing` phase and
immediately advance to `fenced`; refresh operations carry no managed-file
baseline. Uninstall has a typed
payload containing `purge`; committed cleanup either preserves provider state
or durably removes the dedicated root and fsyncs its parent before removing the
sibling journal. Transient/linked or otherwise unreconstructable systemd state
is rejected before `fencing`.

The provider subtransaction remains a second file because provider rollback
already has a five-phase durable protocol, but it is no longer an independent
authority. New inner records contain outer transaction id, profile id, and
candidate digest. The outer record also has typed `provider_effect`:
`changed|unchanged|not_applicable`. `changed` is phase-relative: inner may be
absent in `intent`, `fencing`, and pre-provider `fenced`; once provider mutation
starts it requires the matching inner, and commit-ready requires that inner
deferred committed. `unchanged` requires install/refresh, a successful stable-provider
probe, an absent inner journal, and full agreement among outer identity, active
state, and verified candidate for worker id, plugin, component, component id,
and digest; it journals active plus candidate provenance so a new signed
directive reusing identical bytes remains valid. `not_applicable` is required
only for uninstall; uninstall rejects changed/unchanged, while install/refresh
reject not-applicable. This represents same-digest credential rotation without
fabricating a provider transaction. The accepted matrix is closed:

| outer | install/refresh inner | uninstall inner | recovery |
|---|---|---|---|
| `intent`,`fencing` | absent only | absent only | abort before mutation |
| `adopting` | exact hash-bound legacy inner only for `refresh_recovery` | forbidden | establish/drain fence, then advance |
| `fenced` | absent or matching deferred `prepared`/`state_promoting`/`state_promoted`/`activated`/`committed` | absent | roll back |
| `ready`,`releasing` commit+changed | matching deferred `committed` | forbidden | finish forward without re-fence or mutation |
| `ready`,`releasing` commit+unchanged | absent; verified/active/probed digest bound by outer | forbidden | finish forward without re-fence or mutation |
| `ready`,`releasing` commit+not_applicable | forbidden | absent; uninstall only | finish forward without re-fence or mutation |
| `ready`,`releasing` rollback | absent after durable rollback | absent | release forward without re-fence or mutation |
| `committed` commit+changed | matching deferred `committed` or absent after finalized cleanup | forbidden | finalize provider and apply requested preserve/purge |
| `committed` commit+unchanged | absent; bound unchanged evidence | forbidden | apply requested preserve/purge without provider finalization |
| `committed` commit+not_applicable | forbidden | absent; uninstall only | apply requested preserve/purge without provider finalization |
| `committed` rollback | absent | absent | preserve restored root; clean transaction evidence only |

Every other combination, transaction/profile/digest mismatch, or non-deferred
inner record fails closed without touching agent/provider state. Every entry
point recovers the outer journal before update-marker or same-digest decisions.
An orphan legacy inner journal may become only `refresh_recovery`: it fabricates
no file/unit baseline and mutates only the exact agent fence plus provider
transaction. Automatic adoption requires the previously installed strict
config and exact inner candidate worker/plugin/component agreement; new inner
journals also require profile agreement. Missing/invalid installed config or a
legacy identity mismatch requires `retained recover -config <trusted> -confirm
<inner-transaction-id>`, which prints redacted identity, requires exact explicit
confirmation, and applies the same constrained recovery. It never accepts
credentials or releases an unrelated fence.

Security: journals and audit records are strict, owner-only, contain no
credential values, accept only canonical managed paths/unit names, and bind all
nested records. Transaction snapshot directories necessarily contain sensitive
rollback bytes from provider/agent environment files and TLS private material;
they are canonical real `0700` owner-only directories containing only bounded
`0600` regular snapshots, are never named or copied into audit output, and are
retained only until terminal cleanup. Deletion is ordinary filesystem cleanup,
not a secure-erasure claim. Lifecycle intent, each phase transition, recovery
disposition, and terminal error class append redacted JSONL at
`${XDG_STATE_HOME:-$HOME/.local/state}/wfctl/plugins/workflow-plugin-github/retained-provider-audit.jsonl`;
records contain transaction/operation/phase and immutable identity only, never
config contents, paths, credentials, TLS material, or payload bytes. Audit is a
strict tagged union. Common fields are `event_id`, global `sequence`, timestamp,
transaction identity, operation, phase, and `kind`; allowed kind-specific
redacted fields are `outcome`, `provider_effect`, `purge`, `disposition`,
`error_class`, `count`, `first_seen`, and `last_seen`.

Phase/outcome/purge safety events and recovery-disposition/error diagnostics use
two bounded journal lanes merged by global sequence. The safety lane reserves 16
non-droppable immutable slots; a valid operation can emit at most one event for
each of seven forward phases plus terminal outcome/purge, so malformed capacity
is rejected before mutation and release always has reserved space. The diagnostic
lane has 32 keyed phase/kind/class summaries plus one `other` overflow summary;
matching events coalesce count/first/last timestamps. Once an event receives an
append offset its bytes/digest freeze; later matching diagnostics update/create
a tail summary, never the assigned head. Alternating/unrecognized classes count
into the overflow entry rather than consuming safety capacity.

Each queued event has a stable id and exact canonical serialized digest. Under
the dedicated audit lock, only the lowest-sequence head may receive a pre-append
file offset and drain. The append is one canonical newline-terminated record and
the audit file is fsynced before that head is removed. Recovery clears the head
when the complete matching record exists at the offset, truncates a matching
partial final record back to the offset and re-appends, appends when the file
still ends exactly at the offset, and fails closed for a shorter file or
unrelated tail bytes. No later event can overwrite or bypass an unresolved one,
and every required event type uses this path. Audit failure never blocks release
of an established fence or rolls back a ready operation; it does block a new
operation and deletion of terminal journal/transaction evidence until both lanes
drain.

Infrastructure impact: none beyond the existing user-systemd/Podman boundary;
the sibling journal, install lock, audit, and canonical transaction root
`$HOME/.workflow-compute/.workflow-plugin-github-runner-provider-transactions/<transaction-id>`
survive provider-root purge. The directory and parent are owner-only and fsynced.
Committed cleanup may accept a missing transaction directory only after its
operation-specific preservation/purge outcome is already durable; root purge
is followed by parent fsync before snapshot-directory and journal removal.
Multi-component proof: operation-by-phase and legal outer/inner matrix tests
cover changed retry identity, partial snapshot cleanup, same-digest refresh
recovery, constrained legacy adoption/repair, purge intent, exact systemd state,
and retained-agent fence release. Maintenance is an explicit prerequisite from
merged workflow-compute commit `5472767de1e4629ab68337cd6dd1ac85f4b7577e`
(`cmd/compute-agent/main.go:454`,
`internal/agent/process_supervisor.go:1004-1147`, SPEC V971/V973). STG must serve
a signed agent bundle containing that commit before provider rollout. Runtime
proof exercises duplicate exact begin, begin/end/begin/end, status across crash,
wrong-ID rejection, end-to-job-assignment recovery, and post-release reconnect
against that real bundle before repeating the Podman matrix from an attributable
commit.

Load-bearing assumptions: the cited workflow-compute contract guarantees
duplicate exact-id/profile/reason begin is idempotent and rejects conflicts;
maintenance status distinguishes exact active, inactive, and conflicting
identity; systemd start is idempotent; the sibling lock excludes plugin-owned
lifecycle mutation. Self-challenge: partial secret snapshots are owned by a
journaled sibling directory before copy; a crash around begin remains
`fencing`; a crash around end remains forward-only `releasing`; effective unit
attestation covers drop-ins rather than one fragment; persistent audit failure
preserves journal evidence without availability churn; malformed or
unattestable identity fails closed into the explicit recovery command. Scope:
no manifest change; the recovery command is required operational repair for the
locked lifecycle.

## Task 4 Runtime Launch Transcript

Environment: privileged Ubuntu 24.04 arm64 container booted with real user
systemd, lingering user manager, rootless Podman, real provider binary, and real
GitHub API access. The compute-agent maintenance/update/status dependency was a
disclosed deterministic CLI-seam substitute; it is not evidence for the later
real-agent/STG campaign gate. Docker Desktop nested storage required Podman
`vfs`; Ubuntu's CNI backend required the real `dnsname` plugin plus `dnsmasq`.

```text
Build:
$ CGO_ENABLED=0 GOOS=linux GOARCH=arm64 GOWORK=off go build \
    -o /tmp/github-runner-provider-runtime ./cmd/github-runner-provider
exit 0

Install:
$ github-runner-provider retained install -config bootstrap-config.json
installed=true service_active=true version=v1.0.32

Observe:
provider.service: active/enabled
refresh.path: active/enabled
refresh.timer: active/enabled
retained agent service: active/enabled
container: running, read-only, all capabilities dropped,
  no-new-privileges, no published ports, named DNS bridge

Marker refresh:
$ touch provider.json
refresh.service: Result=success ExecMainStatus=0
probe container: created, started, exited successfully, removed

Credential rotation:
$ github-runner-provider retained install -config bootstrap-config.json
provider environment digest changed; active metadata, provider-state sentinel,
worker identity, and retained agent service remained unchanged

Uninstall and reinstall:
$ github-runner-provider retained uninstall -config bootstrap-config.json
installed=false; provider units/container absent; retained agent active;
provider-state sentinel unchanged
$ github-runner-provider retained install -config bootstrap-config.json
installed=true service_active=true; provider-state sentinel unchanged

Failure-signature scrape: clean from the first successful install onward
Verdict: PASS for the Task 4 user-systemd/Podman lifecycle boundary
```
