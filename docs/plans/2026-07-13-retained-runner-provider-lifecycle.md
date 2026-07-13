# Retained GitHub Runner Provider Lifecycle Implementation Plan

> **For the implementing agent:** REQUIRED SUB-SKILL: Use autodev:executing-plans to implement this plan task-by-task.

**Goal:** Ship a plugin-owned retained Linux provider install/refresh/uninstall lifecycle whose updates arrive through signed workflow-compute package campaigns and whose GitHub workloads return canonical proof through STG.

**Architecture:** Extend the existing `github-runner-provider` binary with side-effect-free version/probe commands and retained lifecycle subcommands. A typed Go lifecycle package owns strict local config/state, crash recovery, Podman candidate activation, and user-systemd units; workflow-compute remains responsible only for generic maintenance fencing, cryptographic package verification, dispatch, proof, and artifacts.

**Tech Stack:** Go stdlib, user systemd, rootless Podman, workflow-compute `compute-agent` CLI, GitHub Runner Provider HTTPS API, GoReleaser v2.

**Base branch:** main

---

## Scope Manifest

**PR Count:** 1
**Tasks:** 5
**Estimated Lines of Change:** ~1500

**Out of scope:**
- Generic supervisor-managed sidecar framework.
- macOS launchd or Windows service/tray lifecycle for the Linux-only runner-job payload.
- App-owned workflow-compute provider installer or provider-specific server API.
- Production deployment or destructive production changes.
- Treating GitHub workflow artifacts as canonical workload proof.

**PR Grouping:**

| PR # | Title | Tasks | Branch |
|------|-------|-------|--------|
| 1 | Plugin-owned retained runner-provider lifecycle | Task 1, Task 2, Task 3, Task 4, Task 5 | codex/provider-retained-installer-20260713 |

**Status:** Draft

## Integration Matrix

| integration | classification | task/proof |
|---|---|---|
| provider release binary | runtime-integrated | Task 1 and Task 5 launch `version` and `probe`; archive contains the command |
| compute-agent maintenance/update commands | runtime-integrated | Task 4 fake-command ordering; global plan Task 8 real retained host |
| user systemd and rootless Podman | runtime-integrated | Task 3/4 rendered units and local runtime launch; global plan Task 8 retained Linux apply |
| GitHub org runner API | runtime-integrated | Task 1 semantic preflight against HTTP fake; global plan Task 8 live org preflight/job |
| STG package campaign/task/proof/artifacts | runtime-integrated | global locked dogfood plan Task 8 after plugin release |
| macOS/Windows retained provider lifecycle | deferred | Linux-only runner-job package and explicit design non-goal |

### Task 1: Provider Version And Workload-Side Probe

**Files:**
- Modify: `cmd/github-runner-provider/main.go`
- Modify/Test: `cmd/github-runner-provider/main_test.go`
- Create/Test: `cmd/github-runner-provider/probe.go`
- Modify: `.goreleaser.yaml`
- Modify/Test: `release_packaging_test.go`

**Steps:**
1. Add RED command tests proving `version` succeeds without provider credentials, unknown subcommands fail, legacy address invocation still serves, and `probe` rejects missing token/CA/HTTPS URL or unexpected JSON fields.
2. Add RED HTTP tests with a TLS server proving `probe` authenticates `GET /readyz`, then posts the strict org preflight request and emits a typed redacted result containing readiness, org/group/ref/workflow, runner-group id, resolved SHA, conflict count, and timestamp.
3. Run `GOWORK=off go test ./cmd/github-runner-provider -run 'Version|Probe|Dispatch' -count=1`; expected RED on missing dispatch/probe symbols or behavior.
4. Implement explicit command dispatch. Preserve no-subcommand and `host:port` service compatibility; `version` must print `internal.Version` and perform no environment/config reads.
5. Implement the bounded TLS probe using typed request/result structs, a private CA pool, provider-token bearer auth, strict single-value JSON decoding, response-size limits, and no credential-bearing errors/output.
6. Add the provider binary `-X .../internal.Version={{.Version}}` release ldflag and packaging assertions that the rendered archive command reports the release version.
7. Run focused tests, `GOWORK=off go test ./... -count=1`, `go vet ./...`, Linux/Windows/macOS cross-builds, and launch a built binary with `version`; expected exact non-empty version and exit 0.
8. Rollback: revert Task 1 commit; legacy provider service address invocation remains the prior behavior.
9. Commit: `feat(provider): add versioned readiness probe`.

### Task 2: Strict Retained Lifecycle State And Recovery

**Files:**
- Create: `internal/retainedprovider/config.go`
- Create: `internal/retainedprovider/state.go`
- Create: `internal/retainedprovider/files.go`
- Create/Test: `internal/retainedprovider/state_test.go`
- Create/Test: `internal/retainedprovider/files_test.go`

**Steps:**
1. Add RED tests for versioned typed config, active state, transaction journal, verified-update projection, and redacted status. Reject unknown JSON fields, relative/out-of-home paths, unsafe unit/profile/component identifiers, line-breaking secrets, symlinked ancestors/files, wrong ownership/modes, malformed SHA256/image IDs, and mismatched worker/plugin/component identity.
2. Add RED filesystem tests for atomic mode-0600 writes with file/directory sync, regular-file-only bounded state cloning, install lock exclusivity, current/prior image reference retention, and prepare/activate/commit recovery after interruption at every journal phase.
3. Run `GOWORK=off go test ./internal/retainedprovider -run 'Config|State|Journal|Files|Recovery' -count=1`; expected RED on missing package/types.
4. Implement minimal typed structs and validation. Do not expose `map[string]any`; JSON decoders disallow unknown fields and multiple values.
5. Implement symlink-free user-home path validation, bounded secure copy, atomic durable writes, OS lock abstraction, and idempotent journal recovery that either restores prior active state or completes a verified commit.
6. Run focused tests, `GOWORK=off go test -race ./internal/retainedprovider -count=1`, full tests, `go vet ./...`, and `git diff --check`; expected PASS.
7. Rollback: revert Task 2 commit; no runtime wiring consumes the new package yet.
8. Commit: `feat(provider): add durable retained state`.

### Task 3: Verified Podman Candidate Activation

**Files:**
- Create: `internal/retainedprovider/command.go`
- Create: `internal/retainedprovider/refresh.go`
- Create/Test: `internal/retainedprovider/refresh_test.go`
- Modify: `cmd/github-runner-provider/main.go`
- Create/Test: `cmd/github-runner-provider/retained_test.go`

**Steps:**
1. Add RED tests with a recording command runner for exact `compute-agent supervisor-update verify` arguments and typed output. Reject unverified identity/digest/path, installer self-digest mismatch during first install, and credential values in argv/errors/status.
2. Add RED refresh tests for serialized execution, digest-idempotent no-op, scratch image build with a static `FROM scratch` container file, immutable image-ID capture, candidate state clone, restrictive Podman flags, and separate bridge probe-container invocation.
3. Add RED failure tests for build/candidate/probe/stable failures and cancellation. Assert previous active state/service remains selected, journal recovery is possible, candidate containers are removed, and current/prior image IDs are not pruned.
4. Run `GOWORK=off go test ./internal/retainedprovider ./cmd/github-runner-provider -run 'Verify|Refresh|Candidate|Rollback|Retained' -count=1`; expected RED.
5. Implement bounded command execution without a shell. Parse verified-update JSON into strict typed projection, hash copied bytes, build a digest-unique image, and run candidate/stable probe containers on `--network bridge` with read-only root, dropped capabilities, no-new-privileges, and explicit mounts.
6. Implement prepare/activate/commit journal transitions with directory sync and recovery. Update active state only after candidate preflight; verify stable from a separate container after restart; roll back on failure.
7. Add `retained refresh` command wiring that loads strict config and emits only redacted typed status.
8. Run focused/race/full tests, `go vet ./...`, cross-build all release targets, and `git diff --check`; expected PASS.
9. Rollback: revert Task 3 commit; no service unit invokes refresh before Task 4.
10. Commit: `feat(provider): refresh retained provider safely`.

### Task 4: User-Systemd Install, Status, And Uninstall

**Files:**
- Create: `internal/retainedprovider/systemd.go`
- Create/Test: `internal/retainedprovider/systemd_test.go`
- Modify: `internal/retainedprovider/refresh.go`
- Modify: `cmd/github-runner-provider/main.go`
- Modify/Test: `cmd/github-runner-provider/retained_test.go`

**Steps:**
1. Add RED golden/semantic tests for provider service, refresh service, marker path unit, boot/periodic timer, and retained-agent environment drop-in. Assert absolute escaped paths, no shell, stable launcher path, exact `--network bridge`, restart policy, no public port/socket mount, and no secret literals in units.
2. Add RED transaction tests proving install/reinstall ordering: preflight -> package verify/self-hash -> maintenance begin -> local status unavailable with empty task/lease -> stop agent -> durable files/units -> daemon-reload -> provider activation/probe -> start same agent -> status unavailable under same maintenance ID -> maintenance end -> idle/online observation.
3. Add RED tests that maintenance remains active on incomplete rollback, exact maintenance ID is required, transient local status is bounded, no STG token/API call is used, and credential rotation preserves worker/provider state.
4. Add RED uninstall tests proving a separate invocation fences the worker, disables/removes provider path/timer/services and agent drop-in, restarts the same retained agent, and preserves state/secrets unless `--purge` is explicit.
5. Run `GOWORK=off go test ./internal/retainedprovider ./cmd/github-runner-provider -run 'Systemd|Install|Reinstall|Status|Uninstall|Maintenance' -count=1`; expected RED.
6. Implement unit rendering and the install/reinstall/status/uninstall state machines with bounded systemctl/compute-agent calls, local status parsing, atomic files, rollback, and redacted evidence.
7. Wire `retained install|refresh|serve-active|status|uninstall`. Return an explicit unsupported-platform error outside Linux without breaking service/version/probe commands.
8. Run focused/race/full tests, `go vet ./...`, cross-build Linux/darwin/windows amd64/arm64, and `git diff --check`; expected PASS.
9. Runtime launch validation: in an isolated Linux user-systemd/Podman environment, render/install units against fake compute-agent/provider endpoints, fire path and timer events, observe refresh invocation, run status, then uninstall. Expected no leaked secrets, provider ready, and original agent unit active after uninstall.
10. Rollback: run `retained uninstall` without purge or revert Task 4 commit; restore the prior agent drop-in backup and restart the unchanged worker unit.
11. Commit: `feat(provider): install retained provider service`.

### Task 5: Release Contract And Global Dogfood Handoff

**Files:**
- Modify: `README.md`
- Modify: `.goreleaser.yaml`
- Modify/Test: `release_packaging_test.go`
- Modify: `docs/plans/2026-07-13-retained-runner-provider-lifecycle-design.md`
- Create: `docs/plans/2026-07-13-retained-runner-provider-lifecycle-plan-review.md`

**Steps:**
1. Add RED release tests requiring the versioned provider binary and retained lifecycle metadata/docs while keeping `github-actions-runner-job` Linux-only and existing archives compatible.
2. Document initial install/reinstall, autonomous STG campaign refresh, status, credential rotation, and separate uninstall. State that GitHub workflow output is orchestration evidence only.
3. Run `GOWORK=off go test ./... -count=1`, `GOWORK=off go test -race ./internal/retainedprovider ./cmd/github-runner-provider -count=1`, `go vet ./...`, `goreleaser release --snapshot --clean`, archive extraction plus `github-runner-provider version`, all release-target cross-builds, and `git diff --check`; expected PASS.
4. Run adversarial self code review over the complete diff. Scan secret flow, argv/env leakage, symlink/ownership checks, crash points, lock release, rollback, systemd escaping, Podman isolation/networking, unsupported OS behavior, and legacy CLI compatibility; fix every Critical/Important finding.
5. Open the plugin PR, request Copilot, monitor checks/threads until green, and admin-merge only with no unresolved findings. Verify merged commit equals intended release tag before tagging.
6. Publish the next plugin release and verify non-draft release assets, checksums, provider `version`, Linux runner-job image, and registry notification.
7. Hand off to Task 8 of the global locked plan: publish the executable/probe-capable provider package to STG; run one manual retained Linux install; dispatch a real GitHub ephemeral job from STG and validate proof/log/artifact refs through STG; publish a second package version or campaign, restart agent/user manager, and prove autonomous catch-up without another install workflow.
8. Keep uninstall as a separate manual workflow and do not run it until retained update/reconnect evidence is complete.
9. Rollback: pin/re-promote the previous plugin release, or invoke retained uninstall without purge; revert dogfood runner labels if STG proof fails.
10. Commit: `docs: document retained provider lifecycle`.
