### Adversarial Review Report

**Phase:** plan
**Artifact:** `docs/plans/2026-07-13-retained-runner-provider-lifecycle.md`
**Status:** PASS

**Findings (Important):**

- `P1` [Missing failure modes] Task 3 named `serve-active` only in final CLI
  wiring and did not test immutable image validation or foreground process
  tracking. Recommendation: add RED tests and implementation steps for image-ID
  inspect/match plus shell-free foreground exec. _Resolution: added to Task 3._
- `P2` [Security / missing integration proof] Task 3's separate probe container
  did not constrain its environment, so an implementation could accidentally
  inherit the GitHub credential. Recommendation: require a provider-token-only
  probe env file and assert absence of GitHub credentials. _Resolution: added to
  Task 3._
- `P3` [Missing rollback wiring] Task 4 omitted executable TLS and environment
  file tests even though those files define the provider/workload credential
  boundary and rollback inputs. Recommendation: add CA/SAN, mode, syntax,
  separation, and transaction-order tests. _Resolution: added to Task 4._

**Findings (Minor):**

- `P4` [Verification-class mismatch] Go change verification named tests/vet but
  omitted the workspace-required golangci gate. _Resolution: Task 5 now runs
  `golangci-lint --new-from-rev` or records/uses the repository CI equivalent if
  no pinned tool exists._

**Bug-class scan transcript:**

| Class | Result | Note |
|---|---|---|
| Project-guidance conflicts | Clean | Plugin ownership, Go, no STG host token, redaction, and real dogfood proof are mapped to tasks. |
| Assumptions under attack | Clean | Path observation, bridge DNS, user systemd, and state shape all have fail-closed tests/proofs. |
| Repo-precedent conflicts | Clean | Existing provider binary, GoReleaser, user-systemd, Podman, and compute-agent commands are reused. |
| Artifact-class precedent | Clean | Provider lifecycle ships in provider release; scenario remains orchestration only. |
| YAGNI violations | Clean | Generic sidecar framework and non-Linux adapters remain out of scope. |
| Missing failure modes | Finding | P1 added active-service validation and foreground lifecycle coverage. |
| Security / privacy | Finding | P2/P3 make credential separation and TLS material executable gates. |
| Infrastructure impact | Clean | Units/images/files are rendered and safe-host applied; production is excluded. |
| Multi-component validation | Clean | Task 5 hands off to real STG campaign/job/proof/artifact validation. |
| Declared integration proof | Finding | P2 completes workload-side probe environment validation. |
| Contributed UI rendering proof | Clean | No UI contribution. |
| Rollback story | Finding | P3 wires durable secret/TLS assets into install rollback tests. |
| Simpler alternative | Clean | Design rejects app script and defers generic supervisor framework. |
| User-intent drift | Clean | One install, autonomous updates, and separate uninstall are explicit. |
| Existence / runtime-validity | Clean | Real commands/endpoints/artifacts are launched or cross-built before release. |
| Over/under-decomposition | Clean | Five cohesive TDD tasks fit one provider PR; global STG proof remains in locked Task 8. |
| Verification-class mismatch | Finding | P4 adds the missing lint/static-analysis gate. |
| Auth/authz chain composition | Clean | Provider bearer/TLS and compute-agent local verification are server/crypto enforced, not client-asserted payload claims. |
| Hidden serial dependencies | Clean | Tasks are deliberately sequential and touch shared command/lifecycle files. |
| Missing rollback wiring | Clean | Each runtime-affecting task includes an explicit rollback action. |
| Missing integration proof | Clean | Local launched runtime plus post-release retained STG proof are required. |
| Missing declared integration matrix | Clean | Every integration is runtime-integrated or explicitly deferred. |
| Missing contributed UI route proof | Clean | Not applicable. |
| Infrastructure verification mismatch | Clean | Unit rendering, safe-host apply, path/timer events, status, and uninstall are required. |
| Plugin-loader runtime layout | Clean | This release command is archive-contained, not a host-discovered plugin child. |
| Config-validation schema rules | Clean | Typed lifecycle JSON and unit semantics have strict tests. |
| Identifier/naming match | Clean | Existing `GITHUB_RUNNER_PROVIDER_*`, `COMPUTE_*`, and command naming are preserved. |
| Planned-code compile-validity | Clean | Plan embeds no pseudo-implementation that could fail compilation. |

**Options the author may not have considered:**

1. A single install shell script is shorter, but repeats the rejected untyped,
   app-owned lifecycle and is unsuitable for crash recovery.
2. A supervisor-native sidecar manager could replace systemd, but belongs in a
   future cross-provider design after a second concrete consumer exists.

**Verdict reasoning:** The initial plan omitted three load-bearing executable
gates. The revised tasks now cover active-image process semantics, probe secret
isolation, TLS/env boundaries, and the Go static-analysis gate. No unresolved
Critical or Important findings remain.
