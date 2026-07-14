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

## Implementation Adversarial Review

| round | verdict | findings | resolution/evidence |
|---|---|---|---|
| 1 | REQUEST-CHANGES | rootless/linger docs; maintenance transaction IDs; local-status freshness; redirect handling; diagnostic audit durability; TLS renewal/current validity | hardened preflight, transaction-scoped maintenance, fresh status timestamps, redirect rejection, durable diagnostics, TLS renewal/validity tests; package tests PASS |
| 2 | REQUEST-CHANGES | reinstall config binding; TLS recovery restart/probe; SELinux labels; unbounded package/image retention; config/probe mismatch | exact installed-config guard on install/refresh/uninstall; fenced TLS probe; `Z`/`z`; current+previous GC; shared config/probe limits; focused + package tests PASS |
| 3 | REQUEST-CHANGES | committed cleanup ordered before journal removal; audit replay accepted truncation; active runtime provenance unbound; labels unbounded; schema not executed | committed cleanup recovery; durable offset checks; config-bound active/journal state; 64 labels; compiled schema/example/negative tests; focused + package tests PASS |
| 4 | REQUEST-CHANGES | nested deferred recovery not config-bound; audit path ambient; partial cleanup crash; audit tagged union loose; custom Podman timeout mismatch; host-specific test commands | config-bound reads before mutation; home-derived audit path; partial cleanup tolerance only after commit; strict queue-shaped variants; exact `podman` basename; current test-binary fixtures; focused + package tests PASS |
| 4 focused | REQUEST-CHANGES | proposed adding `ProfileID` to active artifact state | rejected: active artifact authorization identity is worker/plugin/component; profile is local outer transaction/supervisor routing and remains bound in config+journals. Adding it to provider artifact state would change protocol without closing an authorization gap. |
| 5 | REVERT-AND-REWRITE | failed updates leaked candidate packages/images; new directory entries lacked parent fsync; recovery-overflow conversion retained illegal disposition; credential scanner and root Podman schema mismatched | confirmed. Prior post-build transaction approach rejected. Rewritten with pre-mutation staging ownership, incremental parent-sync directory creation, fresh overflow construction, 32 KiB credential bound, and schema/runtime path parity. |
| post-rewrite 1 | REQUEST-CHANGES | fenced rollback removed snapshot authority before transition; provider-state rollback/promotion lacked replayable rename phases; config paths could alias managed state; candidate cleanup lacked ownership proof; refresh timeout was short | retained snapshots until terminal cleanup; added detached + rollback phases with parent fsync; enforced path separation; labeled/ID-bound cleanup; rendered 45-minute aggregate timeout; focused regressions PASS. |
| post-rewrite 2 | REQUEST-CHANGES | rollback timeout shorter than probes; incomplete authority overlap checks; aggregate timeout omitted worst-case loops; stable/probe names lacked ownership recovery; image cleanup used mutable tags | computed rollback/refresh budgets including probe cleanup; complete ancestor/descendant authority isolation; role-bound ownership and cancellation-safe stale cleanup for all fixed container names; labeled image inventory plus immutable-ID deletion; focused and package tests PASS. |
| post-rewrite 3 | REQUEST-CHANGES | lifecycle failure recovery had a 30-second aggregate deadline; Podman/systemctl/loginctl lacked complete path and recovery authority; prepared/active images still depended on mutable tags; retained provider env could broaden GitHub scopes | computed lifecycle/wiring recovery deadlines; explicit configured host tools with permission/content attestations; immutable-ID or complete-label image inventory; exact singleton GitHub authority and unbound API-base rejection; focused revert/restore proofs and retained package tests PASS. |
| post-rewrite 4 | REQUEST-CHANGES | systemd omitted initial/failure recovery from its service deadline; detached probe cleanup and aggregate arithmetic allowed only half the required commands; same-digest runtime loss had no rebuild path | composed start/stop service deadlines; two-command detached cleanup and four-command-per-attempt aggregate; explicit fenced same-digest repair with owned-image/package-safe rollback; focused revert/restore proofs and retained package tests PASS. |
| post-rewrite 5 | REVERT-AND-REWRITE | same-digest install could select unchanged without runtime integrity; image-loss race could mutate outside maintenance; rendered systemd timeouts truncated fractional seconds; managed path creation trusted home/ancestors without full authority validation | outer/inner digest+effect agreement with fenced restart; ceiling-rounded directives; home and nearest-existing-ancestor owner/writability validation; focused revert/restore proofs and retained package tests PASS. |
| post-rewrite 6 | REQUEST-CHANGES | packaged provider used vulnerable Go TLS and `x/net/idna` paths plus a fixed Kinesis decoder panic; path comment omitted writability; effect guard obscured precedence | Go 1.26.5, `x/net` 0.55.0, `x/sys` 0.45.0, Kinesis 1.43.5; comment and guard clarified; scoped vulnerability and focused tests rerun. |
| post-rewrite 7 | SHIP-IT | no Critical/Important; five inherited Docker advisories have no fixed release and affected archive/copy/AuthZ APIs are not called by this provider path | full scope/checklist pass; residual SDK linkage recorded for later dependency-light extraction; final verification gate required before PR. |
| Copilot 1 | REQUEST-CHANGES | runtime and schema allowed managed container names that cannot serve as TLS DNS SANs/hosts; derived probe labels could exceed 63 bytes | shared lowercase DNS-label invariant, 57-byte base cap, provider URL parity, and RED/GREEN/revert/restore runtime+schema proofs. |
| Copilot 2 | REQUEST-CHANGES | retained filesystem paths and probe CA path accepted control, DEL, padded, or non-canonical values before systemd/process/filesystem use | shared canonical safe absolute-path predicate plus RED/GREEN/revert/restore proof across every retained path and probe CA variants. |
| post-Copilot 2 | SHIP-IT | no Critical/Important after scope-compliance and full bug-class scan; predicate is symmetric across retained config and probe CA boundaries, errors fail closed, tests are hermetic/non-vacuous, and target-specific path semantics compile per OS | full race/static/six-target/snapshot/runtime gates rerun on final code; CI and Copilot re-review still required after push. |
| Copilot 3 | REQUEST-CHANGES | unknown subcommands echoed raw positional input into logs; probe response-read failures swallowed their causal I/O error | constant unknown-command diagnostic and `%w` response-read wrapping with token-shaped and hermetic failing-body RED/GREEN/revert/restore tests. |
| post-Copilot 3 | SHIP-IT | no Critical/Important after scope-compliance and full bug-class scan; unknown input cannot enter diagnostics, read causes propagate without response content, and both tests exercise real hermetic failure paths | full race/static/six-target/snapshot/runtime gates rerun; CI and Copilot re-review required after push. |
| Copilot 4 | REQUEST-CHANGES | retained unknown subcommands still echoed raw input; lock creation could follow a final-component symlink raced between `Lstat` and `OpenFile(O_CREATE)` | constant retained diagnostic; shared Unix/Windows no-follow opener wired to lock and sibling lifecycle-audit creation; deterministic raced-symlink and RED/GREEN/revert/restore proofs plus Windows cross-compile. |
| post-Copilot 4 | SHIP-IT | no Critical/Important after scope-compliance and full bug-class scan; Unix rejects final symlinks, Windows opens/rejects reparse points through existing post-open checks, unsupported platforms fail closed, and no content mutates before validation | full race/static/seven-target/snapshot/runtime gates rerun; Windows CI journal and Copilot re-review required after push. |

Round 5 rejected the prior mechanism. The affected durability/recovery layer was
rewritten rather than advanced. A new post-rewrite review cycle must reach
`SHIP-IT` before PR creation.
