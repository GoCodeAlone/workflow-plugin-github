### Alignment Report

**Status:** PASS

**Coverage:**

| Design requirement | Plan task(s) | Status |
|---|---|---|
| Version/probe/install/refresh/serve/status/uninstall command surface | Tasks 1, 3, 4 | Covered |
| Preserve legacy provider service invocation | Tasks 1, 5 | Covered |
| Typed strict state/config; no new untyped boundary | Task 2 | Covered |
| No STG token on host; local maintenance/status observation | Task 4 | Covered |
| Cryptographically verified package and installer identity | Tasks 2, 3 | Covered |
| Path plus boot/periodic reconciliation | Tasks 2, 4 | Covered |
| Crash-durable activation journal and prior-image rollback | Tasks 2, 3 | Covered |
| Separate workload-side bridge DNS/TLS/API probe | Tasks 1, 3 | Covered |
| GitHub credential isolated from agent/probe/workload | Tasks 1, 3, 4 | Covered |
| User-systemd/rootless Podman hardening | Tasks 3, 4 | Covered |
| Reinstall credential rotation and separate uninstall | Task 4 | Covered |
| Linux-only first adapter; other OS adapters deferred | Tasks 4, 5 | Covered |
| Real release, retained host, STG campaign/job/proof/artifact validation | Task 5 plus parent plan Task 8 | Covered |
| Rollback without worker identity loss | Tasks 2-5 | Covered |

**Scope Check:**

| Plan task | Design requirement | Status |
|---|---|---|
| Task 1 | Side-effect-free version and authenticated semantic probe | Justified |
| Task 2 | Strict durable local state and crash recovery | Justified |
| Task 3 | Verified candidate activation and active provider process | Justified |
| Task 4 | One-time install, autonomous reconciliation, status, rotation, uninstall | Justified |
| Task 5 | Release/runtime proof and parent-plan handoff | Justified |

**Manifest trace:** `plan-scope-check.sh --plan` passed: one PR row, five
existing task headings, and complete task assignment. The workspace
`2026-06-26-github-provider-dogfood-agents.md` scope lock remains authoritative;
this subordinate fix-forward plan implements its existing plugin Task 4 and STG
Task 8 and therefore does not replace or create a competing active lock.

**Drift Items:** None.
