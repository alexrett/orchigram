---
phase: 02-manual-skeleton
plan: 01
subsystem: execution-core
tags: [sqlite, cel, go-workflows, grpc, tui, durability]
requires: [01-01]
provides: [strict-resources, flow-compiler, transactional-outbox, pinned-runs, durable-approvals, interactive-graph]
affects: [03-plugin-lifecycle, 04-triggers, 05-github-sdlc, 06-operator-surface]
tech-stack:
  added: [modernc-sqlite, cel-go, go-workflows, tcell, tview]
  patterns: [compare-and-swap, immutable-execution-plan, transactional-outbox, durable-signal, stateless-tui]
key-files:
  created: [internal/store/store.go, internal/flow/compiler.go, internal/engine/engine.go, internal/orchestrator/orchestrator.go, internal/server/api.go, internal/tui/app.go]
  modified: [cmd/orchigram/main.go, go.mod]
key-decisions:
  - The public resource model and execution plan remain independent from go-workflows types.
  - Approval decisions persist before signal delivery and are redelivered by reconciliation.
  - Daemon shutdown gives active streams a bounded grace period before forcing gRPC stop.
requirements-completed: [CORE-01, CORE-02, CORE-03, CORE-04]
coverage:
  - deliverable: Strict resources and Flow compilation
    verification:
      - kind: test
        ref: internal/resource/resource_test.go and internal/flow/compiler_test.go
        status: pass
    human_judgment: false
  - deliverable: Pinned run and transactional outbox recovery
    verification:
      - kind: test
        ref: internal/orchestrator/orchestrator_test.go and internal/daemon/daemon_test.go
        status: pass
    human_judgment: false
  - deliverable: Interactive TUI approval
    verification:
      - kind: simulation
        ref: TestOperatorApprovesWaitingRunThroughTUI
        status: pass
    human_judgment: false
  - deliverable: Multi-platform walking-skeleton binaries
    verification:
      - kind: command
        ref: make check && make cross-build
        status: pass
    human_judgment: false
duration: 41 min
completed: 2026-08-08
---

# Phase 2 Plan 1: Manual durable walking skeleton summary

Strict declarative Flow resources now run end to end through SQLite, gRPC, CLI, the durable interpreter, and an interactive terminal graph with restart-safe approval.

## Accomplishments

- Added strict typed resource decoding, CAS revisions, generations, global watch revisions, and audit records.
- Added CEL-aware Flow compilation, SCC cycle validation, expanded defaults, canonical plan hashes, and immutable stored plans.
- Added durable trigger receipts, a transactional outbox, one Run UID per occurrence, and crash-boundary reconciliation.
- Kept go-workflows behind `DurableEngine` and implemented the version-pinned generic interpreter with run/node events, approval, rejection, cancellation, and activity boundaries.
- Added real UDS gRPC services and kubectl-style apply/get/describe/delete/flow/run commands.
- Added a custom interactive ASCII graph and stateless TUI resource/run views with live event overlays and approval forms.

## Verification

- `make check`: passed, including deterministic codegen, vet, unit tests, race tests, golangci-lint, Buf, gitleaks, and native builds.
- `make cross-build`: passed for five executables on darwin/linux amd64/arm64.
- `TestRunApprovalSurvivesDaemonRestart`: the daemon restarts at `approval.waiting`, the occurrence still resolves to the same Run UID, and the persisted run completes after approval.
- `TestOperatorApprovesWaitingRunThroughTUI`: a 120x40 simulated terminal selects a Run, opens the approval form with the keyboard, submits it, and observes `run.succeeded`.
- Graph simulations pass at 80x24, 120x40, and 160x50.

## Commit

- `f5125fd` — implement manual durable walking skeleton.

## Deviations from Plan

None. Phase 2 was implemented as the requested vertical tracer rather than as independent horizontal subsystems.

## Issues Encountered

- An accepted approval initially left its losing durable timer scheduled. The interpreter now cancels that timer when the signal wins.
- Retrying a claimed outbox command after editing its Flow initially recompiled the latest resource. Reconciliation now reloads the Run's stored plan hash.
- An active watch stream could hold `GracefulStop` indefinitely. Daemon shutdown now has a three-second grace bound before forced stop.

## Self-Check: PASSED

All key files exist, automated acceptance gates pass, external activities are explicitly documented as at-least-once, and the code commit contains no planning metadata.
