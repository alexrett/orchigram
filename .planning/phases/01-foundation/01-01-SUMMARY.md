---
phase: 01-foundation
plan: 01
subsystem: foundation
tags: [go, grpc, protobuf, buf, sqlite, release]
requires: []
provides: [versioned-control-api, versioned-plugin-api, build-foundation, strict-config, sqlite-schema]
affects: [all-later-phases]
tech-stack:
  added: [Go-1.26, Cobra, protobuf, gRPC, Buf, YAML-v3]
  patterns: [strict-decoding, checked-in-codegen, append-only-migrations, network-closed-default]
key-files:
  created: [api/orchigram/control/v1alpha1/control.proto, api/orchigram/plugin/v1alpha1/plugin.proto, internal/store/migrations/001_initial.sql, Makefile]
  modified: []
key-decisions:
  - Public APIs use v1alpha1 and reusable metadata/envelope messages.
  - Buf STANDARD naming exceptions are limited to the explicitly approved service names and reusable envelopes.
requirements-completed: [FND-01, FND-02, FND-03]
coverage:
  - deliverable: Deterministic versioned protobuf contracts
    verification:
      - kind: command
        ref: make generate-check && buf lint && buf build
        status: pass
    human_judgment: false
  - deliverable: Strict typed configuration and XDG contexts
    verification:
      - kind: test
        ref: internal/config/config_test.go and internal/contextcfg/context_test.go
        status: pass
    human_judgment: false
  - deliverable: Cross-platform core and plugin executables
    verification:
      - kind: command
        ref: make cross-build
        status: pass
    human_judgment: false
duration: 22 min
completed: 2026-08-07
---

# Phase 1 Plan 1: Protocol and build foundation summary

Greenfield Apache-2.0 Go monorepo with strict public control/plugin contracts, deterministic generation, SQLite migrations, and verified multi-platform builds.

## Accomplishments

- Defined all approved daemon services and plugin capability services in versioned protobuf packages.
- Added strict daemon configuration and XDG context discovery with unknown-field rejection tests.
- Added the initial append-only SQLite schema for resources, revisions, audit, plans, receipts, outbox, runs, attempts, events, approvals, cursors, and plugins.
- Added reproducible generation, lint, race, secret, CI, GoReleaser, SBOM, and cross-build gates.
- Added English architecture, durability, security, resource, and plugin documentation.

## Verification

- `make check`: passed, including deterministic codegen, vet, unit tests, race tests, golangci-lint, Buf, gitleaks, and native build.
- `make cross-build`: passed for five executables on darwin/linux amd64/arm64.
- Secret scan: no leaks found.

## Commits

- `17d55e6` — initialize seven-phase project plan.
- `39d2fe0` — establish protocol and build foundation.

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

- Buf STANDARD intentionally rejected reusable RPC envelopes and unsuffixed plugin service names. The exception list was narrowed to the four relevant naming rules because the approved public contract fixes those names.
- The current gitleaks module retains its historical Go module path; the Make target uses that canonical module path.

## Self-Check: PASSED

All key files exist, all acceptance gates pass, and the Git worktree is clean before this summary commit.

