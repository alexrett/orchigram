---
phase: 03-plugin-lifecycle
plan: 01
subsystem: plugins
tags: [grpc, go-plugin, automtls, bundles, process-groups, agents]
requires: [02-01]
provides: [immutable-plugin-bundles, plugin-supervision, exec-plugin, agent-command-plugin, http-plugin, github-task-plugin]
affects: [04-triggers, 05-github-sdlc, 06-operator-surface, 07-publication]
tech-stack:
  added: [hashicorp-go-plugin, Masterminds-semver]
  patterns: [digest-pinning, protocol-negotiation, minimal-environment, rpc-cancel-term-kill, redacted-artifacts]
key-files:
  created: [internal/pluginbundle/bundle.go, internal/pluginhost/host.go, internal/pluginmanager/manager.go, internal/pluginprotocol/protocol.go, internal/pluginruntime/runtime.go, internal/process/runner.go]
  modified: [internal/server/api.go, internal/daemon/daemon.go, internal/flow/compiler.go, internal/cli/root.go]
key-decisions:
  - HashiCorp go-plugin is only the process bootstrap and AutoMTLS layer; protobuf remains the business protocol.
  - Plugin versions are immutable directories and SQLite activation records provide rollback without overwrite.
  - Child commands run in isolated process groups and receive an explicit allowlisted environment.
requirements-completed: [PLG-01, PLG-02, PLG-03]
coverage:
  - deliverable: Bundle install, negotiation, activation, and rollback
    verification:
      - kind: test
        ref: internal/pluginbundle/bundle_test.go and TestPluginVersionsActivateAndRollbackWithoutOverwrite
        status: pass
    human_judgment: false
  - deliverable: Four first-party plugin executables and conformance matrix
    verification:
      - kind: test
        ref: TestFirstPartyPluginConformance
        status: pass
    human_judgment: false
  - deliverable: Real daemon plugin tracer
    verification:
      - kind: test
        ref: TestInstalledPluginExecutesThroughDurableDaemon
        status: pass
    human_judgment: false
  - deliverable: Multi-platform plugin builds
    verification:
      - kind: command
        ref: make check && make cross-build
        status: pass
    human_judgment: false
duration: 58 min
completed: 2026-08-08
---

# Phase 3 Plan 1: Production plugin lifecycle summary

Orchigram now installs digest-pinned plugin bundles, supervises isolated AutoMTLS gRPC processes, executes real plugin-backed Flow nodes, and survives plugin crashes without daemon loss.

## Accomplishments

- Added deterministic tar.gz bundles with strict manifests, semantic versions, protocol ranges, per-platform SHA-256 verification, safe extraction, and immutable version directories.
- Added streaming install/list/enable/disable/doctor control APIs and matching CLI commands.
- Added protocol and health negotiation before installation or activation, capability-aware Flow compilation, and provider-owned action validation.
- Added supervised plugin reuse, crash eviction/restart, activation rollback, bounded calls, malformed stream rejection, and raw artifact preservation.
- Added direct-argv `exec`, typed Codex/Claude/custom `agent-command`, generic idempotent HTTP, and GitHub REST task runtimes as independent executables.
- Added operation-scoped SecretRef resolution, minimal environments, JSONL normalization, raw/output redaction, and agent authentication doctor commands.
- Added process-group cancellation with TERM, bounded wait, KILL, and descendant cleanup verification.

## Verification

- `make check`: passed, including the complete race suite, lint, Buf, secret scan, and native builds.
- `make cross-build`: passed for the core and four plugin executables on darwin/linux amd64/arm64.
- Conformance covers normal execution, three typed fake agent profiles, cancel, timeout, deliberate plugin crash/recovery, incompatible protocol, malformed sequence, duplicate HTTP delivery, stable idempotency, environment isolation, and secret redaction.
- A real daemon gRPC test uploads a bundle in chunks, activates it, compiles an `exec.run` Flow, completes the durable Run, and verifies its artifact.

## Commit

- `35ac088` — implement isolated plugin lifecycle.

## Deviations from Plan

- The GitHub binary implements the shared authenticated REST task boundary here; issue polling and provider streams remain correctly scoped to Phase 5.

## Issues Encountered

- Best-compression packaging of large race-instrumented Go binaries appeared as a stuck race suite. A stack dump proved it was CPU-bound compression; deterministic BestSpeed preserves reproducibility and keeps conformance practical.
- Agent raw logs can echo injected credentials even when Orchigram never logs RPC requests. The host now redacts both raw artifacts and normalized JSON output using operation-scoped secret values.

## Self-Check: PASSED

All plugin gates pass, no plugin process remains after tests, and no secret value or host-only environment variable crosses the verified boundary.
