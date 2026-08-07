---
phase: 04-triggers
plan: 01
subsystem: triggers
tags: [cron, dst, webhook, provider-stream, outbox, teams, tui]
requires: [03-01]
provides: [native-schedules, durable-webhook, provider-cursors, teams-tracer, trigger-tui]
affects: [05-github-sdlc, 06-operator-surface, 07-publication]
tech-stack:
  added: [robfig-cron]
  patterns: [stable-occurrence-identity, receipt-before-ack, fire-once-misfire, operator-enable-override, declarative-json-pointer-mapping]
key-files:
  created: [internal/trigger/controller.go, internal/httpingress/server.go, docs/triggers.md, examples/teams/daily-flow.yaml]
  modified: [internal/store/store.go, internal/daemon/daemon.go, internal/pluginmanager/manager.go, internal/tui/app.go]
key-decisions:
  - Schedule occurrence identity is Trigger UID plus generation plus scheduled UTC instant.
  - Provider cursor, receipt, and outbox are committed before the plugin acknowledgement.
  - HTTP ingress remains absent unless http.listen is explicitly configured.
requirements-completed: [TRG-01, TRG-02, TRG-03, TRG-04]
coverage:
  - deliverable: DST-safe schedules, fireOnce catch-up, concurrency forbid, and restart deduplication
    verification:
      - kind: test
        ref: TestNextOccurrencesHandleBerlinDSTTransitions and TestNativeScheduleRestartCreatesExactlyOneRun
        status: pass
    human_judgment: false
  - deliverable: Durable authenticated webhook ingress
    verification:
      - kind: test
        ref: TestWebhookAuthorizationLimitAndDurableDeduplication
        status: pass
    human_judgment: false
  - deliverable: Receipt-before-ack provider contract
    verification:
      - kind: test
        ref: TestProviderAcknowledgesOnlyAfterReceiptAndCursorPersistence
        status: pass
    human_judgment: false
  - deliverable: Teams-compatible scheduled tracer with secret-only URL
    verification:
      - kind: test
        ref: TestFirstPartyPluginConformance and TestShippedResourcesAreStrictAndFlowsCompile
        status: pass
    human_judgment: false
  - deliverable: Multi-platform builds and repository quality gates
    verification:
      - kind: command
        ref: make check && make cross-build
        status: pass
    human_judgment: false
duration: 46 min
completed: 2026-08-08
---

# Phase 4 Plan 1: Trigger control plane and Teams tracer summary

Orchigram now accepts schedule, generic webhook, manual, and provider events through the same durable receipt/outbox boundary and exposes Trigger operations in both CLI and TUI.

## Accomplishments

- Added five-field cron schedules with IANA timezones, deterministic UTC occurrence identities, DST handling, one-event catch-up, starting deadlines, overlap prevention, and durable skip records.
- Fixed restart replay so an already persisted occurrence advances its cursor rather than being misclassified as a forbidden overlap.
- Added opt-in HTTP ingress with constant-time bearer verification, 1 MiB JSON limit, durable 202 responses, optional idempotency keys, and no default network listener.
- Added provider bidirectional watch integration with cursor/receipt/outbox persistence before acknowledgement and bounded reconnect backoff.
- Added Trigger CLI next/enable/disable and TUI list/detail/control surfaces with next fire, last receipt/run, and last skip reason.
- Added output-aware declarative JSON Pointer mappings and HTTP URL resolution through SecretRef, allowing agent output to populate a Teams-compatible Adaptive Card without persisting its webhook URL.
- Added an English weekday Europe/Berlin Teams tracer and strict example validation.

## Verification

- `make check`: passed, including unit/integration tests, the complete race suite, vet, lint, Buf, and gitleaks.
- `make cross-build`: passed for core and four plugin binaries on darwin/linux amd64/arm64.
- A daemon-level restart test simulates loss after receipt commit and before schedule cursor visibility, then proves one receipt, one Run UID, and one successful run.
- Webhook tests cover missing/wrong bearer, oversized JSON, generated identities, duplicate keys, durable outbox dispatch, and operator disable.

## Commit

- `bfce3a9` — implement durable trigger control plane.

## Deviations from Plan

- Real Teams delivery remains intentionally operator-gated; automated tests send only to `httptest` as required.

## Self-Check: PASSED

All Phase 4 gates pass and the default daemon opens only its Unix socket.
