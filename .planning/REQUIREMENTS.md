# Requirements

## Foundation

- [x] FND-01: Clean Apache-2.0 Go repository with reproducible protobuf generation.
- [x] FND-02: Control and plugin APIs are versioned, linted, and backward-compatibility checked.
- [x] FND-03: Linux/macOS amd64/arm64 builds for core and four plugins.

## Resource and execution core

- [ ] CORE-01: Strict resources with metadata, CAS revisions, generations, events, and audit.
- [ ] CORE-02: Strict Flow compilation, CEL validation, finite-cycle validation, canonical plan hash.
- [ ] CORE-03: Durable trigger receipt plus transactional outbox and one Run UID per occurrence.
- [ ] CORE-04: Pinned execution plan, durable approvals, cancellation, events, and recovery.

## Plugins

- [ ] PLG-01: Immutable bundle install, digest verification, protocol negotiation, activation, rollback.
- [ ] PLG-02: Agent command, exec, HTTP, and GitHub plugin binaries.
- [ ] PLG-03: Crash isolation, deadline/cancel propagation, process-tree cleanup, secret-minimized environments.

## Triggers

- [ ] TRG-01: Five-field schedules with timezone, DST, misfire, concurrency, and deduplication.
- [ ] TRG-02: Opt-in durable webhook ingress with bearer SecretRef and body limit.
- [ ] TRG-03: Cursor-backed provider subscriptions and persisted acknowledgement.
- [ ] TRG-04: Weekday Teams-compatible scheduled tracer.

## GitHub tracer

- [ ] GIT-01: GitHub issue polling with stable cursor/event identity.
- [ ] GIT-02: Isolated workspace plan/approval/implement/test/push/PR workflow.
- [ ] GIT-03: Hidden-marker and deterministic-branch reconciliation prevents duplicate mutations.

## Operator surface

- [ ] OPS-01: English keyboard-first TUI with graph definition/live/replay modes.
- [ ] OPS-02: Local Unix and OpenSSH StreamLocal contexts share the same API.
- [ ] OPS-03: Non-root hardened systemd install with no network listener by default.

## Publication

- [ ] PUB-01: Tests, secret scan, license inventory, SBOM, reproducible artifacts and attestations.
- [ ] PUB-02: Backup/restore, active-run upgrade, plugin rollback, clean Ubuntu installation.
- [ ] PUB-03: Documentation reproduces both end-to-end tracers before public v0.1.0.
