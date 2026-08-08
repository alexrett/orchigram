---
phase: 06-operator-surface
plan: 01
subsystem: operator-surface
tags: [tui, tcell, ssh, streamlocal, systemd, installer, cas, remote-operations]
requires: [05-01]
provides: [k9s-style-tui, reconnecting-ssh-contexts, hardened-systemd-installer, remote-operator-tracer]
affects: [07-publication]
tech-stack:
  added: []
  patterns: [stateless-tui, pinned-plan-replay, supervised-streamlocal, schema-derived-cas-form, embedded-systemd-unit]
key-files:
  created: [internal/contexttransport/transport.go, internal/install/install.go, docs/operator-guide.md, scripts/verify-ssh-context.sh]
  modified: [internal/tui/app.go, internal/tui/graph_test.go, internal/cli/root.go, api/orchigram/control/v1alpha1/control.proto]
key-decisions:
  - TUI replay requests the immutable plan pinned by the Run rather than recompiling the current Flow.
  - SSH contexts supervise one OpenSSH StreamLocal process and preserve the local socket path across reconnects so the same gRPC connection can recover.
  - The embedded installer is the supported single-node upgrade path and always restarts the daemon before plugin bootstrap.
  - SecretRef status is projected by the server and removed from desired state before apply.
requirements-completed: [OPS-01, OPS-02, OPS-03]
coverage:
  - deliverable: Keyboard and mouse graph definition, live overlay, and historical replay
    verification:
      - kind: test
        ref: TestOperatorApprovesWaitingRunThroughTUI, TestGraphDrawsAndNavigatesAtSupportedSizes, TestGraphMouseSelectsAndOpensNode
        status: pass
    human_judgment: false
  - deliverable: Reconnecting local Unix and OpenSSH StreamLocal contexts
    verification:
      - kind: test
        ref: TestSSHContextReconnectsAtTheSameSocket and scripts/verify-ssh-context.sh
        status: pass
    human_judgment: false
  - deliverable: Hardened non-root install with no implicit network listener
    verification:
      - kind: remote
        ref: Ubuntu 24.04 install, exec plus approval tracer, tunnel interruption, daemon restart, and installer upgrade
        status: pass
    human_judgment: false
  - deliverable: Multi-platform builds and repository quality gates
    verification:
      - kind: command
        ref: make check && make cross-build
        status: pass
    human_judgment: false
duration: 75 min
completed: 2026-08-08
---

# Phase 6 Plan 1: Remote operator surface summary

Orchigram now has an English, keyboard-first terminal operator surface, local and SSH contexts using the same gRPC API, and a repeatable hardened systemd installation verified on a real Ubuntu 24.04 host.

## Accomplishments

- Expanded the TUI to Contexts, Flows, Triggers, Repositories, AgentProfiles, Runs, Plugins, SecretRefs, and System, with filtering, a command palette, CAS forms, read-only YAML, trigger and plugin operations, cancellation, durable approval, and graph drill-down.
- Added an exact pinned-plan RPC so definition, live run status, and historical replay use the correct immutable graph even after the source Flow changes.
- Added XDG context mutation and supervised OpenSSH StreamLocal forwarding with option-safe argv, reconnect backoff, private temporary sockets, and reconnecting run event streams.
- Added a Linux installer that provisions the stable service identity, exact state/runtime/log paths, four immutable plugin bundles, and a strongly sandboxed systemd unit without a TCP listener.
- Added an operator smoke Flow and an environment-driven SSH verification script with no test-host address in source.
- Verified the remote exec plus approval tracer, PTY layouts at 80x24, 120x40, and 160x50, tunnel interruption, daemon restart, and an installer upgrade while approval was waiting.
- Removed the earlier spike service only after the replacement passed and rotated the previously shared root password without logging the replacement; key access remains verified.

## Verification

- `make check`: passed, including tests, race, vet, lint, Buf, deterministic generation, and gitleaks.
- `make cross-build`: passed for the core and all plugins on darwin/linux amd64/arm64.
- `scripts/verify-ssh-context.sh`: passed against the real remote service.
- Remote `systemd-analyze security`: 1.3 exposure, improving on the spike baseline of 2.6.
- Remote listener inspection found no Orchigram TCP/UDP listener; control remained a mode-0660 Unix socket owned by `orchigram:orchigram`.

## Commit

- `10bd0a9` — complete remote operator surface.

## Deviations from Plan

- Codex and Claude are not installed on the clean remote host; installer discovery correctly reports both as warnings. Their protocol and fake-command profiles remain covered by Phase 3 conformance tests.

## Self-Check: PASSED

All Phase 6 automated and real-host gates pass. The old spike was removed only after the installed service, plugins, remote context, durable approval, reconnect, hardening, and network-closed behavior were verified.
