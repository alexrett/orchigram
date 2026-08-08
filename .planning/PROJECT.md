# Orchigram

Orchigram is a local-first, single-node agent workflow orchestrator for operators who want a k9s-quality terminal experience without a web control plane.

## Core value

Install one Go distribution on a server, connect through a Unix socket or an SSH context, declaratively configure durable workflows, and operate their full lifecycle from the terminal.

## Validated decisions

- Greenfield Go 1.26 codebase under `github.com/alexrett/orchigram`.
- Apache-2.0 license.
- One `orchigram` binary for TUI, CLI, daemon, and installer; first-party plugins remain independent executables.
- SQLite WAL is the single-node system of record.
- gRPC/protobuf for the control plane and plugin business protocol.
- HashiCorp go-plugin for plugin process bootstrap and AutoMTLS.
- A private durable-engine boundary with go-workflows as the initial adapter.
- No TCP listener by default; HTTP webhook ingress is explicit opt-in.
- English-only operator surface.

## Non-negotiable invariants

- Accepted triggers cannot disappear.
- One trigger occurrence maps to one local Run UID.
- External effects are explicitly at-least-once and carry stable idempotency identities.
- Every run pins its compiled plan, interpreter, plugin binaries, and referenced configuration revisions before acknowledgement.
- Secret values never enter resource projections, events, protobuf diagnostics, artifacts, or logs.
- No public network listener exists unless explicitly configured.
- Approval authority comes from Unix/SSH access, not a new login system.
- A GitHub review requesting changes resumes the same logical SDLC run and pull request; approval never implies automatic merge.

## Current milestone: v0.1.0 First Real Release

**Goal:** Turn the published prototype baseline into an operationally honest first release that can run its own public issue-to-review SDLC loop.

**Target features:**

- Reproducible trigger acceptance and execution with pinned dependencies, durable attempts, observable plugin streams, and truthful health.
- A complete declarative control plane and live k9s-style TUI, including an interactive editable process graph.
- GitHub issue, pull-request review, changes-requested rework, repeated checks, and human-controlled merge readiness.
- Bounded single-node operations, safe backup/upgrade, local and remote validation, and a verifiable v0.1.0 release.

## Prototype baseline

Phases 1–7 produced useful protocol, plugin, trigger, GitHub, TUI, SSH, installer, and release-building primitives. They are retained as implementation evidence, not treated as proof that v0.1.0 is complete. The 2026-08-08 gap audit found release-blocking defects in dependency pinning, attempts and observability, API/TUI semantics, controller health, operational bounds, and GitHub review automation.

## Evolution

This document evolves at phase transitions and milestone boundaries.

After each phase transition:

1. Move invalidated requirements out of scope with a reason.
2. Record architecture decisions that constrain later phases.
3. Keep release claims tied to executable acceptance evidence.

Last updated: 2026-08-08
