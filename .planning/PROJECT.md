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
- External effects are explicitly at-least-once.
- Every run pins its plan hash and interpreter version.
- Secrets never enter resource projections, events, protobuf diagnostics, or logs.
- No public network listener exists unless explicitly configured.
- Approval authority comes from Unix/SSH access, not a new login system.

## Current state

Phases 1-6 are complete: protocol, durable execution, plugins, triggers, GitHub tracer, TUI, SSH contexts, and hardened remote installation are verified. Phase 7 is executing the publication and reproducibility gates.

Last updated: 2026-08-08
