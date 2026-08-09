# Project state

Status: active recovery milestone
Current milestone: v0.1.0 First Real Release
Current phase: 10 of 12
Progress: 2/5 release phases
Last activity: 2026-08-09
Current focus: implement durable GitHub pull-request review events and same-branch changes-requested rework

## Reality check

- Phases 1–7 are a useful prototype baseline, not release completion evidence.
- No v0.1.0 tag or release exists.
- The first release includes GitHub pull-request review events, changes-requested rework, repeated checks, and human-controlled merge readiness.
- Secrets supplied for live testing remain operator-owned runtime inputs and must not be copied into source, issues, pull requests, logs, or planning artifacts.

## Decisions

- Keep the greenfield Go architecture and public contracts; repair semantics rather than restart the rewrite again.
- Continue phase numbering so existing implementation history remains legible.
- Work tracer-first through public issue, implementation branch, pull request, review, and merge cycles.
- Compile and persist accepted work before acknowledgement; dispatch never recompiles mutable current resources.
- Pin executable/configuration metadata, but resolve secret values only at runtime through the pinned SecretRef projection.
- Treat plugin events, attempts, artifacts, controller state, and health as public operational state.
- Implement real bounded parallelism because `maxParallel` is already part of the public Flow contract.
- Never auto-merge in v0.1.0.

## Completed phase evidence

Phase 8 passed acceptance mutation and dependency-switch recovery, physical
attempt/event/artifact evidence, deterministic bounded fork/join with hard-crash
resume, and secret-safe degraded health with recovery. The implementation was
delivered through public issues and independently reviewed pull requests.

The first Phase 9 gate now passes strict first-party action contracts, pinned
plugin schema digests, compile-time mapping and typed CEL diagnostics, and
runtime validation against the accepted immutable plan.

The second Phase 9 gate now passes namespace-local resource resolution,
provider config/event contracts, server-owned readiness conditions, and
transactional Trigger/Flow generation checks at the durable acceptance boundary.

The third Phase 9 gate now passes declarative PluginInstallation adoption,
activation, rollback and deterministic conflict handling; persisted status-only
revisions retain generation/CAS semantics and controller failures participate in
aggregate health.

The fourth Phase 9 gate now passes exact label selection, revision-bound keyset
pagination, durable watch replay, desired-state YAML export, and composed Run
filters through the public gRPC API.

The fifth Phase 9 gate now passes the complete scriptable CLI contract through
a real Unix-socket daemon, including validation-first multi-document apply,
watch/export, graph, run evidence, receipts, plugin inspection, and health.

The sixth Phase 9 gate now passes live resource tombstones, revision/sequence
resume, daemon-socket restart recovery, run overlays and historical replay, and
distinct event/attempt/artifact/log views in the same terminal process.

The final Phase 9 gate now passes strict keyboard create/edit/CAS/delete/start,
approval/rejection/cancellation, real plugin upload/activation/rollback/disable,
and context reconnect paths. The same daemon-compiled graph supports node,
forward/backward/self-loop edge selection and schema-backed edits by keyboard
or mouse at supported sizes; a real 80x24 PTY test creates a Flow through the
shipped binary. Phase 9 is complete.

## Active phase gate

Phase 10 is complete only when stable GitHub issue and pull-request review
events drive one durable public issue-to-PR workflow through changes-requested
rework, repeated checks, later approval, merge-ready notification, and a
human-controlled merge without duplicate mutations or leaked credentials.

## Blockers

No design or authorization blocker. Live external actions remain explicit gates and must use protected runtime secrets.
