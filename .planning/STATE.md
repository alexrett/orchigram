# Project state

Status: active recovery milestone
Current milestone: v0.1.0 First Real Release
Current phase: 9 of 12
Progress: 1/5 release phases
Last activity: 2026-08-08
Current focus: consistent cross-resource resolution and the complete live operator surface

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

## Active phase gate

Phase 9 is complete only when typed plugin schemas and references fail before
storage, the declared API/CLI contracts are executable, and a keyboard-only
operator can edit and run the same interactive graph projection while watches,
reconnect, attempt evidence, and artifacts remain live.

## Blockers

No design or authorization blocker. Live external actions remain explicit gates and must use protected runtime secrets.
