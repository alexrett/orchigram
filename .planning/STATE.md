# Project state

Status: active recovery milestone
Current milestone: v0.1.0 First Real Release
Current phase: 8 of 12
Progress: 0/5 release phases
Last activity: 2026-08-08
Current focus: immutable accepted plans and pinned execution dependencies

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

## Active phase gate

Phase 8 is complete only when acceptance mutation tests, dependency switch tests, per-attempt observability, truthful degraded health, deterministic fork/join execution, and crash-boundary recovery all pass.

## Blockers

No design or authorization blocker. Live external actions remain explicit gates and must use protected runtime secrets.
