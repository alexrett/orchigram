# Orchigram v0.1.0 First Real Release Roadmap

Phases 1–7 are retained as the prototype baseline. The release milestone continues at Phase 8 and requires executable evidence for every phase.

| Phase | Goal | Requirements | Status |
|---|---|---|---|
| 1–7 | Prototype protocol, plugins, triggers, GitHub tracer, TUI, install, and release-building primitives | Historical baseline | Audited; not release proof |
| 8 | Make accepted work reproducible, durable, and observable | DUR-01..09 | Complete |
| 9 | Make schemas, APIs, CLI, and TUI deliver the declared operator contract | CTL-01..04, CLI-01, TUI-01..04 | Complete |
| 10 | Close the public GitHub review and rework loop | GHR-01..06 | In progress |
| 11 | Bound and recover single-node production operation | OPR-01..06 | Pending |
| 12 | Prove both tracers and publish verifiable artifacts | REL-01..05 | Pending |

## Phase 8 — Reproducible durable execution

**Goal:** An acknowledged occurrence always executes the exact validated plan and dependencies that were accepted, with durable attempt-level evidence.

**Success criteria:**

1. Accept a trigger, edit or remove its Flow/profile/repository/plugin activation before dispatch, and recover one run that uses the accepted snapshot.
2. Kill the daemon at each acceptance, outbox, activity, completion, approval, retry, and parallel-join boundary without losing the occurrence or corrupting attempt identity.
3. Observe validated plugin events and separate per-attempt artifacts through the public API and see degraded health for injected controller/plugin failures.
4. Execute a fork/join Flow with a real `maxParallel` bound and deterministic replay.

## Phase 9 — Declarative control plane and operator surface

**Goal:** The public schemas and k9s-style terminal surface become a complete, live projection of the server rather than a partial snapshot viewer.

**Success criteria:**

1. Invalid plugin mappings, CEL types, references, and plugin installations fail before activation with actionable diagnostics.
2. API and CLI tests prove selectors, pagination, filters, watch/export, multi-document apply, health, receipts, and reconciliation.
3. From the TUI alone, an operator connects to a context, creates and edits a Flow graph, installs dependencies, starts a run, inspects attempts/events/logs/artifacts, approves it, and observes completion.
4. Keyboard-only and mouse graph tests pass at 80x24, 120x40, and 160x50, including reconnect and CAS conflicts.

## Phase 10 — GitHub review-event SDLC automation

**Goal:** Orchigram runs its own public issue-to-reviewed-pull-request lifecycle, including changes-requested repair.

**Success criteria:**

1. Stable GitHub issue and review event identities resume correctly after cursor acknowledgement loss or plugin restart.
2. One issue produces one branch, one pull request, and reconciled comments across planning, approval, implementation, tests, and ambiguous mutation retries.
3. A second-account changes-requested review triggers rework on the same branch; tests rerun; a later approval and green checks mark the run merge-ready.
4. A human performs the merge, and the complete public dogfood history maps to retained Orchigram run/attempt evidence without exposing secrets.

## Phase 11 — Bounded single-node operations

**Goal:** A small server can run Orchigram predictably, recoverably, and safely through failure and upgrade.

**Success criteria:**

1. Stress tests prove configured workload/process/resource bounds and useful degraded diagnostics instead of host exhaustion.
2. Retention dry-run and collection preserve active runs and referenced artifacts while reclaiming eligible state.
3. A consistent backup restores accepted and active work, and staged upgrade failure rolls back core plus bundled plugins.
4. The installed service remains non-root, network-closed by default, and no weaker than the validated systemd security baseline.

## Phase 12 — Release proof and publication

**Goal:** Publish v0.1.0 only from reproducible artifacts after both real tracer scenarios and all failure gates pass.

**Success criteria:**

1. Release CI runs every code, protocol, history, license, SBOM, reproducibility, and provenance gate from a clean checkout.
2. Local schedule notification and public GitHub review-loop tracers are reproducible from English documentation.
3. A clean demo-server install passes SSH TUI reconnect, crash recovery, backup/restore, and upgrade/rollback with redacted public evidence.
4. The published tag, checksums, attestations, core archives, and plugin bundles correspond byte-for-byte to the tested commit.
