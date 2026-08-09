# v0.1.0 First Real Release Requirements

The existing code is a prototype baseline. A checked requirement below means its observable release gate has passed, not merely that a type, command, or test double exists.

## Phase 8 — Reproducible durable execution

- [x] DUR-01: Every manual, schedule, webhook, and provider event is compiled before acknowledgement, and its receipt, immutable execution plan, and outbox command are committed atomically.
- [x] DUR-02: A run pins plugin name/version/digest/protocol plus the UID, generation, and resource version of each AgentProfile, Repository, and SecretRef metadata projection it uses; secret values remain runtime-only.
- [x] DUR-03: Editing or deleting a Flow or referenced resource after trigger acceptance cannot change or strand the accepted run.
- [x] DUR-04: Node attempts have durable monotonically increasing identities across retries and restarts; each attempt owns separate events, logs, artifacts, and exit outcome.
- [x] DUR-05: Structured plugin stream events are validated, persisted, watchable, and retained alongside downloadable raw artifacts.
- [x] DUR-06: `maxParallel` provides real bounded deterministic execution, including joins, cancellation, retry, and crash recovery, rather than a schema-only promise.
- [x] DUR-07: Every external activity receives a stable idempotency key derived from run, node, and logical attempt and reuses it after ambiguous completion.
- [x] DUR-08: Health and diagnostics report outbox backlog, controller failures, plugin availability, migration state, and degraded readiness instead of returning an unconditional ready state.
- [x] DUR-09: Crash-boundary tests cover acceptance, dispatch, activity side effects, completion recording, approval, retry timers, and concurrent nodes without duplicating the local Run UID.

## Phase 9 — Declarative control plane and operator surface

- [x] CTL-01: First-party plugins publish non-empty input/output JSON schemas; the Flow compiler validates field mappings, CEL types, required references, and output compatibility before storage.
- [x] CTL-02: Cross-resource references are resolved consistently and rejected or surfaced as explicit status conditions; a Trigger cannot durably accept work for a missing Flow.
- [x] CTL-03: PluginInstallation resources drive an actual reconciliation controller with observed generation, immutable version status, activation state, and diagnostics.
- [x] CTL-04: Resource label selectors, pagination, run filters, watches, and export behave as declared by the public protobuf API.
- [x] CLI-01: CLI coverage includes resource watch/export, flow graph, run list/describe/reconcile, trigger receipts, plugin describe, and system health; apply accepts stdin and multi-document YAML.
- [x] TUI-01: Resource and run screens consume watches, reconnect without stale snapshots, and show controller/status changes live.
- [x] TUI-02: Keyboard-only TUI operations can create, edit with CAS, delete, start, approve/reject/cancel, install/activate/rollback plugins, and switch contexts.
- [x] TUI-03: The ASCII graph is interactive at 80x24 and larger: nodes and edges are selectable by keyboard or mouse, Enter opens schema-derived settings, and validated changes update the same declarative Flow projection.
- [x] TUI-04: Definition, live overlay, and historical replay use the same graph while logs, structured events, attempt history, and artifacts remain distinct inspectable views.

## Phase 10 — GitHub review-event SDLC automation

- [x] GHR-01: The GitHub TriggerProvider durably emits stable, cursor-backed issue events and pull-request review events, including submitted approval and changes-requested state.
- [x] GHR-02: The reference SDLC flow posts and reconciles a plan, waits for durable TUI approval, implements on a deterministic branch, runs tests, and creates or reconciles one pull request.
- [x] GHR-03: A changes-requested review resumes the same logical workflow, feeds review context to a workspace-write rework agent, and updates the same branch and pull request.
- [x] GHR-04: Every rework cycle reruns configured checks and reconciles comments, commits, pushes, and pull-request state without duplicate mutations after daemon or plugin restart.
- [x] GHR-05: Approval plus configured green checks marks a run merge-ready and notifies the operator; v0.1.0 never auto-merges or pushes directly to the default branch.
- [x] GHR-06: A public two-account dogfood issue, pull request, changes-requested review, repair, approval, and human merge passes with retained run evidence and no credential disclosure.

## Phase 11 — Bounded single-node operations

- [x] OPR-01: Configurable global and per-run concurrency, agent-process, memory, and CPU bounds prevent one workload from exhausting the daemon host.
- [x] OPR-02: Retention and garbage collection cover runs, events, receipts, artifacts, workspaces, backups, and inactive plugin versions with dry-run diagnostics and safe defaults.
- [x] OPR-03: Backup establishes a documented consistent barrier across resource and workflow state, and restore reconciles accepted triggers and active runs.
- [x] OPR-04: Core and bundled-plugin upgrade is staged and health-checked with automatic rollback on partial failure; active approvals and retry timers survive.
- [x] OPR-05: Installer and doctor verify system git, configured agent CLI compatibility, authentication state, writable storage, UDS access, and plugin health without printing secrets.
- [x] OPR-06: CI actions are commit-pinned, release history scanning is mandatory, and the hardened service remains non-root with no default TCP listener.

## Phase 12 — Release proof and publication

- [ ] REL-01: `make check`, race, lint, Buf lint/breaking, secret/history scan, dependency licenses, SPDX SBOM, and reproducibility checks run in the release workflow.
- [ ] REL-02: Signed checksums, provenance/attestations, core archives, and independent first-party plugin bundles are produced for supported macOS/Linux amd64/arm64 targets.
- [x] REL-03: A local native schedule-to-Slack-compatible webhook tracer passes with fake-receiver CI coverage and an explicitly authorized real delivery.
- [x] REL-04: The demo server passes clean install, SSH TUI reconnect, restart/crash recovery, resource limits, backup/restore, and upgrade/rollback without publishing its address or credentials.
- [ ] REL-05: English documentation reproduces the notification and full GitHub review tracers; only then is v0.1.0 tagged and the release published.

## Deferred beyond v0.1.0

- Multi-node HA, Raft, RBAC, and hostile multi-tenant plugins.
- Kubernetes contexts and pod execution.
- Jira, Trello, and provider-specific public webhook signatures.
- Vault/KMS ownership and exactly-once semantics for external services.

## Traceability

| Requirement | Phase |
|---|---:|
| DUR-01..09 | 8 |
| CTL-01..04, CLI-01, TUI-01..04 | 9 |
| GHR-01..06 | 10 |
| OPR-01..06 | 11 |
| REL-01..05 | 12 |
