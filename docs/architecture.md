# Architecture

Orchigram has one authoritative daemon and two stateless operator clients: CLI commands and the TUI. Both use the same gRPC API over a Unix socket. A remote context asks OpenSSH to forward the remote Unix socket to a temporary local Unix socket; there is no second remote protocol.

```text
CLI / TUI ── gRPC/UDS ── daemon ── SQLite WAL
    │                       ├────── durable interpreter
    └── OpenSSH UDS ───────└────── gRPC plugin processes
```

Resources use `orchigram.dev/v1alpha1`, strict decoding, Kubernetes-style metadata, revisions, generations, labels, status, events, and optimistic concurrency. Before acknowledging a trigger, the daemon compiles a `Flow` into an immutable `ExecutionPlan` and atomically persists that plan with the receipt and outbox command. Every `Run` pins its plan hash and interpreter version. Non-core plan nodes additionally pin the plugin version and digest, canonical action schemas and contract digest, plus referenced resource metadata/spec snapshots; secret values remain runtime-only.

One namespace-aware resolver validates desired-resource dependencies for apply,
GET/LIST/WATCH readiness projection, Flow compilation, webhook/provider setup,
and runtime binding. Trigger acceptance rechecks the current Trigger generation,
enabled state, target Flow name, and compiled Flow UID/generation inside the
receipt/plan/outbox transaction. See [Resource references](resource-references.md).

The daemon owns triggers, receipts, the transactional outbox, run state,
physical attempt evidence, approvals, plugin lifecycle, workspaces, and
artifacts. Plugins own only provider-specific trigger, task, or agent behavior.
Public resources never expose go-workflows or go-plugin types. Run inspection
uses `RunService`: event replay includes validated plugin stream events,
`ListAttempts` exposes retry identity and outcome, while `ListArtifacts` and
bounded `GetArtifact` expose registered evidence without leaking daemon paths.

`sdk/plugin` is the public author boundary. It owns the shared go-plugin
handshake, protocol-v1 negotiation, task adapters, cancellation registry,
health/draining state, sequence/timestamp assignment, and exactly-one-terminal
stream contract. The host uses the same bridge. Task authors work with JSON SDK
types; only advanced trigger and agent implementations use the public generated
protobuf package. Every task action publishes config/input/output schemas. The
SDK and daemon validate those schemas independently, installation persists a
canonical contract digest, and a later process restart must reproduce it.
Arbitrary plugin streams are still validated independently by the daemon before
their output is trusted.

The interpreter schedules the compiled component DAG, not individual plugin
processes. `maxParallel` bounds active components; stable topological admission
makes the choice of the next ready component replay-safe. Strongly connected
components remain serial inside one slot, and fan-in waits for all predecessor
components, including skipped branches. On the first failed component, the
scheduler stops admission and propagates a durable run-scoped cancellation to
active plugin or agent calls before committing the final Run failure.

`SystemService.Health` is an aggregate control-plane projection rather than a
socket liveness check. Runtime reconciliation owners retain secret-safe failure
state until they observe recovery, while every active plugin receives a bounded
health RPC. The first probe after process loss reports the exited immutable
version; a later successful restart and health pass clears it. Configuration,
schema migration, artifact reconciliation, and listener failures remain
fail-closed startup errors because no authoritative daemon can safely serve
without them.

## Repository layout

- `api/`: versioned protobuf sources.
- `gen/`: checked-in deterministic Go protobuf output.
- `sdk/plugin/`: stable framework-independent community plugin API.
- `cmd/`: core and independent first-party plugin executables.
- `internal/`: daemon, persistence, compiler, interpreter, TUI, transport, and plugin internals.
- `examples/`: complete reference resources.
- `deploy/`: systemd assets.
