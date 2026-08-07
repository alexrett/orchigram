# Architecture

Orchigram has one authoritative daemon and two stateless operator clients: CLI commands and the TUI. Both use the same gRPC API over a Unix socket. A remote context asks OpenSSH to forward the remote Unix socket to a temporary local Unix socket; there is no second remote protocol.

```text
CLI / TUI ── gRPC/UDS ── daemon ── SQLite WAL
    │                       ├────── durable interpreter
    └── OpenSSH UDS ───────└────── gRPC plugin processes
```

Resources use `orchigram.dev/v1alpha1`, strict decoding, Kubernetes-style metadata, revisions, generations, labels, status, events, and optimistic concurrency. The daemon compiles a `Flow` into an immutable `ExecutionPlan`; every `Run` pins its plan hash and interpreter version.

The daemon owns triggers, receipts, the transactional outbox, run state, approvals, plugin lifecycle, workspaces, and artifacts. Plugins own only provider-specific trigger, task, or agent behavior. Public resources never expose go-workflows or go-plugin types.

## Repository layout

- `api/`: versioned protobuf sources.
- `gen/`: checked-in deterministic Go protobuf output.
- `cmd/`: core and independent first-party plugin executables.
- `internal/`: daemon, persistence, compiler, interpreter, TUI, transport, and plugin internals.
- `examples/`: complete reference resources.
- `deploy/`: systemd assets.

