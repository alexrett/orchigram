# Durability contract

Orchigram compiles work before acknowledging it. The immutable
`ExecutionPlan`, `TriggerReceipt`, and outbox command are committed in the same
SQLite transaction. Reconciliation may repeat dispatch, but the receipt's
occurrence identity maps to exactly one local Run UID and dispatch loads the
accepted plan by hash instead of recompiling the current Flow.

Each non-core plan node pins the installed plugin name, version, bundle digest,
and negotiated protocol. It also snapshots the UID, generation, resource
version, and spec of referenced AgentProfile, Repository, and SecretRef
resources. A SecretRef spec contains only a backend coordinate; the value is
resolved when the activity runs and is never written into the plan. Flow edits,
resource deletion, or plugin activation changes after acknowledgement therefore
cannot alter or strand the accepted execution.

External activities are at-least-once. The unavoidable crash window is after a remote side effect succeeds and before its completion is recorded locally. Every plugin call therefore receives a stable idempotency key derived from run, node, logical iteration, and operation. Providers that cannot enforce it must reconcile with deterministic remote identifiers or hidden markers and expose the residual risk to the operator.

The Slack Incoming Webhook tracer demonstrates the residual case. Orchigram
keeps its `Idempotency-Key` stable across HTTP retries, but Incoming Webhooks
expose no documented deduplication identifier. If Slack accepts a message and
the daemon crashes before recording completion, replay can post a duplicate.
This is an inference from the Incoming Webhook contract, not a Slack delivery
guarantee. Successful calls normally return `HTTP 200` and `ok`; every other
status is a failed attempt. Incoming Webhooks are limited to approximately one
message per second with short bursts tolerated, and `429` responses include
`Retry-After`. See Slack's [Incoming Webhook
contract](https://api.slack.com/incoming-webhooks) and [rate-limit
documentation](https://docs.slack.dev/apis/web-api/rate-limits/).

Workflow and activity workers heartbeat every 500 milliseconds, comfortably
inside the two-second SQLite task leases. Long-running activities renew
ownership instead of becoming eligible for concurrent redispatch.

`succeeded`, `failed`, `rejected`, and `cancelled` are immutable terminal run
phases. The store reads the current phase in the same transaction as every
event transition; late node completions, failures, or approval waits become
idempotent no-ops. Run cancellation also propagates directly to active task and
agent calls. The bounded provider `Cancel` RPC runs before the streaming RPC is
cancelled, and command providers terminate the process group with `SIGTERM`
followed by bounded `SIGKILL` escalation.

Approvals, retry timers, plan versions, provider cursors, and run events are durable. TUI state is not. On upgrade, an existing run remains pinned to its interpreter version or becomes visibly blocked; it is never silently reinterpreted by incompatible code.

The compiler collapses strongly connected components and rejects every cycle
without a finite `loop.maxIterations` policy. The durable interpreter executes
the resulting component DAG deterministically. Each loop iteration receives a
different stable activity identity, while activated edges leaving the component
are accumulated across iterations. Editing or deleting the source Flow never
changes a compiled plan after trigger acceptance, including before the local
Run projection and durable workflow instance have been created.

Online backups contain consistent snapshots of the resource/event database,
the workflow engine database, and immutable plugin installations. Restore is an
offline operation into a new directory; recovery tests reopen the snapshot and
reconcile an approval that was waiting when the backup was taken.
