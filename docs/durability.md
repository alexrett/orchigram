# Durability contract

Orchigram guarantees that an acknowledged trigger has a durable `TriggerReceipt` and outbox command in the same SQLite transaction. Reconciliation may repeat dispatch, but the receipt's occurrence identity maps to exactly one local Run UID.

External activities are at-least-once. The unavoidable crash window is after a remote side effect succeeds and before its completion is recorded locally. Every plugin call therefore receives a stable idempotency key derived from run, node, logical iteration, and operation. Providers that cannot enforce it must reconcile with deterministic remote identifiers or hidden markers and expose the residual risk to the operator.

Approvals, retry timers, plan versions, provider cursors, and run events are durable. TUI state is not. On upgrade, an existing run remains pinned to its interpreter version or becomes visibly blocked; it is never silently reinterpreted by incompatible code.

The compiler collapses strongly connected components and rejects every cycle
without a finite `loop.maxIterations` policy. The durable interpreter executes
the resulting component DAG deterministically. Each loop iteration receives a
different stable activity identity, while activated edges leaving the component
are accumulated across iterations. Editing the source Flow never changes the
compiled plan already pinned to an active Run.

Online backups contain consistent snapshots of the resource/event database,
the workflow engine database, and immutable plugin installations. Restore is an
offline operation into a new directory; recovery tests reopen the snapshot and
reconcile an approval that was waiting when the backup was taken.
