# Durability contract

Orchigram guarantees that an acknowledged trigger has a durable `TriggerReceipt` and outbox command in the same SQLite transaction. Reconciliation may repeat dispatch, but the receipt's occurrence identity maps to exactly one local Run UID.

External activities are at-least-once. The unavoidable crash window is after a remote side effect succeeds and before its completion is recorded locally. Every plugin call therefore receives a stable idempotency key derived from run, node, logical iteration, and operation. Providers that cannot enforce it must reconcile with deterministic remote identifiers or hidden markers and expose the residual risk to the operator.

Approvals, retry timers, plan versions, provider cursors, and run events are durable. TUI state is not. On upgrade, an existing run remains pinned to its interpreter version or becomes visibly blocked; it is never silently reinterpreted by incompatible code.

