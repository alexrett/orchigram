# Triggers

Every external occurrence crosses the same SQLite transaction boundary. Start
delivery compiles the target Flow and writes its immutable `ExecutionPlan`, a
`TriggerReceipt`, and a `start-run` outbox command in one transaction before it
returns HTTP 202 or acknowledges a provider event. The receipt's stable
occurrence identity selects exactly one local Run UID. External activities are
still at-least-once and must use their stable node idempotency key.

## Schedule

Schedules use five-field cron and an IANA timezone. v0.1 defaults are
`timezone: UTC`, `misfirePolicy: fireOnce`, `startingDeadline: 1h`, and
`concurrencyPolicy: forbid`. On restart the controller selects at most the most
recent missed occurrence. Its identity contains Trigger UID, generation, and
the scheduled UTC instant, so replay cannot create a second local run.

Use `orchigram trigger next TRIGGER_UID`, or inspect the Trigger in the TUI, to
preview calendar occurrences including DST behavior. Enable and disable are
durable controller overrides and do not silently rewrite resource YAML.

## Generic webhook

There is no network listener by default. An operator must configure
`http.listen`; loopback behind Caddy, Tailscale, or equivalent ingress is the
recommended deployment. Hooks are `POST /v1/hooks/{triggerUID}` and require a
per-trigger bearer `SecretRef`. Bodies are JSON and limited to 1 MiB.

`Idempotency-Key` deduplicates retries and returns the original receipt/run.
Without it the daemon accepts a generated non-deduplicated occurrence. TLS and
public routing are deliberately outside Orchigram v0.1.

## Provider stream

Provider plugins open a bidirectional gRPC watch from their persisted cursor.
`spec.provider.source` selects one immutable trigger descriptor when a plugin
publishes multiple sources. It may be omitted only for a plugin with exactly
one source; an ambiguous or unknown source fails validation before launch.
The initial watch also receives the Trigger generation's durable activation
timestamp so providers can default to events that occurred after subscription
creation without losing events across daemon restart. Non-replay bootstrap
fails closed unless the plugin declares
`trigger.bootstrap.activation-fence`; older providers can run only from an
existing cursor or with an explicit replay request.
The daemon sends an acknowledgement only after plan, receipt, outbox, and cursor
are committed together. A stream restart therefore replays safely and dispatch
does not read mutable current Flow state. The bundled GitHub provider uses
polling rather than requiring public ingress. It publishes `github.issues` for
ready-label events and `github.reviews` for submitted pull-request reviews.
Provider acceptance also verifies the authoritative Trigger generation and
enabled state in that transaction. Disable and delete therefore fence an
in-flight watch before it can commit a later receipt; controller cancellation is
cleanup rather than the correctness boundary.

### Resume an active Run

A provider Trigger can explicitly route accepted occurrences to a waiting node
instead of starting another Run:

```yaml
spec:
  flow: issue-to-pr
  provider:
    plugin: github
    source: github.reviews
    config: {}
  delivery:
    mode: signal
    node: wait_review
```

The referenced Flow node must use `core.event`. The provider supplies the target
Run UID in the protocol event envelope; the daemon verifies that the Run is
active, belongs to the referenced Flow UID, and pins a plan containing that
event node. Receipt, provider cursor, payload, and `signal-run` outbox command
commit together before acknowledgement. Missing, terminal, wrong-Flow, or
wrong-node targets fail without advancing the cursor.

`core.event` exposes the accepted JSON object as its result, so CEL edges can
route states such as `result.review.state == "changes_requested"`. It can live
inside a compiler-bounded cycle. Signal dispatch is at-least-once: the durable
interpreter remembers stable provider event IDs and records `event.duplicate`
without advancing another loop iteration after a crash-window redelivery.

The bundled `github.reviews` source scans marked Orchigram pull requests,
including closed PRs needed for restart replay. It extracts the Run UID from
the strict hidden marker written by `github.pr.ensure` and emits only submitted
`CHANGES_REQUESTED` and `APPROVED` reviews. Its cursor is ordered by GitHub's
submission timestamp and stable review ID; the review ID also becomes the
provider occurrence identity. Pending reviews, comments, unsupported states,
and unmarked PRs never enter the signal path.
