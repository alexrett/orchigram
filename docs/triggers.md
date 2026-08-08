# Triggers

Every external occurrence crosses the same SQLite transaction boundary. The
daemon writes a `TriggerReceipt` and a `start-run` outbox command before it
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
The initial watch also receives the Trigger generation's durable activation
timestamp so providers can default to events that occurred after subscription
creation without losing events across daemon restart.
The daemon sends an acknowledgement only after receipt, outbox, and cursor are
committed together. A stream restart therefore replays safely. The bundled
GitHub provider uses polling rather than requiring public ingress.
