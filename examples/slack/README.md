# Scheduled Slack reminder

This tracer proves the native schedule -> agent -> HTTP path without a
Slack-specific plugin or public resource schema. Install and enable the
first-party `agent-command` and `http` plugin bundles, then apply the resources
in dependency order:

```console
orchigram apply -f examples/slack/secret-ref.yaml
orchigram apply -f examples/slack/agent-profile.yaml
orchigram apply -f examples/slack/weekday-flow.yaml
orchigram apply -f examples/slack/weekday-trigger.yaml
```

The webhook URL is absent from every resource. Put it only in the protected
daemon service environment as `ORCHIGRAM_SLACK_WEBHOOK_URL`; the `SecretRef`
stores the environment key, never the credential. Do not put the URL in shell
history, resource YAML, diagnostics, or an agent prompt.

## Deterministic local acceptance

The repository test replaces the shipped read-only Codex profile with a
deterministic `command` profile and points the same shipped Flow at an
`httptest` receiver. It covers `200 ok`, a `503` followed by `200 ok`, retry
exhaustion, payload shape, stable idempotency headers, and secret
non-disclosure:

```console
go test ./internal/daemon -run TestSlackWeekdayFlowAcceptance -count=1
```

This local receiver is the required repeatable checkpoint; it sends nothing to
Slack. Successful Incoming Webhooks normally return `HTTP 200` with body `ok`.
Any other status is a failed attempt and follows the Flow's bounded retry
policy.

## Optional real Slack checkpoint

A real post remains optional until an operator supplies a sandbox Incoming
Webhook through the daemon's protected SecretRef backend and explicitly
confirms the destination. After that confirmation, start exactly one manual
run with a unique operator-chosen occurrence key:

```console
orchigram run start weekday-engineering-reminder --idempotency-key operator-confirmed-slack-test
```

Slack documents Incoming Webhooks at
<https://api.slack.com/incoming-webhooks> and message payloads at
<https://docs.slack.dev/messaging/>. Incoming Webhooks are limited to roughly
one message per second, with short bursts tolerated. A `429` response includes
`Retry-After`; this example deliberately serializes work with `maxParallel: 1`
and retries only three times.

Orchigram reuses the same `Idempotency-Key` across retries. Incoming Webhooks,
however, expose no documented deduplication identifier. Therefore, if Slack
accepts a request and the daemon crashes before recording completion, replay
can post a duplicate. This is an inference from the Incoming Webhook contract;
operators must treat delivery as at-least-once.
