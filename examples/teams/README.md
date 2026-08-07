# Scheduled Teams-compatible notification

This tracer proves the native schedule -> agent -> HTTP path. Install and
enable the first-party `agent-command` and `http` plugin bundles, then apply
the resources in this directory.

The webhook URL is never present in the resource YAML. Export it only in the
daemon service environment as `ORCHIGRAM_TEAMS_WEBHOOK_URL`; the `SecretRef`
stores that environment key, not its value. No real message is sent by tests.
Run a manual verification only after the operator confirms the destination:

```console
orchigram apply -f examples/teams/secret-ref.yaml
orchigram apply -f examples/teams/agent-profile.yaml
orchigram apply -f examples/teams/daily-flow.yaml
orchigram apply -f examples/teams/weekday-trigger.yaml
orchigram run start daily-engineering-note --idempotency-key operator-confirmed-test
```

For a deterministic local test, replace the Codex profile with a `command`
profile that emits one JSONL result and point the SecretRef at an `httptest`
receiver URL.
