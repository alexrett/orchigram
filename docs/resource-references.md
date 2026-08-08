# Resource references and readiness

Orchigram resolves desired-resource dependencies on the daemon. The CLI and
TUI do not implement a second reference model. References are namespace-local:
a resource in `team-a` cannot silently bind a same-named `SecretRef`, `Flow`,
`Repository`, or `AgentProfile` from `default`.

The v0.1 reference set is:

- `Trigger.spec.flow` to `Flow`;
- `Trigger.spec.webhook.bearerSecretRef` to `SecretRef`;
- `Trigger.spec.provider.plugin` to one active immutable trigger contract and
  each entry in `provider.config.secretRefs` to `SecretRef`;
- `Repository.spec.authSecretRef` to `SecretRef`;
- `AgentProfile.spec.secretRefs`, including `ENV_NAME=resource-name` aliases,
  to `SecretRef`;
- action-owned Flow references resolved while compiling the immutable plan.

Apply and validate return stable path/code diagnostics and do not store a new
desired resource while one of these required references is absent or
incompatible. Existing resources can become temporarily unresolved after an
operator deletes or disables a dependency. GET, LIST, and emitted WATCH
documents then carry a server-owned `status.conditions[type=Ready]` projection
and redacted diagnostics. Applying a document never accepts client-written
status, and export contains desired configuration rather than projected status.

Secret resolution has two distinct stages. Compilation and reference
validation read only `SecretRef` metadata and backend coordinates. A task,
webhook, or provider watch resolves the value immediately before use in the
same namespace. The value is never placed in a diagnostic, status, plan, event,
or debug representation.

Every newly compiled plan pins its resource namespace on each node. A legacy
prototype plan that lacks both a pinned resource binding and a namespace fails
closed; the runtime never guesses `default` for an ambiguous profile,
repository, or secret.

Provider config contains the host-only `secretRefs` map. The daemon validates
those references, removes the map, and then validates the remaining config
against the immutable provider schema. Every emitted provider payload is
validated against the corresponding event schema before the controller can
persist or acknowledge it.

Reference validation during apply is an operator feedback boundary. Trigger
acceptance is the durability boundary: the current Trigger generation, enabled
state, Trigger-to-Flow target, and compiled Flow UID/generation are checked
inside the same SQLite transaction that writes the receipt, immutable plan,
provider cursor, and outbox command. A concurrent update or delete therefore
either orders after a successful acceptance or rejects the stale acceptance
without a partial record. Replaying an already accepted provider occurrence
still returns its existing receipt under the current Trigger generation.
