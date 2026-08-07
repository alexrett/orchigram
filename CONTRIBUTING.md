# Contributing

Orchigram accepts focused changes backed by tests and an explanation of durability and security effects.

1. Discuss large public API or resource-schema changes before implementation.
2. Add or update protobuf and schema tests for contract changes.
3. Preserve stable idempotency keys across retries.
4. Run `make check` and `make cross-build`.
5. Never include credentials, internal prompts, private URLs, or organization-specific adapters.

Protocol breaking changes are not accepted within a published API version. Add a new version instead.

