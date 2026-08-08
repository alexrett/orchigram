# Contributing

Orchigram accepts focused changes backed by tests and an explanation of durability and security effects.

1. Discuss large public API or resource-schema changes before implementation.
2. Add or update protobuf and schema tests for contract changes.
3. Preserve stable idempotency keys across retries.
4. Run `make check` and `make cross-build`.
5. Never include credentials, internal prompts, private URLs, or organization-specific adapters.
6. Build community plugin examples only against `sdk/plugin`; verify an out-of-tree module with `GOWORK=off`.

Protocol breaking changes are not accepted within a published API version. Add a new version instead.

Repository self-operation is documented in
[`examples/self-sdlc`](examples/self-sdlc/README.md). It always uses an issue
branch and reviewed pull request: no direct default-branch push, automatic
merge, embedded credential, or machine-specific address is accepted.
