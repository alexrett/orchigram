# Contributor instructions

- All product UI, diagnostics, code, and documentation are English-only.
- Never commit secrets, private prompts, company identifiers, server addresses, or credentials.
- Public resource schemas must not expose framework-specific or plugin implementation types.
- External effects are at-least-once. Every mutation must receive a stable idempotency key.
- Use direct argv execution; never interpolate user data into a shell command.
- Keep the daemon network-closed by default. Human access is through the Unix socket or OpenSSH forwarding.
- Run `make check` before considering a change complete.

