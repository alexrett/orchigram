# Orchigram

Orchigram is a local-first, declarative agent workflow orchestrator with a k9s-style terminal interface. It runs as one non-root daemon, stores state in SQLite, and is operated locally or through OpenSSH Unix-socket forwarding. There is no web control plane and no Orchigram-specific user authentication system.

> Status: v0.1 is under active development. The repository is intentionally kept local until the publication gates pass.

## Shape of the product

- `orchigram` opens the TUI.
- `orchigram server` runs the daemon.
- `orchigram apply/get/describe/delete` manage strict YAML resources.
- `orchigram run`, `flow`, `trigger`, `plugin`, and `context` provide scriptable operations.
- First-party agent, exec, HTTP, and GitHub providers are independent gRPC plugin executables.

The daemon binds only `/run/orchigram/orchigram.sock` by default. Optional webhook ingress must be explicitly configured and should normally sit behind operator-owned ingress.

## Development

Requirements: Go 1.26, protoc, and Buf.

```console
make generate
make check
make cross-build
```

See [Operator guide](docs/operator-guide.md), [Architecture](docs/architecture.md), [Durability](docs/durability.md), [Triggers](docs/triggers.md), [GitHub SDLC tracer](docs/github-sdlc.md), [Security](docs/security.md), and [Plugin authoring](docs/plugin-authoring.md).

## License

Apache License 2.0.
