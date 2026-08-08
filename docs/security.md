# Security model

Orchigram v0.1 is single-node, single-operator software for trusted plugins.

- The daemon runs as a dedicated non-root user with a hardened systemd namespace.
- The default transport is a mode-restricted Unix socket. Remote access inherits OpenSSH authentication and authorization.
- The daemon opens no network listener by default. Generic webhook ingress is explicit opt-in.
- Secret resources identify environment or file references in v0.1; their values are never ordinary configuration. Additional backends can be added without changing Flow schemas.
- Plugin calls receive only the secrets required for that one operation and a minimal allowlisted environment.
- Public health diagnostics use stable component codes and generic messages;
  raw dependency errors, plugin output, payloads, secrets, filesystem paths,
  and provider coordinates remain restricted to operator-controlled logs.
- Commands are direct argv arrays. User payloads never become shell strings.
- Workspaces isolate repositories from each other but are not a hostile-code sandbox.
- Online backups may be written only below the configured state directory;
  offline restore validates paths, entry types, size limits, and SQLite
  integrity before exposing the restored tree.
- Every release includes SHA-256 checksums, SPDX JSON SBOMs, a dependency
  license inventory, and independent digest-pinned plugin bundles.

Threats deferred beyond v0.1 include hostile multi-tenancy, RBAC, high availability, and container-per-run isolation.
