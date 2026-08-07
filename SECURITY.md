# Security policy

Please report vulnerabilities privately through GitHub Security Advisories once the repository is public. Do not open a public issue for a suspected vulnerability.

Orchigram v0.1 is a single-operator system for trusted plugins. The systemd sandbox and isolated workspaces reduce accidental host damage; they are not a hostile multi-tenant security boundary. Secret values must be supplied by references and must never be placed in resource YAML or diagnostic output.

