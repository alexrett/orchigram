# Plugin authoring

Plugins are deterministic `.tar.gz` bundles containing `plugin.yaml` and one binary per supported platform. Installation checks both the bundle digest and the selected binary digest, negotiates the running plugin, and writes an owner-only immutable version directory. Activation changes the SQLite activation record and never overwrites a previous version.

```yaml
apiVersion: orchigram.dev/plugin/v1alpha1
name: example
version: 0.1.0
protocol: {minimum: 1, maximum: 1}
capabilities: [task.example.run]
platforms:
  - os: linux
    arch: amd64
    path: bin/example_linux_amd64
    sha256: <64 lowercase hex characters>
```

HashiCorp go-plugin provides process bootstrap, protocol negotiation, AutoMTLS, log routing, and cleanup. Protobuf/gRPC in `api/orchigram/plugin/v1alpha1` is the business protocol.

Every call includes request, run, node, attempt, deadline, and idempotency identity. A plugin must:

- emit monotonically increasing stream sequence numbers;
- stop accepting work after shutdown begins;
- implement cancel where advertised;
- avoid logging secret maps or protobuf requests containing them;
- tolerate duplicate requests with the same idempotency key;
- report whether its remote provider honors idempotency.

The conformance suite covers normal completion, cancellation, timeout, crash, incompatible protocol, malformed streams, and duplicate delivery.

The daemon starts plugins with `SkipHostEnv` and AutoMTLS. A first-party command receives only a small platform baseline (`PATH`, `HOME`, temporary-directory and CA variables), declared non-secret environment values, and operation-scoped `SecretRef` values. Commands are argv arrays; Orchigram never evaluates them through a shell.
