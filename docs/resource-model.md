# Resource model

All editable resources use `apiVersion: orchigram.dev/v1alpha1` and one of these kinds: `Flow`, `Trigger`, `Repository`, `AgentProfile`, `PluginInstallation`, or `SecretRef`. `Run` and `TriggerReceipt` are server-owned.

Every resource contains:

```yaml
metadata:
  name: example
  namespace: default
  uid: server-assigned
  resourceVersion: 12
  generation: 3
  labels: {}
spec: {}
status: {}
```

Apply rejects unknown fields. `resourceVersion` is a compare-and-swap precondition; a conflict returns the current document rather than overwriting it. `generation` changes only when `spec` changes. Controllers alone write `status`.

`SecretRef.spec` contains a backend and key/path/environment identifier, never a value. Its status is only `Configured`, `Missing`, or an error category with a redacted diagnostic.

`PluginInstallation` selects one immutable installed name, version, and digest.
Its controller-owned status records observed generation, activation, protocol
and health diagnostics. See [Declarative plugin installations](plugin-installations.md).

List pagination, label selection, durable watch replay, YAML export, and run
filters are defined in [Query and watch contracts](query-contracts.md).
