# Declarative plugin installations

`PluginInstallation` is the desired-state projection for one immutable plugin
bundle. The bundle store records what has been verified and published; the
resource decides whether that exact version should be active. There is no
second activation configuration path.

```yaml
apiVersion: orchigram.dev/v1alpha1
kind: PluginInstallation
metadata:
  name: exec-0-1-0-0123456789
spec:
  plugin: exec
  version: 0.1.0
  digest: 0123456789abcdef...
  enabled: false
```

`orchigram plugin install` verifies and publishes a bundle, then creates the
one deterministic disabled resource for it. Install never activates a new
version. Existing bundle records from prototype databases are adopted on
startup; an already active legacy version is adopted with `enabled: true` so an
upgrade does not silently disable it.

`orchigram plugin enable NAME VERSION` and `plugin disable NAME` are
compatibility commands over the same resources. They update `spec.enabled`
with compare-and-swap and synchronously invoke the normal controller. Rollback
selects an older immutable resource; neither its bundle nor contract is
overwritten. A disabled resource may be deleted. If its verified bundle still
exists, reconciliation recreates the canonical disabled projection.

Generic ResourceService apply/delete acknowledges the durable desired-state
mutation, not process convergence. Clients use `observedGeneration`, resource
watches, and System health to observe the asynchronous controller result. This
avoids returning an ambiguous RPC failure after the desired-state transaction
has already committed.

Only one version of a plugin may be enabled. If concurrent applies leave two
versions enabled, both selected resources enter `Conflict`; the controller
retains the current activation and never chooses a winner by list or timing
order. The operator resolves the conflict by disabling all but one version.

## Status

The daemon persists status with:

- `observedGeneration`;
- `phase`: `Installed`, `Active`, `Conflict`, or `Error`;
- exact plugin, version, and digest;
- installed and active observations;
- sorted capabilities;
- a `Ready` condition;
- stable, secret-safe diagnostics.

Missing bundles, digest mismatch, incompatible protocol, activation failure,
process loss, and failed health probes remain visible. Status updates advance
the global resource revision and emit `MODIFIED` watch events, but never change
generation. A controller update computed from an older generation is rejected
and retried. Applying an edited GET projection cannot write status, and a spec
apply preserves the current server status until the controller observes the new
generation.

The TUI lists both immutable bundle inventory and `PluginInstallation`
resources. The latter is the authoritative YAML/status view and uses the same
ResourceService get/list/watch history as CLI clients.
