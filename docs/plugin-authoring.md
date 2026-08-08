# Plugin authoring

Community plugins are ordinary Go modules built outside the Orchigram source
tree. The supported boundary is `github.com/alexrett/orchigram/sdk/plugin`;
plugin code must not import an Orchigram `internal` package. A complete example
is in [`examples/plugins/echo`](../examples/plugins/echo).

## Create a task plugin

Initialize a separate module and add the SDK:

```console
mkdir orchigram-plugin-echo && cd orchigram-plugin-echo
go mod init example.com/orchigram-plugin-echo
go get github.com/alexrett/orchigram/sdk/plugin
```

A handler implements `ValidateAction` and `Execute`. Validation returns SDK
diagnostics. Execution receives `plugin.TaskRequest` and may emit only
non-terminal events through `plugin.EventSink`:

```go
type handler struct{}

func (handler) ValidateAction(_ context.Context, action string, config json.RawMessage) []plugin.ValidationIssue {
	if action != "echo.echo" {
		return []plugin.ValidationIssue{{Path: "action", Code: "unsupported", Message: "expected echo.echo"}}
	}
	return nil
}

func (handler) Execute(ctx context.Context, request plugin.TaskRequest, sink plugin.EventSink) (any, error) {
	var input struct{ Message string `json:"message"` }
	if err := json.Unmarshal(request.Input, &input); err != nil { return nil, err }
	_ = sink.Emit("echo.progress", map[string]int{"percent": 100})
	return map[string]string{"message": input.Message}, nil
}

func main() {
	plugin.Serve(plugin.Config{
		Metadata: plugin.Metadata{
			Name: "echo", Version: "0.1.0",
			Capabilities: []string{"task.echo.echo"},
			Actions: []plugin.ActionDescriptor{{
				Action: "echo.echo",
				ConfigSchema: json.RawMessage(`{"type":"object","additionalProperties":false}`),
				InputSchema: json.RawMessage(`{"type":"object","properties":{"message":{"type":"string"}},"required":["message"],"additionalProperties":false}`),
				OutputSchema: json.RawMessage(`{"type":"object","properties":{"message":{"type":"string"}},"required":["message"],"additionalProperties":false}`),
			}},
		},
		Task: handler{},
	})
}
```

Metadata names, strict semantic versions, capabilities, and action-specific
Draft 2020-12 config/input/output schemas are validated before serving. Every
declared task action requires exactly one descriptor; agent runtimes declare
`<plugin-name>.run`. Missing, duplicate, malformed, externally referenced, or
capability-mismatched descriptors fail before the process serves. Capability
namespaces are limited to `task`, `trigger`, and `agent`; task capabilities must
use `task.<plugin-name>.<action>`. Every request includes action,
input/config JSON, operation-scoped secrets, request/run/node/attempt identity,
a stable idempotency key, and a deadline. Honor `ctx.Done()` promptly. External
effects are at-least-once, so reconcile repeated idempotency keys instead of
duplicating effects. Never emit or persist secret values.

The SDK negotiates protocol v1, assigns timestamps and gap-free sequences,
rejects author terminal events, emits exactly one `task.completed` or
`task.failed`, maps `Cancel` to the active handler context, and drains active
work only until the shutdown deadline. It applies the complete declared schema
to config and input before calling a task handler and validates the handler's
terminal output before publishing it. Advanced trigger or agent providers may
implement the public generated `gen/orchigram/plugin/v1alpha1`
`TriggerProviderServer` or `AgentRuntimeServer` and pass it in `Config.Trigger`
or `Config.Agent`. Trigger watches participate in the same shutdown admission,
cancellation, and drain accounting. Daemon, resource, bundle, and
workflow-engine internals are not public APIs.

The daemon canonicalizes and stores the negotiated contract with the immutable
plugin installation. Enable and later process restarts must reproduce the same
contract digest. Flow compilation pins the selected action schemas into the
execution plan; activation changes cannot alter an accepted run. The supported
static-analysis subset and warning behavior are documented in
[`flow-contracts.md`](flow-contracts.md).

A provider that honors `WatchStart.activated_at` for safe empty-cursor
bootstrap must declare `trigger.bootstrap.activation-fence`. The host fails
closed without that capability unless a durable cursor already exists or the
operator explicitly requests historical replay. Providers should use stable
event identities and a bounded overlap when their source timestamps are less
precise than the daemon clock.

## Build and pack

Build each target directly and list its path relative to `plugin.yaml`. Author
manifests omit `sha256`; the packer calculates and embeds it:

```yaml
apiVersion: orchigram.dev/plugin/v1alpha1
name: echo
version: 0.1.0
protocol: {minimum: 1, maximum: 1}
capabilities: [task.echo.echo]
platforms:
  - {os: linux, arch: amd64, path: bin/echo-linux-amd64}
  - {os: darwin, arch: arm64, path: bin/echo-darwin-arm64}
```

```console
GOOS=linux GOARCH=amd64 go build -trimpath -o bin/echo-linux-amd64 .
GOOS=darwin GOARCH=arm64 go build -trimpath -o bin/echo-darwin-arm64 .
orchigram plugin pack --manifest plugin.yaml --output dist/echo-0.1.0.tar.gz
orchigram plugin install dist/echo-0.1.0.tar.gz
orchigram plugin enable echo 0.1.0
orchigram plugin doctor echo 0.1.0
```

Packing is local and requires no daemon connection. It rejects absolute or
traversing paths, symlinks and non-regular files, duplicate targets, unknown
manifest fields, oversized inputs, and mismatched supplied digests. Archive
ordering and tar/gzip metadata are canonical. Existing outputs are refused even
when byte-identical; `--force` is required for atomic replacement. The command
prints the final bundle SHA-256 and absolute path.
