// Package cli implements the kubectl-like Orchigram command surface.
package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	controlv1alpha1 "github.com/alexrett/orchigram/gen/orchigram/control/v1alpha1"
	clientpkg "github.com/alexrett/orchigram/internal/client"
	"github.com/alexrett/orchigram/internal/config"
	"github.com/alexrett/orchigram/internal/contextcfg"
	"github.com/alexrett/orchigram/internal/contexttransport"
	"github.com/alexrett/orchigram/internal/daemon"
	installer "github.com/alexrett/orchigram/internal/install"
	"github.com/alexrett/orchigram/internal/tui"
	"github.com/alexrett/orchigram/internal/version"
	"github.com/google/uuid"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/types/known/emptypb"
	"gopkg.in/yaml.v3"
)

type options struct {
	contextName string
	contexts    string
	socket      string
	output      string
}

// NewRoot constructs the full command tree.
func NewRoot() *cobra.Command {
	opts := &options{}
	root := &cobra.Command{
		Use:           "orchigram",
		Short:         "Declarative agent workflows from the terminal",
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return withClient(cmd.Context(), opts, func(client *clientpkg.Client, contextName string) error {
				contexts, err := contextcfg.Load(opts.contexts)
				if err != nil {
					return err
				}
				if opts.socket != "" {
					contextName = "direct"
					contexts = contextcfg.File{Current: contextName, Contexts: map[string]contextcfg.Context{contextName: {Socket: opts.socket}}}
				}
				return tui.RunWithContexts(cmd.Context(), client, contextName, contexts)
			})
		},
	}
	root.Version = version.String()
	root.SetVersionTemplate("{{.Version}}\n")
	root.PersistentFlags().StringVar(&opts.contextName, "context", "", "named context from contexts.yaml")
	root.PersistentFlags().StringVar(&opts.contexts, "contexts", "", "contexts file (default: XDG config)")
	root.PersistentFlags().StringVar(&opts.socket, "socket", "", "direct Unix socket override")
	root.PersistentFlags().StringVarP(&opts.output, "output", "o", "yaml", "output format: yaml or json")
	root.AddCommand(
		newServerCommand(),
		newApplyCommand(opts),
		newGetCommand(opts),
		newDescribeCommand(opts),
		newDeleteCommand(opts),
		newFlowCommand(opts),
		newRunCommand(opts),
		newTriggerCommand(opts),
		newPluginCommand(opts),
		newContextCommand(opts),
		newInstallCommand(),
	)
	return root
}

func newServerCommand() *cobra.Command {
	var configPath, stateDir, runtimeDir, socketPath string
	command := &cobra.Command{Use: "server", Short: "Run the Orchigram daemon", RunE: func(_ *cobra.Command, _ []string) error {
		cfg, err := config.Load(configPath)
		if err != nil {
			return err
		}
		if stateDir != "" {
			cfg.StateDir = stateDir
		}
		if runtimeDir != "" {
			cfg.RuntimeDir = runtimeDir
		}
		if socketPath != "" {
			cfg.SocketPath = socketPath
		}
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		server, err := daemon.Open(ctx, cfg, nil)
		if err != nil {
			return err
		}
		slog.Info("orchigram daemon ready", "socket", cfg.SocketPath, "state", cfg.StateDir)
		return server.Serve(ctx)
	}}
	command.Flags().StringVar(&configPath, "config", "", "server configuration file")
	command.Flags().StringVar(&stateDir, "state-dir", "", "state directory override")
	command.Flags().StringVar(&runtimeDir, "runtime-dir", "", "runtime directory override")
	command.Flags().StringVar(&socketPath, "socket", "", "Unix socket override")
	return command
}

func newApplyCommand(opts *options) *cobra.Command {
	var file string
	var expected uint64
	var dryRun bool
	command := &cobra.Command{Use: "apply -f FILE", Short: "Apply a strict resource", RunE: func(cmd *cobra.Command, _ []string) error {
		if file == "" {
			return errors.New("-f is required")
		}
		data, err := os.ReadFile(file) //nolint:gosec // The operator explicitly supplies the manifest path.
		if err != nil {
			return err
		}
		return withClient(cmd.Context(), opts, func(client *clientpkg.Client, contextName string) error {
			response, err := client.Resources.Apply(cmd.Context(), &controlv1alpha1.ApplyRequest{Meta: requestMeta(contextName), Document: data, ExpectedResourceVersion: expected, DryRun: dryRun})
			if err != nil {
				return err
			}
			if len(response.GetDiagnostics()) > 0 {
				for _, diagnostic := range response.GetDiagnostics() {
					if _, writeErr := fmt.Fprintf(cmd.ErrOrStderr(), "%s: %s (%s)\n", diagnostic.GetPath(), diagnostic.GetMessage(), diagnostic.GetCode()); writeErr != nil {
						return writeErr
					}
				}
				return errors.New("resource validation failed")
			}
			return printDocument(cmd.OutOrStdout(), response.GetResource().GetJson(), opts.output)
		})
	}}
	command.Flags().StringVarP(&file, "filename", "f", "", "YAML or JSON resource")
	command.Flags().Uint64Var(&expected, "resource-version", 0, "required current revision for update")
	command.Flags().BoolVar(&dryRun, "dry-run", false, "validate without persisting")
	return command
}

func newGetCommand(opts *options) *cobra.Command {
	return &cobra.Command{Use: "get KIND [NAME]", Short: "List or get resources", Args: cobra.RangeArgs(1, 2), RunE: func(cmd *cobra.Command, args []string) error {
		return withClient(cmd.Context(), opts, func(client *clientpkg.Client, _ string) error {
			if len(args) == 2 {
				response, err := client.Resources.Get(cmd.Context(), &controlv1alpha1.GetRequest{Key: &controlv1alpha1.ResourceKey{Kind: canonicalKind(args[0]), Namespace: "default", Name: args[1]}})
				if err != nil {
					return err
				}
				return printDocument(cmd.OutOrStdout(), response.GetJson(), opts.output)
			}
			response, err := client.Resources.List(cmd.Context(), &controlv1alpha1.ListRequest{Kind: canonicalKind(args[0]), Namespace: "default", Limit: 100})
			if err != nil {
				return err
			}
			for _, item := range response.GetResources() {
				if _, writeErr := fmt.Fprintf(cmd.OutOrStdout(), "%s\t%s\t%d\t%d\n", item.GetKey().GetName(), item.GetKey().GetUid(), item.GetResourceVersion(), item.GetGeneration()); writeErr != nil {
					return writeErr
				}
			}
			return nil
		})
	}}
}

func newDescribeCommand(opts *options) *cobra.Command {
	return &cobra.Command{Use: "describe KIND NAME", Short: "Describe a resource", Args: cobra.ExactArgs(2), RunE: func(cmd *cobra.Command, args []string) error {
		return withClient(cmd.Context(), opts, func(client *clientpkg.Client, _ string) error {
			response, err := client.Resources.Get(cmd.Context(), &controlv1alpha1.GetRequest{Key: &controlv1alpha1.ResourceKey{Kind: canonicalKind(args[0]), Namespace: "default", Name: args[1]}})
			if err != nil {
				return err
			}
			if _, writeErr := fmt.Fprintf(cmd.OutOrStdout(), "Name: %s\nUID: %s\nResource version: %d\nGeneration: %d\n\n", response.GetKey().GetName(), response.GetKey().GetUid(), response.GetResourceVersion(), response.GetGeneration()); writeErr != nil {
				return writeErr
			}
			return printDocument(cmd.OutOrStdout(), response.GetJson(), opts.output)
		})
	}}
}

func newDeleteCommand(opts *options) *cobra.Command {
	var expected uint64
	command := &cobra.Command{Use: "delete KIND NAME", Short: "Delete a resource", Args: cobra.ExactArgs(2), RunE: func(cmd *cobra.Command, args []string) error {
		if expected == 0 {
			return errors.New("--resource-version is required")
		}
		return withClient(cmd.Context(), opts, func(client *clientpkg.Client, contextName string) error {
			_, err := client.Resources.Delete(cmd.Context(), &controlv1alpha1.DeleteRequest{Meta: requestMeta(contextName), Key: &controlv1alpha1.ResourceKey{Kind: canonicalKind(args[0]), Namespace: "default", Name: args[1]}, ExpectedResourceVersion: expected})
			return err
		})
	}}
	command.Flags().Uint64Var(&expected, "resource-version", 0, "required current revision")
	return command
}

func newFlowCommand(opts *options) *cobra.Command {
	command := &cobra.Command{Use: "flow", Short: "Validate and inspect flows"}
	var file string
	validate := &cobra.Command{Use: "validate -f FILE", Short: "Compile and validate a Flow", RunE: func(cmd *cobra.Command, _ []string) error {
		if file == "" {
			return errors.New("-f is required")
		}
		data, err := os.ReadFile(file) //nolint:gosec // The operator explicitly supplies the Flow path.
		if err != nil {
			return err
		}
		return withClient(cmd.Context(), opts, func(client *clientpkg.Client, _ string) error {
			response, err := client.Flows.Compile(cmd.Context(), &controlv1alpha1.CompileRequest{Flow: data})
			if err != nil {
				return err
			}
			if len(response.GetDiagnostics()) > 0 {
				for _, diagnostic := range response.GetDiagnostics() {
					if _, writeErr := fmt.Fprintf(cmd.ErrOrStderr(), "%s: %s (%s)\n", diagnostic.GetPath(), diagnostic.GetMessage(), diagnostic.GetCode()); writeErr != nil {
						return writeErr
					}
				}
				return errors.New("flow compilation failed")
			}
			_, err = fmt.Fprintf(cmd.OutOrStdout(), "planHash: %s\n", response.GetPlanHash())
			return err
		})
	}}
	validate.Flags().StringVarP(&file, "filename", "f", "", "Flow YAML or JSON")
	command.AddCommand(validate)
	return command
}

func newRunCommand(opts *options) *cobra.Command {
	command := &cobra.Command{Use: "run", Short: "Start, watch, approve, reject, or cancel runs"}
	var input, idempotencyKey string
	start := &cobra.Command{Use: "start FLOW", Short: "Start a manual run", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		inputJSON, err := readInput(input)
		if err != nil {
			return err
		}
		return withClient(cmd.Context(), opts, func(client *clientpkg.Client, contextName string) error {
			response, err := client.Runs.Start(cmd.Context(), &controlv1alpha1.StartRunRequest{Meta: requestMeta(contextName), Flow: args[0], InputJson: inputJSON, IdempotencyKey: idempotencyKey})
			if err != nil {
				return err
			}
			_, err = fmt.Fprintln(cmd.OutOrStdout(), response.GetUid())
			return err
		})
	}}
	start.Flags().StringVar(&input, "input", "{}", "JSON or @file")
	start.Flags().StringVar(&idempotencyKey, "idempotency-key", "", "stable manual occurrence identity")
	watch := &cobra.Command{Use: "watch RUN", Short: "Watch durable run events", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		return withClient(cmd.Context(), opts, func(client *clientpkg.Client, _ string) error {
			stream, err := client.Runs.WatchEvents(cmd.Context(), &controlv1alpha1.WatchRunRequest{Uid: args[0]})
			if err != nil {
				return err
			}
			for {
				event, err := stream.Recv()
				if err != nil {
					return err
				}
				if _, writeErr := fmt.Fprintf(cmd.OutOrStdout(), "%d\t%s\t%s\t%s\n", event.GetSequence(), event.GetNodeId(), event.GetType(), string(event.GetPayloadJson())); writeErr != nil {
					return writeErr
				}
				if strings.HasPrefix(event.GetType(), "run.") && event.GetType() != "run.accepted" {
					return nil
				}
			}
		})
	}}
	command.AddCommand(start, watch, approvalCommand(opts, "approve"), approvalCommand(opts, "reject"), cancelCommand(opts))
	return command
}

func approvalCommand(opts *options, decision string) *cobra.Command {
	var node, reason string
	command := &cobra.Command{Use: decision + " RUN", Short: strings.ToUpper(decision[:1]) + decision[1:] + " a waiting node", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		if node == "" {
			return errors.New("--node is required")
		}
		return withClient(cmd.Context(), opts, func(client *clientpkg.Client, contextName string) error {
			request := &controlv1alpha1.ApprovalRequest{Meta: requestMeta(contextName), RunUid: args[0], NodeId: node, Reason: reason}
			if decision == "approve" {
				_, err := client.Runs.Approve(cmd.Context(), request)
				return err
			}
			_, err := client.Runs.Reject(cmd.Context(), request)
			return err
		})
	}}
	command.Flags().StringVar(&node, "node", "", "approval node ID")
	command.Flags().StringVar(&reason, "reason", "", "operator reason")
	return command
}

func cancelCommand(opts *options) *cobra.Command {
	var reason string
	command := &cobra.Command{Use: "cancel RUN", Short: "Cancel a run", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		return withClient(cmd.Context(), opts, func(client *clientpkg.Client, contextName string) error {
			_, err := client.Runs.Cancel(cmd.Context(), &controlv1alpha1.CancelRunRequest{Meta: requestMeta(contextName), RunUid: args[0], Reason: reason})
			return err
		})
	}}
	command.Flags().StringVar(&reason, "reason", "operator cancellation", "cancellation reason")
	return command
}

func newTriggerCommand(opts *options) *cobra.Command {
	command := &cobra.Command{Use: "trigger", Short: "Inspect and control triggers", RunE: func(cmd *cobra.Command, _ []string) error { return cmd.Help() }}
	var count uint32
	next := &cobra.Command{Use: "next UID", Short: "Preview the next schedule occurrences", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		return withClient(cmd.Context(), opts, func(client *clientpkg.Client, _ string) error {
			response, err := client.Triggers.Next(cmd.Context(), &controlv1alpha1.NextOccurrencesRequest{Uid: args[0], Count: count})
			if err != nil {
				return err
			}
			for _, occurrence := range response.GetOccurrences() {
				if _, err := fmt.Fprintf(cmd.OutOrStdout(), "%s\t%s\n", occurrence.GetScheduledAt().AsTime().Format(time.RFC3339), occurrence.GetIdentity()); err != nil {
					return err
				}
			}
			return nil
		})
	}}
	next.Flags().Uint32Var(&count, "count", 5, "number of occurrences to preview")
	command.AddCommand(next, triggerMutationCommand(opts, true), triggerMutationCommand(opts, false))
	return command
}

func triggerMutationCommand(opts *options, enabled bool) *cobra.Command {
	operation := "disable"
	if enabled {
		operation = "enable"
	}
	return &cobra.Command{Use: operation + " UID", Short: strings.ToUpper(operation[:1]) + operation[1:] + " a trigger", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		return withClient(cmd.Context(), opts, func(client *clientpkg.Client, _ string) error {
			request := &controlv1alpha1.TriggerRequest{Uid: args[0]}
			if enabled {
				_, err := client.Triggers.Enable(cmd.Context(), request)
				return err
			}
			_, err := client.Triggers.Disable(cmd.Context(), request)
			return err
		})
	}}
}

func newPluginCommand(opts *options) *cobra.Command {
	command := &cobra.Command{Use: "plugin", Short: "Install and inspect plugins", RunE: func(cmd *cobra.Command, _ []string) error { return cmd.Help() }}
	install := &cobra.Command{Use: "install BUNDLE", Short: "Upload and verify an immutable plugin bundle", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		bundle, err := os.ReadFile(filepath.Clean(args[0])) //nolint:gosec // The operator explicitly supplies the bundle path.
		if err != nil {
			return err
		}
		return withClient(cmd.Context(), opts, func(client *clientpkg.Client, contextName string) error {
			stream, err := client.Plugins.Install(cmd.Context())
			if err != nil {
				return err
			}
			const chunkSize = 1 << 20
			for offset := 0; offset < len(bundle); offset += chunkSize {
				end := min(offset+chunkSize, len(bundle))
				if err := stream.Send(&controlv1alpha1.PluginUploadRequest{Meta: requestMeta(contextName), BundleChunk: bundle[offset:end], Final: end == len(bundle)}); err != nil {
					return err
				}
			}
			response, err := stream.CloseAndRecv()
			if err != nil {
				return err
			}
			_, err = fmt.Fprintf(cmd.OutOrStdout(), "%s\t%s\t%s\n", response.GetName(), response.GetVersion(), response.GetDigest())
			return err
		})
	}}
	list := &cobra.Command{Use: "list", Short: "List installed plugin versions", RunE: func(cmd *cobra.Command, _ []string) error {
		return withClient(cmd.Context(), opts, func(client *clientpkg.Client, _ string) error {
			response, err := client.Plugins.List(cmd.Context(), &emptypb.Empty{})
			if err != nil {
				return err
			}
			for _, item := range response.GetPlugins() {
				if _, err := fmt.Fprintf(cmd.OutOrStdout(), "%s\t%s\t%s\t%s\n", item.GetName(), item.GetVersion(), item.GetState(), item.GetDigest()); err != nil {
					return err
				}
			}
			return nil
		})
	}}
	command.AddCommand(install, list, pluginMutationCommand(opts, "enable"), pluginMutationCommand(opts, "disable"), pluginDoctorCommand(opts))
	return command
}

func pluginMutationCommand(opts *options, operation string) *cobra.Command {
	use := operation + " NAME"
	if operation == "enable" {
		use += " VERSION"
	}
	arguments := cobra.ExactArgs(1)
	if operation == "enable" {
		arguments = cobra.ExactArgs(2)
	}
	return &cobra.Command{Use: use, Short: strings.ToUpper(operation[:1]) + operation[1:] + " a plugin", Args: arguments, RunE: func(cmd *cobra.Command, args []string) error {
		return withClient(cmd.Context(), opts, func(client *clientpkg.Client, _ string) error {
			request := &controlv1alpha1.PluginRequest{Name: args[0]}
			if len(args) == 2 {
				request.Version = args[1]
			}
			if operation == "enable" {
				_, err := client.Plugins.Enable(cmd.Context(), request)
				return err
			}
			_, err := client.Plugins.Disable(cmd.Context(), request)
			return err
		})
	}}
}

func pluginDoctorCommand(opts *options) *cobra.Command {
	return &cobra.Command{Use: "doctor NAME [VERSION]", Short: "Verify plugin integrity, negotiation, and health", Args: cobra.RangeArgs(1, 2), RunE: func(cmd *cobra.Command, args []string) error {
		return withClient(cmd.Context(), opts, func(client *clientpkg.Client, _ string) error {
			request := &controlv1alpha1.PluginRequest{Name: args[0]}
			if len(args) == 2 {
				request.Version = args[1]
			}
			response, err := client.Plugins.Doctor(cmd.Context(), request)
			if err != nil {
				return err
			}
			for _, diagnostic := range response.GetDiagnostics() {
				if _, err := fmt.Fprintf(cmd.ErrOrStderr(), "%s: %s (%s)\n", diagnostic.GetPath(), diagnostic.GetMessage(), diagnostic.GetCode()); err != nil {
					return err
				}
			}
			if len(response.GetDiagnostics()) > 0 {
				return errors.New("plugin doctor failed")
			}
			_, err = fmt.Fprintln(cmd.OutOrStdout(), "ok")
			return err
		})
	}}
}

func newContextCommand(opts *options) *cobra.Command {
	command := &cobra.Command{Use: "context", Short: "Manage local and SSH contexts"}
	get := &cobra.Command{Use: "get", Short: "List contexts", RunE: func(cmd *cobra.Command, _ []string) error {
		file, err := contextcfg.Load(opts.contexts)
		if err != nil {
			return err
		}
		names := make([]string, 0, len(file.Contexts))
		for name := range file.Contexts {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			contextValue := file.Contexts[name]
			marker := " "
			if name == file.Current {
				marker = "*"
			}
			transport := contextValue.Socket
			if contextValue.SSH != nil {
				transport = "ssh:" + contextValue.SSH.Destination
			}
			if _, writeErr := fmt.Fprintf(cmd.OutOrStdout(), "%s %s\t%s\n", marker, name, transport); writeErr != nil {
				return writeErr
			}
		}
		return nil
	}}
	use := &cobra.Command{Use: "use NAME", Short: "Select the current context", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		file, err := contextcfg.Load(opts.contexts)
		if err != nil {
			return err
		}
		if _, exists := file.Contexts[args[0]]; !exists {
			return fmt.Errorf("context %q is not defined", args[0])
		}
		file.Current = args[0]
		if err := contextcfg.Save(opts.contexts, file); err != nil {
			return err
		}
		_, err = fmt.Fprintln(cmd.OutOrStdout(), args[0])
		return err
	}}
	var socket, destination, remoteSocket, identity string
	set := &cobra.Command{Use: "set NAME", Short: "Create or update a local or SSH context", Args: cobra.ExactArgs(1), RunE: func(_ *cobra.Command, args []string) error {
		if (socket == "") == (destination == "") {
			return errors.New("configure exactly one of --socket or --ssh-destination")
		}
		file, err := contextcfg.Load(opts.contexts)
		if err != nil {
			return err
		}
		if file.Contexts == nil {
			file.Contexts = map[string]contextcfg.Context{}
		}
		selected := contextcfg.Context{Socket: socket}
		if destination != "" {
			if remoteSocket == "" {
				remoteSocket = "/run/orchigram/orchigram.sock"
			}
			selected = contextcfg.Context{SSH: &contextcfg.SSHContext{Destination: destination, Socket: remoteSocket, Identity: identity}}
		}
		file.Contexts[args[0]] = selected
		if file.Current == "" {
			file.Current = args[0]
		}
		return contextcfg.Save(opts.contexts, file)
	}}
	set.Flags().StringVar(&socket, "socket", "", "local Unix socket")
	set.Flags().StringVar(&destination, "ssh-destination", "", "OpenSSH destination, for example operator@host")
	set.Flags().StringVar(&remoteSocket, "remote-socket", "/run/orchigram/orchigram.sock", "remote Unix socket")
	set.Flags().StringVar(&identity, "identity", "", "optional SSH identity file")
	command.AddCommand(get, use, set)
	return command
}

func newInstallCommand() *cobra.Command {
	var pluginDir, root string
	var noStart bool
	command := &cobra.Command{Use: "install", Short: "Install the hardened local systemd service", RunE: func(cmd *cobra.Command, _ []string) error {
		result, err := installer.Run(cmd.Context(), installer.Options{Root: root, PluginDir: pluginDir, Start: !noStart})
		if err != nil {
			return err
		}
		if _, err := fmt.Fprintf(cmd.OutOrStdout(), "binary: %s\nunit: %s\nsocket: %s\n", result.Binary, result.Unit, result.Socket); err != nil {
			return err
		}
		for _, warning := range result.Warnings {
			if _, err := fmt.Fprintln(cmd.ErrOrStderr(), "warning:", warning); err != nil {
				return err
			}
		}
		return nil
	}}
	command.Flags().StringVar(&pluginDir, "plugin-dir", "", "directory containing the four first-party plugin executables")
	command.Flags().StringVar(&root, "root", "/", "installation root (used for image construction and tests)")
	command.Flags().BoolVar(&noStart, "no-start", false, "write installation files without starting systemd")
	return command
}

func withClient(ctx context.Context, opts *options, fn func(*clientpkg.Client, string) error) error {
	contextName := opts.contextName
	socketPath := opts.socket
	if socketPath == "" {
		contexts, err := contextcfg.Load(opts.contexts)
		if err != nil {
			return err
		}
		if contextName == "" {
			contextName = contexts.Current
		}
		selected, ok := contexts.Contexts[contextName]
		if !ok {
			return fmt.Errorf("context %q is not defined", contextName)
		}
		connection, err := contexttransport.Connect(ctx, selected)
		if err != nil {
			return err
		}
		defer func() { _ = connection.Close() }()
		return fn(connection.Client, contextName)
	}
	dialCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	connection, err := contexttransport.Connect(dialCtx, contextcfg.Context{Socket: socketPath})
	if err != nil {
		return err
	}
	defer func() { _ = connection.Close() }()
	return fn(connection.Client, contextName)
}

func requestMeta(contextName string) *controlv1alpha1.RequestMeta {
	return &controlv1alpha1.RequestMeta{RequestId: uuid.NewString(), Context: contextName}
}

func printDocument(writer io.Writer, data []byte, format string) error {
	if format == "json" {
		var value any
		if err := json.Unmarshal(data, &value); err != nil {
			return err
		}
		encoded, err := json.MarshalIndent(value, "", "  ")
		if err != nil {
			return err
		}
		_, err = fmt.Fprintln(writer, string(encoded))
		return err
	}
	if format != "yaml" {
		return fmt.Errorf("unsupported output format %q", format)
	}
	var value any
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	encoded, err := yaml.Marshal(value)
	if err != nil {
		return err
	}
	_, err = writer.Write(encoded)
	return err
}

func readInput(value string) ([]byte, error) {
	if strings.HasPrefix(value, "@") {
		path := strings.TrimPrefix(value, "@")
		data, err := os.ReadFile(filepath.Clean(path)) //nolint:gosec // The operator explicitly supplies the input path.
		if err != nil {
			return nil, err
		}
		if !json.Valid(data) {
			return nil, errors.New("input file is not valid JSON")
		}
		return data, nil
	}
	data := []byte(value)
	if !json.Valid(data) {
		return nil, errors.New("input is not valid JSON")
	}
	return data, nil
}

func canonicalKind(value string) string {
	switch strings.ToLower(value) {
	case "flow", "flows":
		return "Flow"
	case "trigger", "triggers":
		return "Trigger"
	case "repository", "repositories", "repo", "repos":
		return "Repository"
	case "agentprofile", "agentprofiles", "agent":
		return "AgentProfile"
	case "plugininstallation", "plugininstallations":
		return "PluginInstallation"
	case "secretref", "secretrefs", "secret":
		return "SecretRef"
	default:
		return value
	}
}
