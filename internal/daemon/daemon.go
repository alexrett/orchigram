// Package daemon owns the lifecycle of the local Orchigram server process.
package daemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/alexrett/orchigram/internal/config"
	"github.com/alexrett/orchigram/internal/engine"
	"github.com/alexrett/orchigram/internal/flow"
	"github.com/alexrett/orchigram/internal/httpingress"
	"github.com/alexrett/orchigram/internal/orchestrator"
	"github.com/alexrett/orchigram/internal/plugincontroller"
	"github.com/alexrett/orchigram/internal/pluginmanager"
	"github.com/alexrett/orchigram/internal/server"
	"github.com/alexrett/orchigram/internal/store"
	triggercontroller "github.com/alexrett/orchigram/internal/trigger"
	"google.golang.org/grpc"
)

// Daemon is a fully initialized single-node server.
type Daemon struct {
	config       config.Config
	store        *store.Store
	engine       *engine.Adapter
	orchestrator *orchestrator.Orchestrator
	plugins      *pluginmanager.Manager
	pluginState  *plugincontroller.Controller
	triggers     *triggercontroller.Controller
	httpIngress  *httpingress.Server
	grpc         *grpc.Server
	listener     net.Listener
	closeOnce    sync.Once
}

// Open initializes state, workflow history, and the private Unix listener.
func Open(ctx context.Context, cfg config.Config, executor engine.TaskExecutor) (*Daemon, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(cfg.StateDir, 0o750); err != nil {
		return nil, fmt.Errorf("create state directory: %w", err)
	}
	if err := os.MkdirAll(cfg.RuntimeDir, 0o750); err != nil {
		return nil, fmt.Errorf("create runtime directory: %w", err)
	}
	if err := prepareSocket(cfg.SocketPath); err != nil {
		return nil, err
	}
	state, err := store.Open(filepath.Join(cfg.StateDir, "orchigram.sqlite"))
	if err != nil {
		return nil, err
	}
	plugins := pluginmanager.NewWithLimits(state, cfg.StateDir, pluginmanager.Limits{
		MaxConcurrentActivities: cfg.Operations.MaxConcurrentActivities,
		MaxAgentProcesses:       cfg.Operations.MaxAgentProcesses,
	})
	pluginsOwnedByDaemon := false
	defer func() {
		if !pluginsOwnedByDaemon {
			plugins.Close()
		}
	}()
	if err := plugins.ReconcileArtifacts(ctx); err != nil {
		_ = state.Close()
		return nil, fmt.Errorf("reconcile plugin artifacts: %w", err)
	}
	if err := plugins.ReconcileContracts(ctx); err != nil {
		_ = state.Close()
		return nil, fmt.Errorf("reconcile plugin contracts: %w", err)
	}
	if executor == nil {
		executor = plugins
	}
	pluginState := plugincontroller.New(state, plugins)
	if err := pluginState.Reconcile(ctx); err != nil {
		_ = state.Close()
		return nil, fmt.Errorf("reconcile plugin installations: %w", err)
	}
	durable, err := engine.Open(ctx, filepath.Join(cfg.StateDir, "workflows.sqlite"), state, executor, engine.OpenOptions{MaxConcurrentActivities: cfg.Operations.MaxConcurrentActivities})
	if err != nil {
		_ = state.Close()
		return nil, err
	}
	var listenConfig net.ListenConfig
	listener, err := listenConfig.Listen(ctx, "unix", cfg.SocketPath)
	if err != nil {
		_ = durable.Close()
		_ = state.Close()
		return nil, fmt.Errorf("listen on unix socket: %w", err)
	}
	if err := os.Chmod(cfg.SocketPath, 0o660); err != nil { //nolint:gosec // Group access is the explicit Unix authorization boundary.
		_ = listener.Close()
		_ = durable.Close()
		_ = state.Close()
		return nil, fmt.Errorf("set unix socket mode: %w", err)
	}
	compiler := flow.NewCompiler(plugins)
	control := orchestrator.New(state, compiler, durable, orchestrator.Options{MaxActiveRuns: cfg.Operations.MaxActiveRuns})
	triggers := triggercontroller.NewController(state, plugins, control)
	grpcServer := grpc.NewServer(
		grpc.MaxRecvMsgSize(2<<20),
		grpc.MaxSendMsgSize(8<<20),
	)
	server.NewAPI(state, compiler, control, durable, plugins, triggers, cfg.StateDir, pluginState).Register(grpcServer)
	ingress, err := httpingress.Listen(ctx, cfg.HTTP.Listen, state, plugins, control)
	if err != nil {
		_ = listener.Close()
		_ = durable.Close()
		_ = state.Close()
		return nil, err
	}
	pluginsOwnedByDaemon = true
	return &Daemon{config: cfg, store: state, engine: durable, orchestrator: control, plugins: plugins, pluginState: pluginState, triggers: triggers, httpIngress: ingress, grpc: grpcServer, listener: listener}, nil
}

// Serve starts reconciliation and serves until context cancellation or failure.
func (d *Daemon) Serve(ctx context.Context) error {
	d.orchestrator.Start(ctx)
	d.pluginState.Start(ctx)
	d.triggers.Start(ctx)
	serveError := make(chan error, 1)
	go func() { serveError <- d.grpc.Serve(d.listener) }()
	var ingressErrors <-chan error
	if d.httpIngress != nil {
		ingressErrors = d.httpIngress.Errors()
	}
	select {
	case <-ctx.Done():
		graceful := make(chan struct{})
		go func() {
			d.grpc.GracefulStop()
			close(graceful)
		}()
		select {
		case <-graceful:
		case <-time.After(3 * time.Second):
			d.grpc.Stop()
		}
		return d.Close()
	case err := <-serveError:
		if errors.Is(err, grpc.ErrServerStopped) {
			return d.Close()
		}
		_ = d.Close()
		return fmt.Errorf("serve gRPC: %w", err)
	case err := <-ingressErrors:
		if err == nil && ctx.Err() != nil {
			return d.Close()
		}
		_ = d.Close()
		return fmt.Errorf("serve webhook ingress: %w", err)
	}
}

// Close releases all daemon resources exactly once.
func (d *Daemon) Close() error {
	var result error
	d.closeOnce.Do(func() {
		if d.listener != nil {
			_ = d.listener.Close()
		}
		if d.httpIngress != nil {
			result = errors.Join(result, d.httpIngress.Close())
		}
		if d.engine != nil {
			result = errors.Join(result, d.engine.Close())
		}
		if d.plugins != nil {
			d.plugins.Close()
		}
		if d.store != nil {
			result = errors.Join(result, d.store.Close())
		}
		if err := os.Remove(d.config.SocketPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			slog.Warn("remove runtime socket", "error", err)
		}
	})
	return result
}

func prepareSocket(path string) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect runtime socket: %w", err)
	}
	if info.Mode()&os.ModeSocket == 0 {
		return fmt.Errorf("refusing to replace non-socket path %s", path)
	}
	if err := os.Remove(path); err != nil {
		return fmt.Errorf("remove stale runtime socket: %w", err)
	}
	return nil
}
