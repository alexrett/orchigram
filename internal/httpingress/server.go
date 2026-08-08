// Package httpingress implements the explicit opt-in generic webhook listener.
package httpingress

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/alexrett/orchigram/internal/resource"
	"github.com/alexrett/orchigram/internal/store"
	"github.com/google/uuid"
)

const maxBody = 1 << 20

// SecretResolver returns values without exposing them through resource APIs.
type SecretResolver interface {
	ResolveSecret(context.Context, string, string) ([]byte, error)
}

// Acceptor owns compilation and the durable receipt/plan/outbox boundary.
type Acceptor interface {
	AcceptTrigger(context.Context, string, uint64, string, string, string, json.RawMessage, bool) (store.Receipt, error)
}

// Server owns the one explicitly configured HTTP listener.
type Server struct {
	state    *store.Store
	acceptor Acceptor
	secrets  SecretResolver
	server   *http.Server
	listener net.Listener
	errors   chan error
}

// Listen binds the operator-selected address. It is never called for an empty address.
func Listen(ctx context.Context, address string, state *store.Store, secrets SecretResolver, acceptors ...Acceptor) (*Server, error) {
	if strings.TrimSpace(address) == "" {
		return nil, nil
	}
	var acceptor Acceptor = state
	if len(acceptors) > 0 && acceptors[0] != nil {
		acceptor = acceptors[0]
	}
	handler := &Server{state: state, acceptor: acceptor, secrets: secrets, errors: make(chan error, 1)}
	handler.server = &http.Server{
		Addr: address, Handler: handler, ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 30 * time.Second,
	}
	var listenConfig net.ListenConfig
	listener, err := listenConfig.Listen(ctx, "tcp", address)
	if err != nil {
		return nil, fmt.Errorf("listen for webhooks: %w", err)
	}
	handler.listener = listener
	go func() {
		err := handler.server.Serve(listener)
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		handler.errors <- err
	}()
	go func() {
		<-ctx.Done()
		shutdownContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = handler.server.Shutdown(shutdownContext)
		cancel()
	}()
	return handler, nil
}

// Errors reports the terminal Serve outcome.
func (s *Server) Errors() <-chan error { return s.errors }

// Address returns the bound address, useful when tests request port zero.
func (s *Server) Address() string { return s.listener.Addr().String() }

// Close stops accepting webhooks and waits only for bounded HTTP shutdown.
func (s *Server) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.server.Shutdown(ctx)
}

// ServeHTTP accepts one durable generic hook.
func (s *Server) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		writeError(writer, http.StatusMethodNotAllowed, "method_not_allowed")
		return
	}
	uid := strings.TrimPrefix(request.URL.Path, "/v1/hooks/")
	if uid == request.URL.Path || uid == "" || strings.Contains(uid, "/") {
		writeError(writer, http.StatusNotFound, "hook_not_found")
		return
	}
	document, err := s.state.ResourceByUID(request.Context(), "Trigger", uid)
	if err != nil {
		writeError(writer, http.StatusNotFound, "hook_not_found")
		return
	}
	trigger, err := resource.DecodeTrigger(document.JSON)
	if err != nil || trigger.Spec.Webhook == nil || (trigger.Spec.Enabled != nil && !*trigger.Spec.Enabled) {
		writeError(writer, http.StatusNotFound, "hook_not_found")
		return
	}
	enabled := trigger.Spec.Enabled == nil || *trigger.Spec.Enabled
	state, err := s.state.EnsureTriggerState(request.Context(), trigger.Metadata.UID, trigger.Metadata.Generation, enabled, time.Now())
	if err != nil {
		writeError(writer, http.StatusServiceUnavailable, "hook_state_unavailable")
		return
	}
	if !state.Enabled {
		writeError(writer, http.StatusNotFound, "hook_not_found")
		return
	}
	secret, err := s.secrets.ResolveSecret(request.Context(), trigger.Metadata.Namespace, trigger.Spec.Webhook.BearerSecretRef)
	if err != nil {
		writeError(writer, http.StatusServiceUnavailable, "hook_secret_unavailable")
		return
	}
	provided := strings.TrimPrefix(request.Header.Get("Authorization"), "Bearer ")
	if provided == "" || subtle.ConstantTimeCompare([]byte(provided), secret) != 1 {
		writeError(writer, http.StatusUnauthorized, "unauthorized")
		return
	}
	body, err := io.ReadAll(io.LimitReader(request.Body, maxBody+1))
	if err != nil {
		writeError(writer, http.StatusBadRequest, "body_unreadable")
		return
	}
	if len(body) > maxBody {
		writeError(writer, http.StatusRequestEntityTooLarge, "body_too_large")
		return
	}
	if !json.Valid(body) {
		writeError(writer, http.StatusBadRequest, "invalid_json")
		return
	}
	idempotencyKey := strings.TrimSpace(request.Header.Get("Idempotency-Key"))
	deduplicated := idempotencyKey != ""
	if len(idempotencyKey) > 256 {
		writeError(writer, http.StatusBadRequest, "idempotency_key_too_long")
		return
	}
	if idempotencyKey == "" {
		idempotencyKey = uuid.NewString()
	}
	receipt, err := s.acceptor.AcceptTrigger(request.Context(), trigger.Metadata.UID, trigger.Metadata.Generation, "webhook:"+idempotencyKey, trigger.Spec.Flow, trigger.Metadata.Namespace, body, deduplicated)
	if err != nil {
		if unavailableTrigger(err) {
			writeError(writer, http.StatusNotFound, "hook_not_found")
			return
		}
		writeError(writer, http.StatusInternalServerError, "persistence_failed")
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(writer).Encode(map[string]any{"receiptUID": receipt.UID, "runUID": receipt.RunUID, "duplicate": receipt.Existing, "deduplicated": receipt.Deduplicated})
}

func unavailableTrigger(err error) bool {
	var staleFlow *store.StaleFlowPlanError
	var staleTrigger *store.StaleTriggerGenerationError
	var changedReference *store.TriggerReferenceChangedError
	return errors.Is(err, store.ErrNotFound) || errors.Is(err, store.ErrTriggerDisabled) ||
		errors.As(err, &staleFlow) || errors.As(err, &staleTrigger) || errors.As(err, &changedReference)
}

func writeError(writer http.ResponseWriter, statusCode int, code string) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(statusCode)
	_ = json.NewEncoder(writer).Encode(map[string]string{"error": code})
}
