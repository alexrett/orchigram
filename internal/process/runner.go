// Package process runs trusted configured argv without shell expansion and
// owns process-group cancellation for plugin activities.
package process

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"sync"
	"time"
)

const defaultCaptureLimit = 4 << 20

// Spec is a fully constructed command. No field is interpreted by a shell.
type Spec struct {
	Executable    string
	Args          []string
	Directory     string
	Environment   []string
	Stdin         []byte
	TerminateWait time.Duration
	CaptureLimit  int
}

// Output is one raw stdout or stderr chunk.
type Output struct {
	Stream string
	Data   []byte
}

// Result records the observable process outcome.
type Result struct {
	ExitCode int
	Stdout   []byte
	Stderr   []byte
	Outcome  string
}

// ExitError reports a completed process with a non-zero status.
type ExitError struct{ Result Result }

func (e *ExitError) Error() string {
	return fmt.Sprintf("process exited with status %d", e.Result.ExitCode)
}

type activeProcess struct {
	command *exec.Cmd
	done    chan struct{}
	once    sync.Once
	mu      sync.RWMutex
	outcome string
	wait    time.Duration
}

// Runner tracks active commands so cancellation RPCs can target one request.
type Runner struct {
	mu     sync.RWMutex
	active map[string]*activeProcess
}

// NewRunner returns an empty process registry.
func NewRunner() *Runner { return &Runner{active: map[string]*activeProcess{}} }

// Run executes one argv and streams serialized raw output to emit.
func (r *Runner) Run(ctx context.Context, requestID string, spec Spec, emit func(Output) error) (Result, error) {
	if requestID == "" {
		return Result{}, errors.New("request ID is required")
	}
	if spec.Executable == "" {
		return Result{}, errors.New("executable is required")
	}
	if spec.TerminateWait <= 0 {
		spec.TerminateWait = 2 * time.Second
	}
	if spec.CaptureLimit <= 0 {
		spec.CaptureLimit = defaultCaptureLimit
	}
	command := exec.CommandContext(context.WithoutCancel(ctx), spec.Executable, spec.Args...) //nolint:gosec // Trusted declarative argv is the explicit purpose of this boundary; no shell is used.
	command.Dir = spec.Directory
	command.Env = append([]string(nil), spec.Environment...)
	command.Stdin = bytes.NewReader(spec.Stdin)
	configureProcessGroup(command)
	var stdoutCapture, stderrCapture limitedBuffer
	stdoutCapture.limit, stderrCapture.limit = spec.CaptureLimit, spec.CaptureLimit
	entry := &activeProcess{command: command, done: make(chan struct{}), wait: spec.TerminateWait}
	var outputError error
	var outputMu sync.Mutex
	emitChunk := func(stream string, capture *limitedBuffer, data []byte) {
		outputMu.Lock()
		defer outputMu.Unlock()
		_, _ = capture.Write(data)
		if emit != nil && outputError == nil {
			outputError = emit(Output{Stream: stream, Data: append([]byte(nil), data...)})
			if outputError != nil {
				go entry.terminate("stream-error")
			}
		}
	}
	// Let os/exec own the copy goroutines by supplying writers instead of
	// StdoutPipe/StderrPipe. Cmd.Wait then cannot close a pipe before our reader
	// has captured the final short-lived process output.
	command.Stdout = outputWriter(func(data []byte) { emitChunk("stdout", &stdoutCapture, data) })
	command.Stderr = outputWriter(func(data []byte) { emitChunk("stderr", &stderrCapture, data) })
	r.mu.Lock()
	if _, exists := r.active[requestID]; exists {
		r.mu.Unlock()
		return Result{}, fmt.Errorf("request %q is already running", requestID)
	}
	r.active[requestID] = entry
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		delete(r.active, requestID)
		r.mu.Unlock()
	}()
	if err := command.Start(); err != nil {
		close(entry.done)
		return Result{}, fmt.Errorf("start %s: %w", spec.Executable, err)
	}

	waitResult := make(chan error, 1)
	go func() {
		waitResult <- command.Wait()
		close(entry.done)
	}()
	select {
	case <-ctx.Done():
		entry.terminate("cancelled")
	case <-entry.done:
	}
	waitErr := <-waitResult
	entry.mu.RLock()
	outcome := entry.outcome
	entry.mu.RUnlock()
	if outcome == "" {
		outcome = "exited"
	}
	result := Result{ExitCode: command.ProcessState.ExitCode(), Stdout: stdoutCapture.Bytes(), Stderr: stderrCapture.Bytes(), Outcome: outcome}
	if outputError != nil {
		return result, outputError
	}
	if waitErr != nil {
		var exitErr *exec.ExitError
		if errors.As(waitErr, &exitErr) {
			return result, &ExitError{Result: result}
		}
		return result, waitErr
	}
	return result, nil
}

// Cancel terminates the process group for an active request.
func (r *Runner) Cancel(requestID string) (string, bool) {
	r.mu.RLock()
	entry := r.active[requestID]
	r.mu.RUnlock()
	if entry == nil {
		return "not-running", false
	}
	entry.terminate("cancelled")
	entry.mu.RLock()
	defer entry.mu.RUnlock()
	return entry.outcome, true
}

func (p *activeProcess) terminate(reason string) {
	p.once.Do(func() {
		p.mu.Lock()
		p.outcome = reason + ":term"
		p.mu.Unlock()
		_ = signalProcessGroup(p.command, false)
		select {
		case <-p.done:
			return
		case <-time.After(p.wait):
		}
		p.mu.Lock()
		p.outcome = reason + ":kill"
		p.mu.Unlock()
		_ = signalProcessGroup(p.command, true)
		<-p.done
	})
}

type outputWriter func([]byte)

func (writer outputWriter) Write(data []byte) (int, error) {
	writer(data)
	return len(data), nil
}

type limitedBuffer struct {
	data  bytes.Buffer
	limit int
}

func (b *limitedBuffer) Write(data []byte) (int, error) {
	original := len(data)
	remaining := b.limit - b.data.Len()
	if remaining > 0 {
		if len(data) > remaining {
			data = data[:remaining]
		}
		_, _ = b.data.Write(data)
	}
	return original, nil
}

func (b *limitedBuffer) Bytes() []byte { return append([]byte(nil), b.data.Bytes()...) }

// MinimalEnvironment builds an explicit child environment from an allowlist.
func MinimalEnvironment(base map[string]string, injected map[string]string) []string {
	values := make([]string, 0, len(base)+len(injected))
	for key, value := range base {
		if key != "" && !strings.ContainsAny(key, "=\x00") && !strings.ContainsRune(value, '\x00') {
			values = append(values, key+"="+value)
		}
	}
	for key, value := range injected {
		if key != "" && !strings.ContainsAny(key, "=\x00") && !strings.ContainsRune(value, '\x00') {
			values = append(values, key+"="+value)
		}
	}
	return values
}
