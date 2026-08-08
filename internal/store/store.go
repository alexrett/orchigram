package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/alexrett/orchigram/internal/flow"
	"github.com/alexrett/orchigram/internal/resource"
	"github.com/google/uuid"

	// Register the pure-Go SQLite database/sql driver.
	_ "modernc.org/sqlite"
)

// ErrNotFound is returned when an authoritative object does not exist.
var ErrNotFound = errors.New("not found")

// ConflictError reports a failed compare-and-swap operation.
type ConflictError struct {
	Expected uint64
	Current  uint64
}

func (e *ConflictError) Error() string {
	return fmt.Sprintf("resource version conflict: expected %d, current %d", e.Expected, e.Current)
}

// Store is the authoritative SQLite state and event store.
type Store struct {
	db  *sql.DB
	now func() time.Time
}

// Open opens a SQLite WAL database and applies deterministic migrations.
func Open(path string) (*Store, error) {
	if err := ensureParent(path); err != nil {
		return nil, err
	}
	dsn := "file:" + filepath.ToSlash(path) + "?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=foreign_keys(1)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(1)
	result := &Store{db: db, now: time.Now}
	if err := result.migrate(context.Background()); err != nil {
		_ = db.Close()
		return nil, err
	}
	return result, nil
}

// Close releases the database handle.
func (s *Store) Close() error { return s.db.Close() }

func ensureParent(path string) error {
	if path == ":memory:" {
		return nil
	}
	parent := filepath.Dir(path)
	if parent == "." || parent == "" {
		return nil
	}
	if err := os.MkdirAll(parent, 0o750); err != nil {
		return fmt.Errorf("create sqlite parent: %w", err)
	}
	return nil
}

func (s *Store) migrate(ctx context.Context) error {
	entries, err := fs.ReadDir(Migrations, "migrations")
	if err != nil {
		return fmt.Errorf("read migrations: %w", err)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		var version int
		if _, err := fmt.Sscanf(entry.Name(), "%d_", &version); err != nil {
			return fmt.Errorf("migration filename %q: %w", entry.Name(), err)
		}
		var exists int
		err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='schema_migrations'").Scan(&exists)
		if err != nil {
			return fmt.Errorf("inspect migration table: %w", err)
		}
		if exists == 1 {
			var applied int
			if err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM schema_migrations WHERE version = ?", version).Scan(&applied); err != nil {
				return fmt.Errorf("check migration %d: %w", version, err)
			}
			if applied == 1 {
				continue
			}
		}
		body, err := fs.ReadFile(Migrations, "migrations/"+entry.Name())
		if err != nil {
			return fmt.Errorf("read migration %d: %w", version, err)
		}
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, string(body)); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("apply migration %d: %w", version, err)
		}
		if _, err := tx.ExecContext(ctx, "INSERT INTO schema_migrations(version, applied_at) VALUES (?, ?)", version, s.timestamp()); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("record migration %d: %w", version, err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit migration %d: %w", version, err)
		}
	}
	return nil
}

// ApplyOptions are audit and compare-and-swap inputs.
type ApplyOptions struct {
	ExpectedResourceVersion uint64
	RequestID               string
	Actor                   string
	Context                 string
}

// Apply atomically stores a resource, event, and audit record.
func (s *Store) Apply(ctx context.Context, doc resource.Document, options ApplyOptions) (resource.Document, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return resource.Document{}, err
	}
	defer func() { _ = tx.Rollback() }()

	var currentJSON []byte
	var currentVersion, currentGeneration uint64
	var currentUID string
	err = tx.QueryRowContext(ctx, `SELECT spec_json, resource_version, generation, uid FROM resources WHERE kind=? AND namespace=? AND name=?`, doc.Kind, doc.Metadata.Namespace, doc.Metadata.Name).Scan(&currentJSON, &currentVersion, &currentGeneration, &currentUID)
	exists := err == nil
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return resource.Document{}, fmt.Errorf("read current resource: %w", err)
	}
	expected := options.ExpectedResourceVersion
	if expected == 0 {
		expected = doc.Metadata.ResourceVersion
	}
	if exists && expected != currentVersion {
		return resource.Document{}, &ConflictError{Expected: expected, Current: currentVersion}
	}
	if !exists && expected != 0 {
		return resource.Document{}, &ConflictError{Expected: expected, Current: 0}
	}

	revision, err := nextRevision(ctx, tx)
	if err != nil {
		return resource.Document{}, err
	}
	meta := doc.Metadata
	meta.ResourceVersion = revision
	if exists {
		meta.UID = currentUID
		meta.Generation = currentGeneration
		currentDoc, decodeErr := resource.DecodeStrict(currentJSON)
		if decodeErr != nil {
			return resource.Document{}, fmt.Errorf("decode stored resource: %w", decodeErr)
		}
		if !jsonEqual(currentDoc.Spec, doc.Spec) {
			meta.Generation++
		}
	} else {
		meta.UID = uuid.NewString()
		meta.Generation = 1
	}
	doc, err = doc.WithServerMetadata(meta)
	if err != nil {
		return resource.Document{}, err
	}
	labels, _ := json.Marshal(meta.Labels)
	oldHash := digest(currentJSON)
	newHash := digest(doc.JSON)
	now := s.timestamp()
	if exists {
		_, err = tx.ExecContext(ctx, `UPDATE resources SET uid=?,resource_version=?,generation=?,labels_json=?,spec_json=?,status_json=?,updated_at=? WHERE kind=? AND namespace=? AND name=?`, meta.UID, revision, meta.Generation, labels, doc.JSON, doc.Status, now, doc.Kind, meta.Namespace, meta.Name)
	} else {
		_, err = tx.ExecContext(ctx, `INSERT INTO resources(kind,namespace,name,uid,resource_version,generation,labels_json,spec_json,status_json,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)`, doc.Kind, meta.Namespace, meta.Name, meta.UID, revision, meta.Generation, labels, doc.JSON, doc.Status, now, now)
	}
	if err != nil {
		return resource.Document{}, fmt.Errorf("write resource: %w", err)
	}
	eventType := "ADDED"
	if exists {
		eventType = "MODIFIED"
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO resource_events(revision,event_type,kind,namespace,name,uid,resource_json,observed_at) VALUES(?,?,?,?,?,?,?,?)`, revision, eventType, doc.Kind, meta.Namespace, meta.Name, meta.UID, doc.JSON, now); err != nil {
		return resource.Document{}, fmt.Errorf("append resource event: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO audit_records(revision,request_id,actor,context_name,operation,resource_uid,old_hash,new_hash,occurred_at) VALUES(?,?,?,?,?,?,?,?,?)`, revision, valueOr(options.RequestID, uuid.NewString()), valueOr(options.Actor, "unix-peer"), options.Context, strings.ToLower(eventType), meta.UID, oldHash, newHash, now); err != nil {
		return resource.Document{}, fmt.Errorf("append audit record: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return resource.Document{}, err
	}
	return doc, nil
}

// Get returns one canonical resource.
func (s *Store) Get(ctx context.Context, kind, namespace, name string) (resource.Document, error) {
	if namespace == "" {
		namespace = resource.DefaultNamespace
	}
	var data []byte
	if err := s.db.QueryRowContext(ctx, `SELECT spec_json FROM resources WHERE kind=? AND namespace=? AND name=?`, kind, namespace, name).Scan(&data); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return resource.Document{}, ErrNotFound
		}
		return resource.Document{}, err
	}
	doc, err := resource.DecodeStrict(data)
	if err != nil {
		return resource.Document{}, fmt.Errorf("decode stored resource: %w", err)
	}
	return doc, nil
}

// List returns canonical resources in stable key order.
func (s *Store) List(ctx context.Context, kind, namespace string, limit int) ([]resource.Document, uint64, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	query := `SELECT spec_json FROM resources WHERE (?='' OR kind=?) AND (?='' OR namespace=?) ORDER BY kind,namespace,name LIMIT ?`
	rows, err := s.db.QueryContext(ctx, query, kind, kind, namespace, namespace, limit)
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = rows.Close() }()
	result := make([]resource.Document, 0)
	for rows.Next() {
		var data []byte
		if err := rows.Scan(&data); err != nil {
			return nil, 0, err
		}
		doc, err := resource.DecodeStrict(data)
		if err != nil {
			return nil, 0, err
		}
		result = append(result, doc)
	}
	var revision uint64
	if err := s.db.QueryRowContext(ctx, "SELECT value FROM revisions WHERE singleton=1").Scan(&revision); err != nil {
		return nil, 0, err
	}
	return result, revision, rows.Err()
}

// Delete performs a CAS delete and appends event and audit state.
func (s *Store) Delete(ctx context.Context, kind, namespace, name string, expected uint64, requestID string) error {
	if namespace == "" {
		namespace = resource.DefaultNamespace
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	var data []byte
	var current uint64
	var uid string
	if err := tx.QueryRowContext(ctx, `SELECT spec_json,resource_version,uid FROM resources WHERE kind=? AND namespace=? AND name=?`, kind, namespace, name).Scan(&data, &current, &uid); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		return err
	}
	if current != expected {
		return &ConflictError{Expected: expected, Current: current}
	}
	revision, err := nextRevision(ctx, tx)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM resources WHERE kind=? AND namespace=? AND name=?`, kind, namespace, name); err != nil {
		return err
	}
	now := s.timestamp()
	if _, err := tx.ExecContext(ctx, `INSERT INTO resource_events(revision,event_type,kind,namespace,name,uid,resource_json,observed_at) VALUES(?,?,?,?,?,?,NULL,?)`, revision, "DELETED", kind, namespace, name, uid, now); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO audit_records(revision,request_id,actor,context_name,operation,resource_uid,old_hash,new_hash,occurred_at) VALUES(?,?,?,?,?,?,?,?,?)`, revision, valueOr(requestID, uuid.NewString()), "unix-peer", "", "deleted", uid, digest(data), "", now); err != nil {
		return err
	}
	return tx.Commit()
}

// ResourceEvent is one global revision in the watch log.
type ResourceEvent struct {
	Revision   uint64
	Type       string
	Document   []byte
	ObservedAt time.Time
}

// EventsAfter returns a bounded watch page after a global revision.
func (s *Store) EventsAfter(ctx context.Context, kind, namespace string, revision uint64, limit int) ([]ResourceEvent, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, `SELECT revision,event_type,resource_json,observed_at FROM resource_events WHERE revision>? AND (?='' OR kind=?) AND (?='' OR namespace=?) ORDER BY revision LIMIT ?`, revision, kind, kind, namespace, namespace, limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	result := []ResourceEvent{}
	for rows.Next() {
		var event ResourceEvent
		var observed string
		if err := rows.Scan(&event.Revision, &event.Type, &event.Document, &observed); err != nil {
			return nil, err
		}
		event.ObservedAt, _ = time.Parse(time.RFC3339Nano, observed)
		result = append(result, event)
	}
	return result, rows.Err()
}

func nextRevision(ctx context.Context, tx *sql.Tx) (uint64, error) {
	var revision uint64
	if err := tx.QueryRowContext(ctx, `UPDATE revisions SET value=value+1 WHERE singleton=1 RETURNING value`).Scan(&revision); err != nil {
		return 0, fmt.Errorf("allocate revision: %w", err)
	}
	return revision, nil
}

func jsonEqual(left, right []byte) bool {
	var a, b any
	if json.Unmarshal(left, &a) != nil || json.Unmarshal(right, &b) != nil {
		return false
	}
	leftCanonical, leftErr := json.Marshal(a)
	rightCanonical, rightErr := json.Marshal(b)
	return leftErr == nil && rightErr == nil && string(leftCanonical) == string(rightCanonical)
}

func digest(value []byte) string {
	if len(value) == 0 {
		return ""
	}
	sum := sha256.Sum256(value)
	return hex.EncodeToString(sum[:])
}

func valueOr(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

func (s *Store) timestamp() string { return s.now().UTC().Format(time.RFC3339Nano) }

// PutPlan idempotently persists an immutable compiled plan.
func (s *Store) PutPlan(ctx context.Context, plan flow.ExecutionPlan) error {
	data, err := json.Marshal(plan)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `INSERT OR IGNORE INTO compiled_plans(plan_hash,flow_uid,flow_generation,interpreter_version,plan_json,created_at) VALUES(?,?,?,?,?,?)`, plan.PlanHash, plan.FlowUID, plan.FlowGeneration, plan.InterpreterVersion, data, s.timestamp())
	return err
}

// GetPlan returns an immutable compiled plan by content hash.
func (s *Store) GetPlan(ctx context.Context, planHash string) (flow.ExecutionPlan, error) {
	var data []byte
	if err := s.db.QueryRowContext(ctx, `SELECT plan_json FROM compiled_plans WHERE plan_hash=?`, planHash).Scan(&data); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return flow.ExecutionPlan{}, ErrNotFound
		}
		return flow.ExecutionPlan{}, err
	}
	var plan flow.ExecutionPlan
	if err := json.Unmarshal(data, &plan); err != nil {
		return flow.ExecutionPlan{}, fmt.Errorf("decode compiled plan: %w", err)
	}
	return plan, nil
}

// StartPayload is the durable normalized start command in the outbox.
type StartPayload struct {
	RunUID         string          `json:"runUID"`
	ReceiptUID     string          `json:"receiptUID"`
	FlowName       string          `json:"flowName"`
	Namespace      string          `json:"namespace"`
	Input          json.RawMessage `json:"input"`
	IdempotencyKey string          `json:"idempotencyKey"`
}

// Receipt is the durable acknowledgement of one external occurrence.
type Receipt struct {
	UID          string
	TriggerUID   string
	OccurrenceID string
	RunUID       string
	Deduplicated bool
	AcceptedAt   time.Time
	Existing     bool
}

// AcceptTrigger persists a receipt and outbox command in one transaction.
func (s *Store) AcceptTrigger(ctx context.Context, triggerUID, occurrenceID, flowName, namespace string, input json.RawMessage, deduplicated bool) (Receipt, error) {
	return s.acceptTrigger(ctx, triggerUID, occurrenceID, flowName, namespace, input, deduplicated, "", "")
}

// AcceptProviderTrigger persists receipt, outbox, and provider cursor atomically.
func (s *Store) AcceptProviderTrigger(ctx context.Context, triggerUID, occurrenceID, flowName, namespace string, input json.RawMessage, cursor string) (Receipt, error) {
	return s.acceptTrigger(ctx, triggerUID, occurrenceID, flowName, namespace, input, true, cursor, occurrenceID)
}

func (s *Store) acceptTrigger(ctx context.Context, triggerUID, occurrenceID, flowName, namespace string, input json.RawMessage, deduplicated bool, providerCursor, providerEventID string) (Receipt, error) {
	if namespace == "" {
		namespace = resource.DefaultNamespace
	}
	if len(input) == 0 {
		input = json.RawMessage(`{}`)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Receipt{}, err
	}
	defer func() { _ = tx.Rollback() }()
	existing, err := receiptByOccurrence(ctx, tx, triggerUID, occurrenceID)
	if err == nil {
		existing.Existing = true
		if providerCursor != "" {
			if err := upsertProviderCursor(ctx, tx, triggerUID, providerCursor, providerEventID, s.timestamp()); err != nil {
				return Receipt{}, err
			}
		}
		return existing, tx.Commit()
	}
	if !errors.Is(err, ErrNotFound) {
		return Receipt{}, err
	}
	now := s.timestamp()
	receipt := Receipt{UID: uuid.NewString(), TriggerUID: triggerUID, OccurrenceID: occurrenceID, RunUID: uuid.NewString(), Deduplicated: deduplicated}
	receipt.AcceptedAt, _ = time.Parse(time.RFC3339Nano, now)
	payload := StartPayload{RunUID: receipt.RunUID, ReceiptUID: receipt.UID, FlowName: flowName, Namespace: namespace, Input: input, IdempotencyKey: "trigger/" + triggerUID + "/" + occurrenceID}
	payloadJSON, _ := json.Marshal(payload)
	if _, err := tx.ExecContext(ctx, `INSERT INTO trigger_receipts(uid,trigger_uid,occurrence_id,payload_json,deduplicated,run_uid,accepted_at) VALUES(?,?,?,?,?,?,?)`, receipt.UID, triggerUID, occurrenceID, input, boolInt(deduplicated), receipt.RunUID, now); err != nil {
		return Receipt{}, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO outbox(command_type,aggregate_uid,idempotency_key,payload_json,state,available_at) VALUES(?,?,?,?,?,?)`, "start-run", receipt.RunUID, payload.IdempotencyKey, payloadJSON, "pending", now); err != nil {
		return Receipt{}, err
	}
	if providerCursor != "" {
		if err := upsertProviderCursor(ctx, tx, triggerUID, providerCursor, providerEventID, now); err != nil {
			return Receipt{}, err
		}
	}
	if err := tx.Commit(); err != nil {
		return Receipt{}, err
	}
	return receipt, nil
}

func upsertProviderCursor(ctx context.Context, tx *sql.Tx, triggerUID, cursor, eventID, now string) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO provider_cursors(trigger_uid,cursor,provider_event_id,acknowledged_at) VALUES(?,?,?,?) ON CONFLICT(trigger_uid) DO UPDATE SET cursor=excluded.cursor,provider_event_id=excluded.provider_event_id,acknowledged_at=excluded.acknowledged_at`, triggerUID, cursor, eventID, now)
	return err
}

func receiptByOccurrence(ctx context.Context, tx *sql.Tx, triggerUID, occurrenceID string) (Receipt, error) {
	var receipt Receipt
	var deduplicated int
	var accepted string
	if err := tx.QueryRowContext(ctx, `SELECT uid,trigger_uid,occurrence_id,run_uid,deduplicated,accepted_at FROM trigger_receipts WHERE trigger_uid=? AND occurrence_id=?`, triggerUID, occurrenceID).Scan(&receipt.UID, &receipt.TriggerUID, &receipt.OccurrenceID, &receipt.RunUID, &deduplicated, &accepted); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Receipt{}, ErrNotFound
		}
		return Receipt{}, err
	}
	receipt.Deduplicated = deduplicated == 1
	receipt.AcceptedAt, _ = time.Parse(time.RFC3339Nano, accepted)
	return receipt, nil
}

// ReceiptByOccurrence returns the durable receipt for a stable trigger identity.
// Controllers use this read before concurrency checks so replaying the same
// occurrence advances its cursor instead of being misclassified as overlap.
func (s *Store) ReceiptByOccurrence(ctx context.Context, triggerUID, occurrenceID string) (Receipt, error) {
	var receipt Receipt
	var deduplicated int
	var accepted string
	if err := s.db.QueryRowContext(ctx, `SELECT uid,trigger_uid,occurrence_id,run_uid,deduplicated,accepted_at FROM trigger_receipts WHERE trigger_uid=? AND occurrence_id=?`, triggerUID, occurrenceID).Scan(&receipt.UID, &receipt.TriggerUID, &receipt.OccurrenceID, &receipt.RunUID, &deduplicated, &accepted); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Receipt{}, ErrNotFound
		}
		return Receipt{}, err
	}
	receipt.Deduplicated = deduplicated == 1
	receipt.AcceptedAt, _ = time.Parse(time.RFC3339Nano, accepted)
	return receipt, nil
}

// OutboxCommand is a claimed durable command.
type OutboxCommand struct {
	ID       int64
	Payload  StartPayload
	Attempts int
}

// ClaimStart claims one pending or stale start command.
func (s *Store) ClaimStart(ctx context.Context, staleAfter time.Duration) (OutboxCommand, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return OutboxCommand{}, err
	}
	defer func() { _ = tx.Rollback() }()
	stale := s.now().UTC().Add(-staleAfter).Format(time.RFC3339Nano)
	var command OutboxCommand
	var payload []byte
	if err := tx.QueryRowContext(ctx, `SELECT id,payload_json,attempts FROM outbox WHERE command_type='start-run' AND (state='pending' OR (state='inflight' AND claimed_at<?)) AND available_at<=? ORDER BY id LIMIT 1`, stale, s.timestamp()).Scan(&command.ID, &payload, &command.Attempts); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return OutboxCommand{}, ErrNotFound
		}
		return OutboxCommand{}, err
	}
	if err := json.Unmarshal(payload, &command.Payload); err != nil {
		return OutboxCommand{}, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE outbox SET state='inflight',attempts=attempts+1,claimed_at=? WHERE id=?`, s.timestamp(), command.ID); err != nil {
		return OutboxCommand{}, err
	}
	command.Attempts++
	if err := tx.Commit(); err != nil {
		return OutboxCommand{}, err
	}
	return command, nil
}

// CompleteOutbox records successful durable dispatch.
func (s *Store) CompleteOutbox(ctx context.Context, id int64) error {
	_, err := s.db.ExecContext(ctx, `UPDATE outbox SET state='completed',completed_at=?,last_error='' WHERE id=?`, s.timestamp(), id)
	return err
}

// RetryOutbox records an error and makes the command available later.
func (s *Store) RetryOutbox(ctx context.Context, id int64, cause error, delay time.Duration) error {
	_, err := s.db.ExecContext(ctx, `UPDATE outbox SET state='pending',available_at=?,last_error=? WHERE id=?`, s.now().UTC().Add(delay).Format(time.RFC3339Nano), cause.Error(), id)
	return err
}

// Run is the server-owned durable run projection.
type Run struct {
	UID                string
	FlowUID            string
	PlanHash           string
	InterpreterVersion string
	Phase              string
	CreatedAt          time.Time
	UpdatedAt          time.Time
}

// EnsureRun idempotently creates the pinned local run and its first event.
func (s *Store) EnsureRun(ctx context.Context, payload StartPayload, plan flow.ExecutionPlan) (bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback() }()
	var exists int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM runs WHERE uid=?`, payload.RunUID).Scan(&exists); err != nil {
		return false, err
	}
	if exists == 1 {
		return false, tx.Commit()
	}
	planJSON, _ := json.Marshal(plan)
	if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO compiled_plans(plan_hash,flow_uid,flow_generation,interpreter_version,plan_json,created_at) VALUES(?,?,?,?,?,?)`, plan.PlanHash, plan.FlowUID, plan.FlowGeneration, plan.InterpreterVersion, planJSON, s.timestamp()); err != nil {
		return false, err
	}
	now := s.timestamp()
	if _, err := tx.ExecContext(ctx, `INSERT INTO runs(uid,flow_uid,trigger_receipt_uid,plan_hash,interpreter_version,phase,input_json,current_nodes_json,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)`, payload.RunUID, plan.FlowUID, payload.ReceiptUID, plan.PlanHash, plan.InterpreterVersion, "pending", payload.Input, []byte(`[]`), now, now); err != nil {
		return false, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO run_events(run_uid,sequence,event_type,payload_json,occurred_at) VALUES(?,?,?,?,?)`, payload.RunUID, 1, "run.accepted", []byte(`{}`), now); err != nil {
		return false, err
	}
	return true, tx.Commit()
}

// GetRun returns one run projection.
func (s *Store) GetRun(ctx context.Context, uid string) (Run, error) {
	var result Run
	var created, updated string
	if err := s.db.QueryRowContext(ctx, `SELECT uid,flow_uid,plan_hash,interpreter_version,phase,created_at,updated_at FROM runs WHERE uid=?`, uid).Scan(&result.UID, &result.FlowUID, &result.PlanHash, &result.InterpreterVersion, &result.Phase, &created, &updated); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Run{}, ErrNotFound
		}
		return Run{}, err
	}
	result.CreatedAt, _ = time.Parse(time.RFC3339Nano, created)
	result.UpdatedAt, _ = time.Parse(time.RFC3339Nano, updated)
	return result, nil
}

// ListRuns returns newest runs first.
func (s *Store) ListRuns(ctx context.Context, limit int) ([]Run, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, `SELECT uid,flow_uid,plan_hash,interpreter_version,phase,created_at,updated_at FROM runs ORDER BY created_at DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	result := []Run{}
	for rows.Next() {
		var run Run
		var created, updated string
		if err := rows.Scan(&run.UID, &run.FlowUID, &run.PlanHash, &run.InterpreterVersion, &run.Phase, &created, &updated); err != nil {
			return nil, err
		}
		run.CreatedAt, _ = time.Parse(time.RFC3339Nano, created)
		run.UpdatedAt, _ = time.Parse(time.RFC3339Nano, updated)
		result = append(result, run)
	}
	return result, rows.Err()
}

// AppendRunEvent serializes a state transition and its event.
func (s *Store) AppendRunEvent(ctx context.Context, runUID, nodeID, eventType, phase string, attempt uint32, payload json.RawMessage) error {
	if len(payload) == 0 {
		payload = json.RawMessage(`{}`)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	var currentPhase string
	if err := tx.QueryRowContext(ctx, `SELECT phase FROM runs WHERE uid=?`, runUID).Scan(&currentPhase); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		return err
	}
	if terminalRunPhase(currentPhase) {
		return tx.Commit()
	}
	var sequence uint64
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(sequence),0)+1 FROM run_events WHERE run_uid=?`, runUID).Scan(&sequence); err != nil {
		return err
	}
	now := s.timestamp()
	if _, err := tx.ExecContext(ctx, `INSERT INTO run_events(run_uid,sequence,node_id,attempt,event_type,payload_json,occurred_at) VALUES(?,?,?,?,?,?,?)`, runUID, sequence, nodeID, attempt, eventType, payload, now); err != nil {
		return err
	}
	completed := any(nil)
	if phase == "succeeded" || phase == "failed" || phase == "cancelled" || phase == "rejected" {
		completed = now
	}
	if _, err := tx.ExecContext(ctx, `UPDATE runs SET phase=?,updated_at=?,completed_at=COALESCE(?,completed_at) WHERE uid=?`, phase, now, completed, runUID); err != nil {
		return err
	}
	return tx.Commit()
}

func terminalRunPhase(phase string) bool {
	return phase == "succeeded" || phase == "failed" || phase == "rejected" || phase == "cancelled"
}

// RequestRunCancellation atomically records operator intent, the terminal run
// projection, and an engine-delivery record. Repeated requests preserve the
// first cancellation reason and do not append duplicate events.
func (s *Store) RequestRunCancellation(ctx context.Context, runUID, reason string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	var phase string
	if err := tx.QueryRowContext(ctx, `SELECT phase FROM runs WHERE uid=?`, runUID).Scan(&phase); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		return err
	}
	if phase != "cancelled" && terminalRunPhase(phase) {
		return tx.Commit()
	}
	now := s.timestamp()
	result, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO run_cancellations(run_uid,reason,requested_at) VALUES(?,?,?)`, runUID, reason, now)
	if err != nil {
		return err
	}
	inserted, _ := result.RowsAffected()
	if inserted == 0 {
		return tx.Commit()
	}
	var sequence uint64
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(sequence),0)+1 FROM run_events WHERE run_uid=?`, runUID).Scan(&sequence); err != nil {
		return err
	}
	payload, _ := json.Marshal(map[string]string{"reason": reason})
	if _, err := tx.ExecContext(ctx, `INSERT INTO run_events(run_uid,sequence,node_id,attempt,event_type,payload_json,occurred_at) VALUES(?,?,?,?,?,?,?)`, runUID, sequence, "", 0, "run.cancelled", payload, now); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE runs SET phase='cancelled',updated_at=?,completed_at=COALESCE(completed_at,?) WHERE uid=?`, now, now, runUID); err != nil {
		return err
	}
	return tx.Commit()
}

// RunCancellation is durable cancellation intent awaiting engine delivery.
type RunCancellation struct {
	RunUID string
	Reason string
}

// UndeliveredRunCancellations returns cancellation intents in request order.
func (s *Store) UndeliveredRunCancellations(ctx context.Context, limit int) ([]RunCancellation, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, `SELECT run_uid,reason FROM run_cancellations WHERE delivery_completed=0 ORDER BY requested_at,run_uid LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	result := []RunCancellation{}
	for rows.Next() {
		var cancellation RunCancellation
		if err := rows.Scan(&cancellation.RunUID, &cancellation.Reason); err != nil {
			return nil, err
		}
		result = append(result, cancellation)
	}
	return result, rows.Err()
}

// MarkRunCancellationDelivered acknowledges both provider and workflow delivery.
func (s *Store) MarkRunCancellationDelivered(ctx context.Context, runUID string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE run_cancellations SET delivery_completed=1,delivered_at=? WHERE run_uid=?`, s.timestamp(), runUID)
	return err
}

// RunEvent is an operator-visible durable transition.
type RunEvent struct {
	Sequence   uint64
	RunUID     string
	NodeID     string
	Attempt    uint32
	Type       string
	Payload    json.RawMessage
	OccurredAt time.Time
}

// RunEventsAfter returns an ordered page for watches and replay.
func (s *Store) RunEventsAfter(ctx context.Context, runUID string, after uint64, limit int) ([]RunEvent, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, `SELECT sequence,run_uid,node_id,attempt,event_type,payload_json,occurred_at FROM run_events WHERE run_uid=? AND sequence>? ORDER BY sequence LIMIT ?`, runUID, after, limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	result := []RunEvent{}
	for rows.Next() {
		var event RunEvent
		var occurred string
		if err := rows.Scan(&event.Sequence, &event.RunUID, &event.NodeID, &event.Attempt, &event.Type, &event.Payload, &occurred); err != nil {
			return nil, err
		}
		event.OccurredAt, _ = time.Parse(time.RFC3339Nano, occurred)
		result = append(result, event)
	}
	return result, rows.Err()
}

// EnsureApproval idempotently creates a durable pending decision.
func (s *Store) EnsureApproval(ctx context.Context, runUID, nodeID string, expiresAt time.Time) error {
	var expiry any
	if !expiresAt.IsZero() {
		expiry = expiresAt.UTC().Format(time.RFC3339Nano)
	}
	_, err := s.db.ExecContext(ctx, `INSERT OR IGNORE INTO approvals(run_uid,node_id,state,expires_at) VALUES(?,?,?,?)`, runUID, nodeID, "pending", expiry)
	return err
}

// DecideApproval atomically applies the first durable approval decision.
func (s *Store) DecideApproval(ctx context.Context, runUID, nodeID, state, actor, reason string) error {
	if state != "approved" && state != "rejected" {
		return fmt.Errorf("invalid approval state %q", state)
	}
	result, err := s.db.ExecContext(ctx, `UPDATE approvals SET state=?,actor=?,reason=?,decided_at=?,signal_delivered=0 WHERE run_uid=? AND node_id=? AND state='pending'`, state, actor, reason, s.timestamp(), runUID, nodeID)
	if err != nil {
		return err
	}
	changed, _ := result.RowsAffected()
	if changed == 0 {
		return fmt.Errorf("approval is missing or already decided")
	}
	return nil
}

// ApprovalSignal is a durable decision awaiting engine delivery.
type ApprovalSignal struct {
	RunUID string
	NodeID string
	State  string
	Reason string
}

// UndeliveredApprovalSignals returns decisions that reconciliation must signal.
func (s *Store) UndeliveredApprovalSignals(ctx context.Context, limit int) ([]ApprovalSignal, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, `SELECT run_uid,node_id,state,reason FROM approvals WHERE state!='pending' AND signal_delivered=0 ORDER BY decided_at LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	result := []ApprovalSignal{}
	for rows.Next() {
		var signal ApprovalSignal
		if err := rows.Scan(&signal.RunUID, &signal.NodeID, &signal.State, &signal.Reason); err != nil {
			return nil, err
		}
		result = append(result, signal)
	}
	return result, rows.Err()
}

// MarkApprovalSignaled records successful delivery to the durable engine.
func (s *Store) MarkApprovalSignaled(ctx context.Context, runUID, nodeID string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE approvals SET signal_delivered=1 WHERE run_uid=? AND node_id=? AND state!='pending'`, runUID, nodeID)
	return err
}

// PluginRecord is the authoritative immutable installation projection.
type PluginRecord struct {
	Name         string
	Version      string
	Digest       string
	ManifestJSON json.RawMessage
	State        string
	InstalledAt  time.Time
	Active       bool
}

// PutPlugin records one immutable plugin version idempotently.
func (s *Store) PutPlugin(ctx context.Context, record PluginRecord) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	var existingDigest string
	err = tx.QueryRowContext(ctx, `SELECT digest FROM plugin_installations WHERE name=? AND version=?`, record.Name, record.Version).Scan(&existingDigest)
	if err == nil {
		if existingDigest != record.Digest {
			return fmt.Errorf("plugin %s version %s already has digest %s", record.Name, record.Version, existingDigest)
		}
		return tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	state := record.State
	if state == "" {
		state = "installed"
	}
	installedAt := record.InstalledAt
	if installedAt.IsZero() {
		installedAt = s.now().UTC()
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO plugin_installations(name,version,digest,manifest_json,state,installed_at) VALUES(?,?,?,?,?,?)`, record.Name, record.Version, record.Digest, []byte(record.ManifestJSON), state, installedAt.Format(time.RFC3339Nano)); err != nil {
		return err
	}
	return tx.Commit()
}

// ActivatePlugin atomically switches one plugin name to an installed version.
func (s *Store) ActivatePlugin(ctx context.Context, name, version string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	var exists int
	if err := tx.QueryRowContext(ctx, `SELECT 1 FROM plugin_installations WHERE name=? AND version=?`, name, version).Scan(&exists); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO plugin_activations(name,version,activated_at) VALUES(?,?,?) ON CONFLICT(name) DO UPDATE SET version=excluded.version,activated_at=excluded.activated_at`, name, version, s.timestamp()); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE plugin_installations SET state=CASE WHEN name=? AND version=? THEN 'active' WHEN name=? AND state='active' THEN 'installed' ELSE state END`, name, version, name); err != nil {
		return err
	}
	return tx.Commit()
}

// DisablePlugin removes an activation without deleting immutable versions.
func (s *Store) DisablePlugin(ctx context.Context, name string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	result, err := tx.ExecContext(ctx, `DELETE FROM plugin_activations WHERE name=?`, name)
	if err != nil {
		return err
	}
	changed, _ := result.RowsAffected()
	if changed == 0 {
		return ErrNotFound
	}
	if _, err := tx.ExecContext(ctx, `UPDATE plugin_installations SET state='installed' WHERE name=? AND state='active'`, name); err != nil {
		return err
	}
	return tx.Commit()
}

// Plugin returns one installed version, or the active version when version is empty.
func (s *Store) Plugin(ctx context.Context, name, version string) (PluginRecord, error) {
	query := `SELECT p.name,p.version,p.digest,p.manifest_json,p.state,p.installed_at,CASE WHEN a.version=p.version THEN 1 ELSE 0 END FROM plugin_installations p LEFT JOIN plugin_activations a ON a.name=p.name WHERE p.name=? AND p.version=?`
	arguments := []any{name, version}
	if version == "" {
		query = `SELECT p.name,p.version,p.digest,p.manifest_json,p.state,p.installed_at,1 FROM plugin_activations a JOIN plugin_installations p ON p.name=a.name AND p.version=a.version WHERE p.name=?`
		arguments = []any{name}
	}
	var record PluginRecord
	var installed string
	var active int
	if err := s.db.QueryRowContext(ctx, query, arguments...).Scan(&record.Name, &record.Version, &record.Digest, &record.ManifestJSON, &record.State, &installed, &active); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return PluginRecord{}, ErrNotFound
		}
		return PluginRecord{}, err
	}
	record.InstalledAt, _ = time.Parse(time.RFC3339Nano, installed)
	record.Active = active == 1
	return record, nil
}

// ListPlugins returns immutable versions in stable name/version order.
func (s *Store) ListPlugins(ctx context.Context) ([]PluginRecord, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT p.name,p.version,p.digest,p.manifest_json,p.state,p.installed_at,CASE WHEN a.version=p.version THEN 1 ELSE 0 END FROM plugin_installations p LEFT JOIN plugin_activations a ON a.name=p.name ORDER BY p.name,p.version`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	result := []PluginRecord{}
	for rows.Next() {
		var record PluginRecord
		var installed string
		var active int
		if err := rows.Scan(&record.Name, &record.Version, &record.Digest, &record.ManifestJSON, &record.State, &installed, &active); err != nil {
			return nil, err
		}
		record.InstalledAt, _ = time.Parse(time.RFC3339Nano, installed)
		record.Active = active == 1
		result = append(result, record)
	}
	return result, rows.Err()
}

// TriggerState is the durable controller cursor for one resource generation.
type TriggerState struct {
	TriggerUID string
	Generation uint64
	Enabled    bool
	CursorAt   time.Time
}

// TriggerSkip is a durable non-run occurrence decision.
type TriggerSkip struct {
	TriggerUID   string
	OccurrenceID string
	Reason       string
	ScheduledAt  time.Time
}

// EnsureTriggerState creates a cursor or resets it when the resource generation changes.
func (s *Store) EnsureTriggerState(ctx context.Context, triggerUID string, generation uint64, enabled bool, initialCursor time.Time) (TriggerState, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return TriggerState{}, err
	}
	defer func() { _ = tx.Rollback() }()
	var state TriggerState
	var enabledValue int
	var cursor string
	err = tx.QueryRowContext(ctx, `SELECT trigger_uid,generation,enabled,cursor_at FROM trigger_states WHERE trigger_uid=?`, triggerUID).Scan(&state.TriggerUID, &state.Generation, &enabledValue, &cursor)
	if errors.Is(err, sql.ErrNoRows) {
		now := s.timestamp()
		if _, err := tx.ExecContext(ctx, `INSERT INTO trigger_states(trigger_uid,generation,enabled,cursor_at,updated_at) VALUES(?,?,?,?,?)`, triggerUID, generation, boolInt(enabled), initialCursor.UTC().Format(time.RFC3339Nano), now); err != nil {
			return TriggerState{}, err
		}
		state = TriggerState{TriggerUID: triggerUID, Generation: generation, Enabled: enabled, CursorAt: initialCursor.UTC()}
		return state, tx.Commit()
	}
	if err != nil {
		return TriggerState{}, err
	}
	state.Enabled = enabledValue == 1
	state.CursorAt, _ = time.Parse(time.RFC3339Nano, cursor)
	if state.Generation != generation {
		if _, err := tx.ExecContext(ctx, `UPDATE trigger_states SET generation=?,enabled=?,cursor_at=?,updated_at=? WHERE trigger_uid=?`, generation, boolInt(enabled), initialCursor.UTC().Format(time.RFC3339Nano), s.timestamp(), triggerUID); err != nil {
			return TriggerState{}, err
		}
		state.Generation, state.Enabled, state.CursorAt = generation, enabled, initialCursor.UTC()
	}
	return state, tx.Commit()
}

// TriggerState returns the durable controller state for one trigger.
func (s *Store) TriggerState(ctx context.Context, triggerUID string) (TriggerState, error) {
	var state TriggerState
	var enabledValue int
	var cursor string
	if err := s.db.QueryRowContext(ctx, `SELECT trigger_uid,generation,enabled,cursor_at FROM trigger_states WHERE trigger_uid=?`, triggerUID).Scan(&state.TriggerUID, &state.Generation, &enabledValue, &cursor); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return TriggerState{}, ErrNotFound
		}
		return TriggerState{}, err
	}
	state.Enabled = enabledValue == 1
	state.CursorAt, _ = time.Parse(time.RFC3339Nano, cursor)
	return state, nil
}

// AdvanceTriggerCursor durably records the latest evaluated scheduled instant.
func (s *Store) AdvanceTriggerCursor(ctx context.Context, triggerUID string, cursor time.Time) error {
	result, err := s.db.ExecContext(ctx, `UPDATE trigger_states SET cursor_at=?,updated_at=? WHERE trigger_uid=?`, cursor.UTC().Format(time.RFC3339Nano), s.timestamp(), triggerUID)
	if err != nil {
		return err
	}
	changed, _ := result.RowsAffected()
	if changed == 0 {
		return ErrNotFound
	}
	return nil
}

// SetTriggerEnabled applies an operator override without modifying resource spec.
func (s *Store) SetTriggerEnabled(ctx context.Context, triggerUID string, enabled bool) error {
	result, err := s.db.ExecContext(ctx, `UPDATE trigger_states SET enabled=?,updated_at=? WHERE trigger_uid=?`, boolInt(enabled), s.timestamp(), triggerUID)
	if err != nil {
		return err
	}
	changed, _ := result.RowsAffected()
	if changed == 0 {
		return ErrNotFound
	}
	return nil
}

// RecordTriggerSkip idempotently records a missed or concurrency-forbidden occurrence.
func (s *Store) RecordTriggerSkip(ctx context.Context, triggerUID, occurrenceID, reason string, scheduledAt time.Time) error {
	_, err := s.db.ExecContext(ctx, `INSERT OR IGNORE INTO trigger_skips(trigger_uid,occurrence_id,reason,scheduled_at,recorded_at) VALUES(?,?,?,?,?)`, triggerUID, occurrenceID, reason, scheduledAt.UTC().Format(time.RFC3339Nano), s.timestamp())
	return err
}

// TriggerSkips returns recent missed or concurrency-forbidden occurrences.
func (s *Store) TriggerSkips(ctx context.Context, triggerUID string, limit int) ([]TriggerSkip, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, `SELECT trigger_uid,occurrence_id,reason,scheduled_at FROM trigger_skips WHERE trigger_uid=? ORDER BY scheduled_at DESC LIMIT ?`, triggerUID, limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	result := []TriggerSkip{}
	for rows.Next() {
		var skip TriggerSkip
		var scheduled string
		if err := rows.Scan(&skip.TriggerUID, &skip.OccurrenceID, &skip.Reason, &scheduled); err != nil {
			return nil, err
		}
		skip.ScheduledAt, _ = time.Parse(time.RFC3339Nano, scheduled)
		result = append(result, skip)
	}
	return result, rows.Err()
}

// HasActiveRunForTrigger reports whether a receipt has a non-terminal local run.
func (s *Store) HasActiveRunForTrigger(ctx context.Context, triggerUID string) (bool, error) {
	var count int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM trigger_receipts tr LEFT JOIN runs r ON r.trigger_receipt_uid=tr.uid WHERE tr.trigger_uid=? AND (r.uid IS NULL OR r.phase NOT IN ('succeeded','failed','rejected','cancelled'))`, triggerUID).Scan(&count)
	return count > 0, err
}

// TriggerReceipts returns recent durable acknowledgements for one trigger.
func (s *Store) TriggerReceipts(ctx context.Context, triggerUID string, limit int) ([]Receipt, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, `SELECT uid,trigger_uid,occurrence_id,run_uid,deduplicated,accepted_at FROM trigger_receipts WHERE trigger_uid=? ORDER BY accepted_at DESC LIMIT ?`, triggerUID, limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	result := []Receipt{}
	for rows.Next() {
		var receipt Receipt
		var deduplicated int
		var accepted string
		if err := rows.Scan(&receipt.UID, &receipt.TriggerUID, &receipt.OccurrenceID, &receipt.RunUID, &deduplicated, &accepted); err != nil {
			return nil, err
		}
		receipt.Deduplicated = deduplicated == 1
		receipt.AcceptedAt, _ = time.Parse(time.RFC3339Nano, accepted)
		result = append(result, receipt)
	}
	return result, rows.Err()
}

// ProviderCursor returns the last durably acknowledged provider cursor.
func (s *Store) ProviderCursor(ctx context.Context, triggerUID string) (string, string, error) {
	var cursor, eventID string
	if err := s.db.QueryRowContext(ctx, `SELECT cursor,provider_event_id FROM provider_cursors WHERE trigger_uid=?`, triggerUID).Scan(&cursor, &eventID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", "", nil
		}
		return "", "", err
	}
	return cursor, eventID, nil
}

// ResourceByUID returns one editable resource by immutable identity.
func (s *Store) ResourceByUID(ctx context.Context, kind, uid string) (resource.Document, error) {
	var data []byte
	if err := s.db.QueryRowContext(ctx, `SELECT spec_json FROM resources WHERE kind=? AND uid=?`, kind, uid).Scan(&data); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return resource.Document{}, ErrNotFound
		}
		return resource.Document{}, err
	}
	document, err := resource.DecodeStrict(data)
	if err != nil {
		return resource.Document{}, err
	}
	return document, nil
}

// ApprovalState returns the persisted decision.
func (s *Store) ApprovalState(ctx context.Context, runUID, nodeID string) (string, error) {
	var state string
	if err := s.db.QueryRowContext(ctx, `SELECT state FROM approvals WHERE run_uid=? AND node_id=?`, runUID, nodeID).Scan(&state); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", ErrNotFound
		}
		return "", err
	}
	return state, nil
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}
