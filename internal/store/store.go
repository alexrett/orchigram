package store

import (
	"bytes"
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

// ErrTriggerDisabled rejects an occurrence after an operator has disabled its Trigger.
var ErrTriggerDisabled = errors.New("trigger is disabled")

// StaleTriggerGenerationError rejects events emitted by a superseded provider watch.
type StaleTriggerGenerationError struct {
	Expected uint64
	Current  uint64
}

func (e *StaleTriggerGenerationError) Error() string {
	return fmt.Sprintf("stale Trigger generation: watch=%d current=%d", e.Expected, e.Current)
}

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
	type migration struct {
		name    string
		version int
	}
	migrations := make([]migration, 0, len(entries))
	maximumSupported := 0
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		var version int
		if _, err := fmt.Sscanf(entry.Name(), "%d_", &version); err != nil {
			return fmt.Errorf("migration filename %q: %w", entry.Name(), err)
		}
		migrations = append(migrations, migration{name: entry.Name(), version: version})
		if version > maximumSupported {
			maximumSupported = version
		}
	}
	if len(migrations) == 0 {
		return errors.New("no embedded schema migrations")
	}
	var exists int
	if err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='schema_migrations'").Scan(&exists); err != nil {
		return fmt.Errorf("inspect migration table: %w", err)
	}
	if exists == 1 {
		var appliedVersion int
		if err := s.db.QueryRowContext(ctx, "SELECT COALESCE(MAX(version), 0) FROM schema_migrations").Scan(&appliedVersion); err != nil {
			return fmt.Errorf("inspect schema migration version: %w", err)
		}
		if appliedVersion > maximumSupported {
			return fmt.Errorf("database schema version %d is newer than supported version %d", appliedVersion, maximumSupported)
		}
	}
	for _, migration := range migrations {
		if exists == 1 {
			var applied int
			if err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM schema_migrations WHERE version = ?", migration.version).Scan(&applied); err != nil {
				return fmt.Errorf("check migration %d: %w", migration.version, err)
			}
			if applied == 1 {
				continue
			}
		}
		body, err := fs.ReadFile(Migrations, "migrations/"+migration.name)
		if err != nil {
			return fmt.Errorf("read migration %d: %w", migration.version, err)
		}
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, string(body)); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("apply migration %d: %w", migration.version, err)
		}
		if _, err := tx.ExecContext(ctx, "INSERT INTO schema_migrations(version, applied_at) VALUES (?, ?)", migration.version, s.timestamp()); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("record migration %d: %w", migration.version, err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit migration %d: %w", migration.version, err)
		}
		exists = 1
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
	nowTime := s.now().UTC()
	now := nowTime.Format(time.RFC3339Nano)
	if exists {
		_, err = tx.ExecContext(ctx, `UPDATE resources SET uid=?,resource_version=?,generation=?,labels_json=?,spec_json=?,status_json=?,updated_at=? WHERE kind=? AND namespace=? AND name=?`, meta.UID, revision, meta.Generation, labels, doc.JSON, doc.Status, now, doc.Kind, meta.Namespace, meta.Name)
	} else {
		_, err = tx.ExecContext(ctx, `INSERT INTO resources(kind,namespace,name,uid,resource_version,generation,labels_json,spec_json,status_json,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)`, doc.Kind, meta.Namespace, meta.Name, meta.UID, revision, meta.Generation, labels, doc.JSON, doc.Status, now, now)
	}
	if err != nil {
		return resource.Document{}, fmt.Errorf("write resource: %w", err)
	}
	if doc.Kind == "Trigger" {
		trigger, decodeErr := resource.DecodeTrigger(doc.JSON)
		if decodeErr != nil {
			return resource.Document{}, fmt.Errorf("decode applied Trigger: %w", decodeErr)
		}
		enabled := trigger.Spec.Enabled == nil || *trigger.Spec.Enabled
		if _, err := ensureTriggerStateTx(ctx, tx, trigger.Metadata.UID, trigger.Metadata.Generation, enabled, nowTime, now); err != nil {
			return resource.Document{}, fmt.Errorf("initialize Trigger state: %w", err)
		}
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
	return s.delete(ctx, kind, namespace, name, expected, requestID, nil)
}

func (s *Store) delete(ctx context.Context, kind, namespace, name string, expected uint64, requestID string, afterCommit func()) error {
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
	if kind == "Trigger" {
		if _, err := tx.ExecContext(ctx, `DELETE FROM provider_cursors WHERE trigger_uid=?`, uid); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM trigger_states WHERE trigger_uid=?`, uid); err != nil {
			return err
		}
	}
	now := s.timestamp()
	if _, err := tx.ExecContext(ctx, `INSERT INTO resource_events(revision,event_type,kind,namespace,name,uid,resource_json,observed_at) VALUES(?,?,?,?,?,?,NULL,?)`, revision, "DELETED", kind, namespace, name, uid, now); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO audit_records(revision,request_id,actor,context_name,operation,resource_uid,old_hash,new_hash,occurred_at) VALUES(?,?,?,?,?,?,?,?,?)`, revision, valueOr(requestID, uuid.NewString()), "unix-peer", "", "deleted", uid, digest(data), "", now); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	if afterCommit != nil {
		afterCommit()
	}
	return nil
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
	PlanHash       string          `json:"planHash,omitempty"`
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
	return s.acceptTrigger(ctx, triggerUID, 0, occurrenceID, flowName, namespace, input, deduplicated, "", "", nil, nil)
}

// AcceptTriggerWithPlan persists an immutable compiled plan in the same
// transaction as the trigger receipt and start command. Runtime acceptors must
// use this method; AcceptTrigger remains for store-level compatibility tests
// and pre-v0.1 databases whose commands do not yet carry a plan hash.
func (s *Store) AcceptTriggerWithPlan(ctx context.Context, triggerUID, occurrenceID, flowName, namespace string, input json.RawMessage, deduplicated bool, plan flow.ExecutionPlan) (Receipt, error) {
	return s.acceptTrigger(ctx, triggerUID, 0, occurrenceID, flowName, namespace, input, deduplicated, "", "", nil, &plan)
}

// AcceptProviderTrigger persists receipt, outbox, and provider cursor atomically.
func (s *Store) AcceptProviderTrigger(ctx context.Context, triggerUID string, triggerGeneration uint64, occurrenceID, flowName, namespace string, input json.RawMessage, cursor string) (Receipt, error) {
	return s.acceptProviderTrigger(ctx, triggerUID, triggerGeneration, occurrenceID, flowName, namespace, input, cursor, nil)
}

// AcceptProviderTriggerWithPlan atomically persists a provider cursor, receipt,
// immutable plan, and outbox command after validating the Trigger generation.
func (s *Store) AcceptProviderTriggerWithPlan(ctx context.Context, triggerUID string, triggerGeneration uint64, occurrenceID, flowName, namespace string, input json.RawMessage, cursor string, plan flow.ExecutionPlan) (Receipt, error) {
	return s.acceptTrigger(ctx, triggerUID, triggerGeneration, occurrenceID, flowName, namespace, input, true, cursor, occurrenceID, nil, &plan)
}

func (s *Store) acceptProviderTrigger(ctx context.Context, triggerUID string, triggerGeneration uint64, occurrenceID, flowName, namespace string, input json.RawMessage, cursor string, afterValidation func()) (Receipt, error) {
	return s.acceptTrigger(ctx, triggerUID, triggerGeneration, occurrenceID, flowName, namespace, input, true, cursor, occurrenceID, afterValidation, nil)
}

func (s *Store) acceptTrigger(ctx context.Context, triggerUID string, triggerGeneration uint64, occurrenceID, flowName, namespace string, input json.RawMessage, deduplicated bool, providerCursor, providerEventID string, afterProviderValidation func(), plan *flow.ExecutionPlan) (Receipt, error) {
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
	if providerCursor != "" {
		var resourceGeneration, stateGeneration uint64
		var enabled int
		if err := tx.QueryRowContext(ctx, `SELECT resources.generation,trigger_states.generation,trigger_states.enabled
			FROM resources JOIN trigger_states ON trigger_states.trigger_uid=resources.uid
			WHERE resources.kind='Trigger' AND resources.uid=?`, triggerUID).Scan(&resourceGeneration, &stateGeneration, &enabled); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return Receipt{}, ErrNotFound
			}
			return Receipt{}, err
		}
		if resourceGeneration != triggerGeneration {
			return Receipt{}, &StaleTriggerGenerationError{Expected: triggerGeneration, Current: resourceGeneration}
		}
		if stateGeneration != triggerGeneration {
			return Receipt{}, &StaleTriggerGenerationError{Expected: triggerGeneration, Current: stateGeneration}
		}
		if enabled == 0 {
			return Receipt{}, ErrTriggerDisabled
		}
		if afterProviderValidation != nil {
			afterProviderValidation()
		}
	}
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
	if plan != nil {
		if plan.PlanHash == "" || plan.FlowUID == "" || plan.InterpreterVersion == "" {
			return Receipt{}, errors.New("accepted execution plan is incomplete")
		}
		planJSON, marshalErr := json.Marshal(plan)
		if marshalErr != nil {
			return Receipt{}, fmt.Errorf("encode accepted execution plan: %w", marshalErr)
		}
		if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO compiled_plans(plan_hash,flow_uid,flow_generation,interpreter_version,plan_json,created_at) VALUES(?,?,?,?,?,?)`, plan.PlanHash, plan.FlowUID, plan.FlowGeneration, plan.InterpreterVersion, planJSON, s.timestamp()); err != nil {
			return Receipt{}, err
		}
	}
	now := s.timestamp()
	receipt := Receipt{UID: uuid.NewString(), TriggerUID: triggerUID, OccurrenceID: occurrenceID, RunUID: uuid.NewString(), Deduplicated: deduplicated}
	receipt.AcceptedAt, _ = time.Parse(time.RFC3339Nano, now)
	payload := StartPayload{RunUID: receipt.RunUID, ReceiptUID: receipt.UID, FlowName: flowName, Namespace: namespace, Input: input, IdempotencyKey: "trigger/" + triggerUID + "/" + occurrenceID}
	if plan != nil {
		payload.PlanHash = plan.PlanHash
	}
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

// OutboxStatus is the secret-free readiness projection for durable dispatch.
type OutboxStatus struct {
	Active int
	Failed int
}

// InspectOutboxStatus reports unresolved commands and retry failures without
// exposing command payloads or their dependency error text.
func (s *Store) InspectOutboxStatus(ctx context.Context) (OutboxStatus, error) {
	var result OutboxStatus
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(SUM(CASE WHEN last_error!='' THEN 1 ELSE 0 END),0) FROM outbox WHERE state!='completed'`).Scan(&result.Active, &result.Failed)
	return result, err
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

// CompleteStartIfRunCancelled suppresses a claimed start after cancellation.
// The conditional update is the serialization point with cancellation: when it
// does not complete the command, the outbox remains capable of starting the
// workflow until the normal dispatch path completes it.
func (s *Store) CompleteStartIfRunCancelled(ctx context.Context, id int64, runUID string) (bool, error) {
	result, err := s.db.ExecContext(ctx, `UPDATE outbox SET state='completed',completed_at=?,last_error='' WHERE id=? AND command_type='start-run' AND aggregate_uid=? AND EXISTS (SELECT 1 FROM run_cancellations WHERE run_uid=?)`, s.timestamp(), id, runUID, runUID)
	if err != nil {
		return false, err
	}
	updated, err := result.RowsAffected()
	return updated == 1, err
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

// NodeAttempt is one durable physical execution of a logical node iteration.
type NodeAttempt struct {
	RunUID           string
	NodeID           string
	LogicalIteration int
	Attempt          uint32
	FrameworkAttempt uint32
	Phase            string
	IdempotencyKey   string
	Input            json.RawMessage
	Output           json.RawMessage
	ErrorText        string
	ExitOutcome      string
	StartedAt        time.Time
	CompletedAt      time.Time
}

// BeginNodeAttempt durably records one physical attempt before an external
// invocation. Re-delivery replays a terminal record; an incomplete delivery is
// marked lost and receives a new monotonic physical attempt.
func (s *Store) BeginNodeAttempt(ctx context.Context, runUID, nodeID string, logicalIteration int, frameworkAttempt uint32, idempotencyKey string, input json.RawMessage) (NodeAttempt, bool, error) {
	if logicalIteration < 0 || frameworkAttempt == 0 || idempotencyKey == "" {
		return NodeAttempt{}, false, errors.New("node attempt identity is incomplete")
	}
	if len(input) == 0 {
		input = json.RawMessage(`{}`)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return NodeAttempt{}, false, err
	}
	defer func() { _ = tx.Rollback() }()
	latest, err := nodeAttemptForFrameworkTx(ctx, tx, runUID, nodeID, logicalIteration, frameworkAttempt)
	if err == nil && !latest.CompletedAt.IsZero() {
		return latest, false, tx.Commit()
	}
	if err != nil && !errors.Is(err, ErrNotFound) {
		return NodeAttempt{}, false, err
	}
	hasIncompleteDelivery := err == nil
	var runPhase string
	if err := tx.QueryRowContext(ctx, `SELECT phase FROM runs WHERE uid=?`, runUID).Scan(&runPhase); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return NodeAttempt{}, false, ErrNotFound
		}
		return NodeAttempt{}, false, err
	}
	if terminalRunPhase(runPhase) {
		return NodeAttempt{}, false, fmt.Errorf("run %s is already %s", runUID, runPhase)
	}
	now := s.timestamp()
	if hasIncompleteDelivery {
		const lostMessage = "activity delivery ended without a durable completion"
		if _, err := tx.ExecContext(ctx, `UPDATE node_attempts SET phase='failed',error_text=?,exit_outcome='delivery-lost',completed_at=? WHERE run_uid=? AND node_id=? AND logical_iteration=? AND attempt=? AND completed_at IS NULL`, lostMessage, now, runUID, nodeID, logicalIteration, latest.Attempt); err != nil {
			return NodeAttempt{}, false, err
		}
		payload, _ := json.Marshal(map[string]any{"error": lostMessage, "outcome": "delivery-lost"})
		if _, err := s.appendRunEventTx(ctx, tx, runUID, nodeID, "node.failed", "running", latest.Attempt, payload, now); err != nil {
			return NodeAttempt{}, false, err
		}
	}
	var physicalAttempt int64
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(attempt),0)+1 FROM node_attempts WHERE run_uid=? AND node_id=? AND logical_iteration=?`, runUID, nodeID, logicalIteration).Scan(&physicalAttempt); err != nil {
		return NodeAttempt{}, false, err
	}
	if physicalAttempt <= 0 || physicalAttempt > int64(^uint32(0)) {
		return NodeAttempt{}, false, errors.New("node attempt sequence is exhausted")
	}
	attempt := uint32(physicalAttempt) //nolint:gosec // Explicit range check above.
	if _, err := tx.ExecContext(ctx, `INSERT INTO node_attempts(run_uid,node_id,logical_iteration,attempt,framework_attempt,phase,idempotency_key,input_json,started_at) VALUES(?,?,?,?,?,?,?,?,?)`, runUID, nodeID, logicalIteration, attempt, frameworkAttempt, "running", idempotencyKey, input, now); err != nil {
		return NodeAttempt{}, false, err
	}
	if _, err := s.appendRunEventTx(ctx, tx, runUID, nodeID, "node.started", "running", attempt, mustJSONObject(nil), now); err != nil {
		return NodeAttempt{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return NodeAttempt{}, false, err
	}
	started, _ := time.Parse(time.RFC3339Nano, now)
	return NodeAttempt{RunUID: runUID, NodeID: nodeID, LogicalIteration: logicalIteration, Attempt: attempt, FrameworkAttempt: frameworkAttempt, Phase: "running", IdempotencyKey: idempotencyKey, Input: input, StartedAt: started}, true, nil
}

// CompleteNodeAttempt records the first terminal outcome and appends exactly
// one node transition. Re-delivery returns the stored outcome unchanged.
func (s *Store) CompleteNodeAttempt(ctx context.Context, runUID, nodeID string, logicalIteration int, attempt uint32, phase string, output json.RawMessage, errorText, exitOutcome string) (NodeAttempt, error) {
	if phase != "succeeded" && phase != "failed" && phase != "cancelled" {
		return NodeAttempt{}, fmt.Errorf("invalid node attempt phase %q", phase)
	}
	if len(output) == 0 {
		output = json.RawMessage(`{}`)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return NodeAttempt{}, err
	}
	defer func() { _ = tx.Rollback() }()
	existing, err := nodeAttemptTx(ctx, tx, runUID, nodeID, logicalIteration, attempt)
	if err != nil {
		return NodeAttempt{}, err
	}
	if !existing.CompletedAt.IsZero() {
		return existing, tx.Commit()
	}
	effectiveOutcome := exitOutcome
	if existing.ExitOutcome != "" {
		effectiveOutcome = existing.ExitOutcome
	}
	now := s.timestamp()
	if _, err := tx.ExecContext(ctx, `UPDATE node_attempts SET phase=?,output_json=?,error_text=?,exit_outcome=CASE WHEN exit_outcome='' THEN ? ELSE exit_outcome END,completed_at=? WHERE run_uid=? AND node_id=? AND logical_iteration=? AND attempt=? AND completed_at IS NULL`, phase, output, errorText, exitOutcome, now, runUID, nodeID, logicalIteration, attempt); err != nil {
		return NodeAttempt{}, err
	}
	eventType := "node.completed"
	payload := output
	if phase != "succeeded" {
		eventType = "node.failed"
		payload, _ = json.Marshal(map[string]any{"error": errorText, "outcome": effectiveOutcome})
	}
	if _, err := s.appendRunEventTx(ctx, tx, runUID, nodeID, eventType, "running", attempt, payload, now); err != nil {
		return NodeAttempt{}, err
	}
	if err := tx.Commit(); err != nil {
		return NodeAttempt{}, err
	}
	return s.NodeAttempt(ctx, runUID, nodeID, logicalIteration, attempt)
}

// SetNodeAttemptOutcome records plugin-specific process or transport outcome
// before the engine records the terminal activity result.
func (s *Store) SetNodeAttemptOutcome(ctx context.Context, runUID, nodeID string, logicalIteration int, attempt uint32, outcome string) error {
	if outcome == "" {
		return nil
	}
	_, err := s.db.ExecContext(ctx, `UPDATE node_attempts SET exit_outcome=? WHERE run_uid=? AND node_id=? AND logical_iteration=? AND attempt=? AND exit_outcome=''`, outcome, runUID, nodeID, logicalIteration, attempt)
	return err
}

// NodeAttempt returns one durable attempt.
func (s *Store) NodeAttempt(ctx context.Context, runUID, nodeID string, logicalIteration int, attempt uint32) (NodeAttempt, error) {
	return nodeAttemptQuery(ctx, s.db, runUID, nodeID, logicalIteration, attempt)
}

// ListNodeAttempts returns attempts in logical and physical execution order.
func (s *Store) ListNodeAttempts(ctx context.Context, runUID, nodeID string, limit int) ([]NodeAttempt, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	query := `SELECT run_uid,node_id,logical_iteration,attempt,framework_attempt,phase,idempotency_key,input_json,output_json,error_text,exit_outcome,started_at,completed_at FROM node_attempts WHERE run_uid=?`
	arguments := []any{runUID}
	if nodeID != "" {
		query += ` AND node_id=?`
		arguments = append(arguments, nodeID)
	}
	query += ` ORDER BY logical_iteration,attempt,node_id LIMIT ?`
	arguments = append(arguments, limit)
	rows, err := s.db.QueryContext(ctx, query, arguments...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	result := []NodeAttempt{}
	for rows.Next() {
		attempt, scanErr := scanNodeAttempt(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		result = append(result, attempt)
	}
	return result, rows.Err()
}

func nodeAttemptTx(ctx context.Context, tx *sql.Tx, runUID, nodeID string, logicalIteration int, attempt uint32) (NodeAttempt, error) {
	return nodeAttemptQuery(ctx, tx, runUID, nodeID, logicalIteration, attempt)
}

func nodeAttemptForFrameworkTx(ctx context.Context, tx *sql.Tx, runUID, nodeID string, logicalIteration int, frameworkAttempt uint32) (NodeAttempt, error) {
	query := `SELECT run_uid,node_id,logical_iteration,attempt,framework_attempt,phase,idempotency_key,input_json,output_json,error_text,exit_outcome,started_at,completed_at FROM node_attempts WHERE run_uid=? AND node_id=? AND logical_iteration=? AND framework_attempt=? ORDER BY attempt DESC LIMIT 1`
	return scanNodeAttempt(tx.QueryRowContext(ctx, query, runUID, nodeID, logicalIteration, frameworkAttempt))
}

type rowScanner interface {
	Scan(...any) error
}

type rowQueryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func nodeAttemptQuery(ctx context.Context, queryer rowQueryer, runUID, nodeID string, logicalIteration int, attempt uint32) (NodeAttempt, error) {
	return scanNodeAttempt(queryer.QueryRowContext(ctx, `SELECT run_uid,node_id,logical_iteration,attempt,framework_attempt,phase,idempotency_key,input_json,output_json,error_text,exit_outcome,started_at,completed_at FROM node_attempts WHERE run_uid=? AND node_id=? AND logical_iteration=? AND attempt=?`, runUID, nodeID, logicalIteration, attempt))
}

func scanNodeAttempt(row rowScanner) (NodeAttempt, error) {
	var result NodeAttempt
	var output []byte
	var started string
	var completed sql.NullString
	if err := row.Scan(&result.RunUID, &result.NodeID, &result.LogicalIteration, &result.Attempt, &result.FrameworkAttempt, &result.Phase, &result.IdempotencyKey, &result.Input, &output, &result.ErrorText, &result.ExitOutcome, &started, &completed); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return NodeAttempt{}, ErrNotFound
		}
		return NodeAttempt{}, err
	}
	result.Output = output
	result.StartedAt, _ = time.Parse(time.RFC3339Nano, started)
	if completed.Valid {
		result.CompletedAt, _ = time.Parse(time.RFC3339Nano, completed.String)
	}
	return result, nil
}

// AppendPluginEvent persists one validated provider event and projects it into
// the existing run event stream. Duplicate stream sequences are idempotent.
func (s *Store) AppendPluginEvent(ctx context.Context, runUID, nodeID string, logicalIteration int, attempt uint32, sequence uint64, eventType string, payload json.RawMessage, occurredAt time.Time) error {
	if sequence == 0 || eventType == "" || !json.Valid(payload) {
		return errors.New("plugin event is incomplete")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	when := occurredAt.UTC().Format(time.RFC3339Nano)
	result, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO plugin_events(run_uid,node_id,logical_iteration,attempt,sequence,event_type,payload_json,occurred_at) VALUES(?,?,?,?,?,?,?,?)`, runUID, nodeID, logicalIteration, attempt, sequence, eventType, payload, when)
	if err != nil {
		return err
	}
	inserted, _ := result.RowsAffected()
	if inserted == 0 {
		var storedType, storedWhen string
		var storedPayload []byte
		if err := tx.QueryRowContext(ctx, `SELECT event_type,payload_json,occurred_at FROM plugin_events WHERE run_uid=? AND node_id=? AND logical_iteration=? AND attempt=? AND sequence=?`, runUID, nodeID, logicalIteration, attempt, sequence).Scan(&storedType, &storedPayload, &storedWhen); err != nil {
			return err
		}
		if storedType != eventType || storedWhen != when || !bytes.Equal(storedPayload, payload) {
			return errors.New("plugin event sequence conflicts with durable evidence")
		}
	} else {
		envelope, _ := json.Marshal(map[string]any{"sequence": sequence, "occurredAt": when, "payload": json.RawMessage(payload)})
		if err := s.appendRunEvidenceTx(ctx, tx, runUID, nodeID, "plugin."+eventType, attempt, envelope, s.timestamp()); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) appendRunEvidenceTx(ctx context.Context, tx *sql.Tx, runUID, nodeID, eventType string, attempt uint32, payload json.RawMessage, occurredAt string) error {
	var currentPhase string
	if err := tx.QueryRowContext(ctx, `SELECT phase FROM runs WHERE uid=?`, runUID).Scan(&currentPhase); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		return err
	}
	var sequence uint64
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(sequence),0)+1 FROM run_events WHERE run_uid=?`, runUID).Scan(&sequence); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO run_events(run_uid,sequence,node_id,attempt,event_type,payload_json,occurred_at) VALUES(?,?,?,?,?,?,?)`, runUID, sequence, nodeID, attempt, eventType, payload, occurredAt); err != nil {
		return err
	}
	if !terminalRunPhase(currentPhase) {
		_, err := tx.ExecContext(ctx, `UPDATE runs SET updated_at=? WHERE uid=?`, occurredAt, runUID)
		return err
	}
	return nil
}

// ArtifactRecord is durable metadata for a private run artifact.
type ArtifactRecord struct {
	UID              string
	RunUID           string
	NodeID           string
	LogicalIteration int
	Attempt          uint32
	Name             string
	MediaType        string
	RelativePath     string
	SizeBytes        int64
	SHA256           string
	CreatedAt        time.Time
	UpdatedAt        time.Time
}

// PutArtifact upserts metadata for one immutable attempt-local artifact name.
func (s *Store) PutArtifact(ctx context.Context, artifact ArtifactRecord) (ArtifactRecord, error) {
	if artifact.RunUID == "" || artifact.NodeID == "" || artifact.LogicalIteration < 0 || artifact.Attempt == 0 || artifact.Name == "" || artifact.RelativePath == "" || artifact.SizeBytes < 0 {
		return ArtifactRecord{}, errors.New("artifact metadata is incomplete")
	}
	decodedDigest, err := hex.DecodeString(artifact.SHA256)
	if err != nil || len(decodedDigest) != sha256.Size {
		return ArtifactRecord{}, errors.New("artifact sha256 is invalid")
	}
	cleanPath := filepath.Clean(filepath.FromSlash(artifact.RelativePath))
	if filepath.IsAbs(cleanPath) || cleanPath == "." || cleanPath == ".." || strings.HasPrefix(cleanPath, ".."+string(filepath.Separator)) {
		return ArtifactRecord{}, errors.New("artifact path escapes the state directory")
	}
	artifact.RelativePath = filepath.ToSlash(cleanPath)
	if artifact.UID == "" {
		artifact.UID = uuid.NewString()
	}
	if artifact.MediaType == "" {
		artifact.MediaType = "application/octet-stream"
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ArtifactRecord{}, err
	}
	defer func() { _ = tx.Rollback() }()
	var completed sql.NullString
	if err := tx.QueryRowContext(ctx, `SELECT completed_at FROM node_attempts WHERE run_uid=? AND node_id=? AND logical_iteration=? AND attempt=?`, artifact.RunUID, artifact.NodeID, artifact.LogicalIteration, artifact.Attempt).Scan(&completed); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ArtifactRecord{}, ErrNotFound
		}
		return ArtifactRecord{}, err
	}
	existing, existingErr := scanArtifact(tx.QueryRowContext(ctx, `SELECT uid,run_uid,node_id,logical_iteration,attempt,name,media_type,relative_path,size_bytes,sha256,created_at,updated_at FROM artifacts WHERE run_uid=? AND node_id=? AND logical_iteration=? AND attempt=? AND name=?`, artifact.RunUID, artifact.NodeID, artifact.LogicalIteration, artifact.Attempt, artifact.Name))
	if existingErr == nil {
		artifact.UID = existing.UID
		if completed.Valid {
			if existing.MediaType != artifact.MediaType || existing.RelativePath != artifact.RelativePath || existing.SizeBytes != artifact.SizeBytes || existing.SHA256 != artifact.SHA256 {
				return ArtifactRecord{}, errors.New("completed attempt artifact differs from durable metadata")
			}
			return existing, tx.Commit()
		}
	} else if !errors.Is(existingErr, ErrNotFound) {
		return ArtifactRecord{}, existingErr
	}
	now := s.timestamp()
	if _, err := tx.ExecContext(ctx, `INSERT INTO artifacts(uid,run_uid,node_id,logical_iteration,attempt,name,media_type,relative_path,size_bytes,sha256,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(run_uid,node_id,logical_iteration,attempt,name) DO UPDATE SET media_type=excluded.media_type,relative_path=excluded.relative_path,size_bytes=excluded.size_bytes,sha256=excluded.sha256,updated_at=excluded.updated_at`, artifact.UID, artifact.RunUID, artifact.NodeID, artifact.LogicalIteration, artifact.Attempt, artifact.Name, artifact.MediaType, artifact.RelativePath, artifact.SizeBytes, artifact.SHA256, now, now); err != nil {
		return ArtifactRecord{}, err
	}
	if err := tx.Commit(); err != nil {
		return ArtifactRecord{}, err
	}
	return s.Artifact(ctx, artifact.RunUID, artifact.NodeID, artifact.LogicalIteration, artifact.Attempt, artifact.Name)
}

// Artifact returns one attempt-local artifact by coordinates.
func (s *Store) Artifact(ctx context.Context, runUID, nodeID string, logicalIteration int, attempt uint32, name string) (ArtifactRecord, error) {
	return scanArtifact(s.db.QueryRowContext(ctx, `SELECT uid,run_uid,node_id,logical_iteration,attempt,name,media_type,relative_path,size_bytes,sha256,created_at,updated_at FROM artifacts WHERE run_uid=? AND node_id=? AND logical_iteration=? AND attempt=? AND name=?`, runUID, nodeID, logicalIteration, attempt, name))
}

// ArtifactByUID resolves server download metadata.
func (s *Store) ArtifactByUID(ctx context.Context, uid string) (ArtifactRecord, error) {
	return scanArtifact(s.db.QueryRowContext(ctx, `SELECT uid,run_uid,node_id,logical_iteration,attempt,name,media_type,relative_path,size_bytes,sha256,created_at,updated_at FROM artifacts WHERE uid=?`, uid))
}

// ListArtifacts returns run artifacts in execution order.
func (s *Store) ListArtifacts(ctx context.Context, runUID string, limit int) ([]ArtifactRecord, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, `SELECT uid,run_uid,node_id,logical_iteration,attempt,name,media_type,relative_path,size_bytes,sha256,created_at,updated_at FROM artifacts WHERE run_uid=? ORDER BY logical_iteration,attempt,node_id,name LIMIT ?`, runUID, limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	result := []ArtifactRecord{}
	for rows.Next() {
		artifact, scanErr := scanArtifact(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		result = append(result, artifact)
	}
	return result, rows.Err()
}

func scanArtifact(row rowScanner) (ArtifactRecord, error) {
	var artifact ArtifactRecord
	var created, updated string
	if err := row.Scan(&artifact.UID, &artifact.RunUID, &artifact.NodeID, &artifact.LogicalIteration, &artifact.Attempt, &artifact.Name, &artifact.MediaType, &artifact.RelativePath, &artifact.SizeBytes, &artifact.SHA256, &created, &updated); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ArtifactRecord{}, ErrNotFound
		}
		return ArtifactRecord{}, err
	}
	artifact.CreatedAt, _ = time.Parse(time.RFC3339Nano, created)
	artifact.UpdatedAt, _ = time.Parse(time.RFC3339Nano, updated)
	return artifact, nil
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
	_, err = s.appendRunEventTx(ctx, tx, runUID, nodeID, eventType, phase, attempt, payload, s.timestamp())
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) appendRunEventTx(ctx context.Context, tx *sql.Tx, runUID, nodeID, eventType, phase string, attempt uint32, payload json.RawMessage, occurredAt string) (bool, error) {
	var currentPhase string
	if err := tx.QueryRowContext(ctx, `SELECT phase FROM runs WHERE uid=?`, runUID).Scan(&currentPhase); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, ErrNotFound
		}
		return false, err
	}
	if terminalRunPhase(currentPhase) {
		return false, nil
	}
	var sequence uint64
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(sequence),0)+1 FROM run_events WHERE run_uid=?`, runUID).Scan(&sequence); err != nil {
		return false, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO run_events(run_uid,sequence,node_id,attempt,event_type,payload_json,occurred_at) VALUES(?,?,?,?,?,?,?)`, runUID, sequence, nodeID, attempt, eventType, payload, occurredAt); err != nil {
		return false, err
	}
	completed := any(nil)
	if phase == "succeeded" || phase == "failed" || phase == "cancelled" || phase == "rejected" {
		completed = occurredAt
	}
	if _, err := tx.ExecContext(ctx, `UPDATE runs SET phase=?,updated_at=?,completed_at=COALESCE(?,completed_at) WHERE uid=?`, phase, occurredAt, completed, runUID); err != nil {
		return false, err
	}
	return true, nil
}

func mustJSONObject(payload json.RawMessage) json.RawMessage {
	if len(payload) == 0 {
		return json.RawMessage(`{}`)
	}
	return payload
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

// MarkRunCancellationDeliveredIfStartImpossible acknowledges a missing
// workflow only after every durable start command for the Run is completed.
// Keeping the predicate in the update prevents an outbox/cancellation race
// from losing the cancellation intent.
func (s *Store) MarkRunCancellationDeliveredIfStartImpossible(ctx context.Context, runUID string) (bool, error) {
	result, err := s.db.ExecContext(ctx, `UPDATE run_cancellations SET delivery_completed=1,delivered_at=? WHERE run_uid=? AND delivery_completed=0 AND NOT EXISTS (SELECT 1 FROM outbox WHERE command_type='start-run' AND aggregate_uid=? AND state!='completed')`, s.timestamp(), runUID, runUID)
	if err != nil {
		return false, err
	}
	updated, err := result.RowsAffected()
	return updated == 1, err
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
	Name           string
	Version        string
	Digest         string
	ManifestJSON   json.RawMessage
	ContractJSON   json.RawMessage
	ContractDigest string
	State          string
	InstalledAt    time.Time
	Active         bool
}

// PutPlugin records one immutable plugin version idempotently.
func (s *Store) PutPlugin(ctx context.Context, record PluginRecord) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	var existingDigest, existingContractDigest string
	var existingContract []byte
	err = tx.QueryRowContext(ctx, `SELECT digest,contract_json,contract_digest FROM plugin_installations WHERE name=? AND version=?`, record.Name, record.Version).Scan(&existingDigest, &existingContract, &existingContractDigest)
	if err == nil {
		if existingDigest != record.Digest {
			return fmt.Errorf("plugin %s version %s already has digest %s", record.Name, record.Version, existingDigest)
		}
		if existingContractDigest == "" && len(existingContract) == 0 && record.ContractDigest != "" {
			if _, err := tx.ExecContext(ctx, `UPDATE plugin_installations SET contract_json=?,contract_digest=? WHERE name=? AND version=? AND contract_digest=''`, []byte(record.ContractJSON), record.ContractDigest, record.Name, record.Version); err != nil {
				return err
			}
		} else if existingContractDigest != record.ContractDigest {
			return fmt.Errorf("plugin %s version %s action contract changed under the same immutable digest", record.Name, record.Version)
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
	if record.ContractDigest == "" || len(record.ContractJSON) == 0 {
		return errors.New("plugin action contract is required")
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO plugin_installations(name,version,digest,manifest_json,contract_json,contract_digest,state,installed_at) VALUES(?,?,?,?,?,?,?,?)`, record.Name, record.Version, record.Digest, []byte(record.ManifestJSON), []byte(record.ContractJSON), record.ContractDigest, state, installedAt.Format(time.RFC3339Nano)); err != nil {
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
	query := `SELECT p.name,p.version,p.digest,p.manifest_json,p.contract_json,p.contract_digest,p.state,p.installed_at,CASE WHEN a.version=p.version THEN 1 ELSE 0 END FROM plugin_installations p LEFT JOIN plugin_activations a ON a.name=p.name WHERE p.name=? AND p.version=?`
	arguments := []any{name, version}
	if version == "" {
		query = `SELECT p.name,p.version,p.digest,p.manifest_json,p.contract_json,p.contract_digest,p.state,p.installed_at,1 FROM plugin_activations a JOIN plugin_installations p ON p.name=a.name AND p.version=a.version WHERE p.name=?`
		arguments = []any{name}
	}
	var record PluginRecord
	var installed string
	var active int
	if err := s.db.QueryRowContext(ctx, query, arguments...).Scan(&record.Name, &record.Version, &record.Digest, &record.ManifestJSON, &record.ContractJSON, &record.ContractDigest, &record.State, &installed, &active); err != nil {
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
	rows, err := s.db.QueryContext(ctx, `SELECT p.name,p.version,p.digest,p.manifest_json,p.contract_json,p.contract_digest,p.state,p.installed_at,CASE WHEN a.version=p.version THEN 1 ELSE 0 END FROM plugin_installations p LEFT JOIN plugin_activations a ON a.name=p.name ORDER BY p.name,p.version`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	result := []PluginRecord{}
	for rows.Next() {
		var record PluginRecord
		var installed string
		var active int
		if err := rows.Scan(&record.Name, &record.Version, &record.Digest, &record.ManifestJSON, &record.ContractJSON, &record.ContractDigest, &record.State, &installed, &active); err != nil {
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
	state, err := ensureTriggerStateTx(ctx, tx, triggerUID, generation, enabled, initialCursor, s.timestamp())
	if err != nil {
		return TriggerState{}, err
	}
	return state, tx.Commit()
}

func ensureTriggerStateTx(ctx context.Context, tx *sql.Tx, triggerUID string, generation uint64, enabled bool, initialCursor time.Time, updatedAt string) (TriggerState, error) {
	var state TriggerState
	var enabledValue int
	var cursor string
	err := tx.QueryRowContext(ctx, `SELECT trigger_uid,generation,enabled,cursor_at FROM trigger_states WHERE trigger_uid=?`, triggerUID).Scan(&state.TriggerUID, &state.Generation, &enabledValue, &cursor)
	if errors.Is(err, sql.ErrNoRows) {
		if _, err := tx.ExecContext(ctx, `INSERT INTO trigger_states(trigger_uid,generation,enabled,cursor_at,updated_at) VALUES(?,?,?,?,?)`, triggerUID, generation, boolInt(enabled), initialCursor.UTC().Format(time.RFC3339Nano), updatedAt); err != nil {
			return TriggerState{}, err
		}
		state = TriggerState{TriggerUID: triggerUID, Generation: generation, Enabled: enabled, CursorAt: initialCursor.UTC()}
		return state, nil
	}
	if err != nil {
		return TriggerState{}, err
	}
	state.Enabled = enabledValue == 1
	state.CursorAt, _ = time.Parse(time.RFC3339Nano, cursor)
	if state.Generation != generation {
		if state.Generation > generation {
			return TriggerState{}, &StaleTriggerGenerationError{Expected: generation, Current: state.Generation}
		}
		if _, err := tx.ExecContext(ctx, `UPDATE trigger_states SET generation=?,enabled=?,cursor_at=?,updated_at=? WHERE trigger_uid=?`, generation, boolInt(enabled), initialCursor.UTC().Format(time.RFC3339Nano), updatedAt, triggerUID); err != nil {
			return TriggerState{}, err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM provider_cursors WHERE trigger_uid=?`, triggerUID); err != nil {
			return TriggerState{}, err
		}
		state.Generation, state.Enabled, state.CursorAt = generation, enabled, initialCursor.UTC()
	}
	return state, nil
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
	return s.setTriggerEnabled(ctx, triggerUID, enabled, nil)
}

func (s *Store) setTriggerEnabled(ctx context.Context, triggerUID string, enabled bool, afterCommit func()) error {
	result, err := s.db.ExecContext(ctx, `UPDATE trigger_states SET enabled=?,updated_at=? WHERE trigger_uid=?`, boolInt(enabled), s.timestamp(), triggerUID)
	if err != nil {
		return err
	}
	changed, _ := result.RowsAffected()
	if changed == 0 {
		return ErrNotFound
	}
	if afterCommit != nil {
		afterCommit()
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
