PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS schema_migrations (
    version INTEGER PRIMARY KEY,
    applied_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS revisions (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    value INTEGER NOT NULL
);
INSERT OR IGNORE INTO revisions(singleton, value) VALUES (1, 0);

CREATE TABLE IF NOT EXISTS resources (
    kind TEXT NOT NULL,
    namespace TEXT NOT NULL,
    name TEXT NOT NULL,
    uid TEXT NOT NULL UNIQUE,
    resource_version INTEGER NOT NULL,
    generation INTEGER NOT NULL,
    labels_json BLOB NOT NULL,
    spec_json BLOB NOT NULL,
    status_json BLOB NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY(kind, namespace, name)
);

CREATE TABLE IF NOT EXISTS resource_events (
    revision INTEGER PRIMARY KEY,
    event_type TEXT NOT NULL,
    kind TEXT NOT NULL,
    namespace TEXT NOT NULL,
    name TEXT NOT NULL,
    uid TEXT NOT NULL,
    resource_json BLOB,
    observed_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS audit_records (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    revision INTEGER NOT NULL,
    request_id TEXT NOT NULL,
    actor TEXT NOT NULL,
    context_name TEXT NOT NULL,
    operation TEXT NOT NULL,
    resource_uid TEXT NOT NULL,
    old_hash TEXT NOT NULL,
    new_hash TEXT NOT NULL,
    occurred_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS compiled_plans (
    plan_hash TEXT PRIMARY KEY,
    flow_uid TEXT NOT NULL,
    flow_generation INTEGER NOT NULL,
    interpreter_version TEXT NOT NULL,
    plan_json BLOB NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS trigger_receipts (
    uid TEXT PRIMARY KEY,
    trigger_uid TEXT NOT NULL,
    occurrence_id TEXT NOT NULL,
    provider_cursor TEXT NOT NULL DEFAULT '',
    payload_json BLOB NOT NULL,
    deduplicated INTEGER NOT NULL,
    run_uid TEXT NOT NULL,
    accepted_at TEXT NOT NULL,
    UNIQUE(trigger_uid, occurrence_id)
);

CREATE TABLE IF NOT EXISTS outbox (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    command_type TEXT NOT NULL,
    aggregate_uid TEXT NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE,
    payload_json BLOB NOT NULL,
    state TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    available_at TEXT NOT NULL,
    claimed_at TEXT,
    completed_at TEXT,
    last_error TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS runs (
    uid TEXT PRIMARY KEY,
    flow_uid TEXT NOT NULL,
    trigger_receipt_uid TEXT NOT NULL,
    plan_hash TEXT NOT NULL,
    interpreter_version TEXT NOT NULL,
    phase TEXT NOT NULL,
    input_json BLOB NOT NULL,
    output_json BLOB,
    current_nodes_json BLOB NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    completed_at TEXT,
    FOREIGN KEY(plan_hash) REFERENCES compiled_plans(plan_hash)
);

CREATE TABLE IF NOT EXISTS node_attempts (
    run_uid TEXT NOT NULL,
    node_id TEXT NOT NULL,
    logical_iteration INTEGER NOT NULL,
    attempt INTEGER NOT NULL,
    phase TEXT NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE,
    input_json BLOB NOT NULL,
    output_json BLOB,
    error_text TEXT NOT NULL DEFAULT '',
    started_at TEXT NOT NULL,
    completed_at TEXT,
    PRIMARY KEY(run_uid, node_id, logical_iteration, attempt),
    FOREIGN KEY(run_uid) REFERENCES runs(uid)
);

CREATE TABLE IF NOT EXISTS run_events (
    run_uid TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    node_id TEXT NOT NULL DEFAULT '',
    attempt INTEGER NOT NULL DEFAULT 0,
    event_type TEXT NOT NULL,
    payload_json BLOB NOT NULL,
    occurred_at TEXT NOT NULL,
    PRIMARY KEY(run_uid, sequence),
    FOREIGN KEY(run_uid) REFERENCES runs(uid)
);

CREATE TABLE IF NOT EXISTS approvals (
    run_uid TEXT NOT NULL,
    node_id TEXT NOT NULL,
    state TEXT NOT NULL,
    actor TEXT NOT NULL DEFAULT '',
    reason TEXT NOT NULL DEFAULT '',
    expires_at TEXT,
    decided_at TEXT,
    PRIMARY KEY(run_uid, node_id),
    FOREIGN KEY(run_uid) REFERENCES runs(uid)
);

CREATE TABLE IF NOT EXISTS provider_cursors (
    trigger_uid TEXT PRIMARY KEY,
    cursor TEXT NOT NULL,
    provider_event_id TEXT NOT NULL,
    acknowledged_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS plugin_installations (
    name TEXT NOT NULL,
    version TEXT NOT NULL,
    digest TEXT NOT NULL,
    manifest_json BLOB NOT NULL,
    state TEXT NOT NULL,
    installed_at TEXT NOT NULL,
    PRIMARY KEY(name, version),
    UNIQUE(digest)
);

CREATE TABLE IF NOT EXISTS plugin_activations (
    name TEXT PRIMARY KEY,
    version TEXT NOT NULL,
    activated_at TEXT NOT NULL,
    FOREIGN KEY(name, version) REFERENCES plugin_installations(name, version)
);

CREATE INDEX IF NOT EXISTS resource_events_lookup ON resource_events(kind, namespace, revision);
CREATE INDEX IF NOT EXISTS outbox_dispatch ON outbox(state, available_at, id);
CREATE INDEX IF NOT EXISTS runs_flow_created ON runs(flow_uid, created_at DESC);
CREATE INDEX IF NOT EXISTS run_events_watch ON run_events(run_uid, sequence);
CREATE INDEX IF NOT EXISTS receipts_trigger_accepted ON trigger_receipts(trigger_uid, accepted_at DESC);

