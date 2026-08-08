ALTER TABLE node_attempts RENAME TO node_attempts_v1;

CREATE TABLE node_attempts (
    run_uid TEXT NOT NULL,
    node_id TEXT NOT NULL,
    logical_iteration INTEGER NOT NULL,
    attempt INTEGER NOT NULL,
    framework_attempt INTEGER NOT NULL,
    phase TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    input_json BLOB NOT NULL,
    output_json BLOB,
    error_text TEXT NOT NULL DEFAULT '',
    exit_outcome TEXT NOT NULL DEFAULT '',
    started_at TEXT NOT NULL,
    completed_at TEXT,
    PRIMARY KEY(run_uid, node_id, logical_iteration, attempt),
    FOREIGN KEY(run_uid) REFERENCES runs(uid)
);

INSERT INTO node_attempts(
    run_uid,node_id,logical_iteration,attempt,framework_attempt,phase,idempotency_key,
    input_json,output_json,error_text,started_at,completed_at
)
SELECT
    run_uid,node_id,logical_iteration,attempt,attempt,phase,idempotency_key,
    input_json,output_json,error_text,started_at,completed_at
FROM node_attempts_v1;

DROP TABLE node_attempts_v1;

CREATE TABLE plugin_events (
    run_uid TEXT NOT NULL,
    node_id TEXT NOT NULL,
    logical_iteration INTEGER NOT NULL,
    attempt INTEGER NOT NULL,
    sequence INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    payload_json BLOB NOT NULL,
    occurred_at TEXT NOT NULL,
    PRIMARY KEY(run_uid,node_id,logical_iteration,attempt,sequence),
    FOREIGN KEY(run_uid,node_id,logical_iteration,attempt)
        REFERENCES node_attempts(run_uid,node_id,logical_iteration,attempt)
);

CREATE TABLE artifacts (
    uid TEXT PRIMARY KEY,
    run_uid TEXT NOT NULL,
    node_id TEXT NOT NULL,
    logical_iteration INTEGER NOT NULL,
    attempt INTEGER NOT NULL,
    name TEXT NOT NULL,
    media_type TEXT NOT NULL,
    relative_path TEXT NOT NULL,
    size_bytes INTEGER NOT NULL,
    sha256 TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(run_uid,node_id,logical_iteration,attempt,name),
    FOREIGN KEY(run_uid,node_id,logical_iteration,attempt)
        REFERENCES node_attempts(run_uid,node_id,logical_iteration,attempt)
);

CREATE INDEX node_attempts_run_started
    ON node_attempts(run_uid,started_at,node_id,logical_iteration,attempt);
CREATE INDEX plugin_events_run_sequence
    ON plugin_events(run_uid,node_id,logical_iteration,attempt,sequence);
CREATE INDEX artifacts_run_attempt
    ON artifacts(run_uid,node_id,logical_iteration,attempt,name);
