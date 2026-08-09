CREATE TABLE IF NOT EXISTS occurrence_tombstones (
    trigger_uid TEXT NOT NULL,
    occurrence_id TEXT NOT NULL,
    receipt_uid TEXT NOT NULL,
    run_uid TEXT NOT NULL,
    deduplicated INTEGER NOT NULL,
    accepted_at TEXT NOT NULL,
    collected_at TEXT NOT NULL,
    PRIMARY KEY(trigger_uid, occurrence_id)
);

CREATE INDEX IF NOT EXISTS occurrence_tombstones_collected
    ON occurrence_tombstones(collected_at, trigger_uid, occurrence_id);
