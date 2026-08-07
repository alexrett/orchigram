CREATE TABLE IF NOT EXISTS trigger_states (
    trigger_uid TEXT PRIMARY KEY,
    generation INTEGER NOT NULL,
    enabled INTEGER NOT NULL,
    cursor_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS trigger_skips (
    trigger_uid TEXT NOT NULL,
    occurrence_id TEXT NOT NULL,
    reason TEXT NOT NULL,
    scheduled_at TEXT NOT NULL,
    recorded_at TEXT NOT NULL,
    PRIMARY KEY(trigger_uid, occurrence_id)
);

CREATE INDEX IF NOT EXISTS trigger_skips_recent ON trigger_skips(trigger_uid, scheduled_at DESC);
