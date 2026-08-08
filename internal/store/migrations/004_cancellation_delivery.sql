CREATE TABLE IF NOT EXISTS run_cancellations (
    run_uid TEXT PRIMARY KEY,
    reason TEXT NOT NULL DEFAULT '',
    delivery_completed INTEGER NOT NULL DEFAULT 0,
    requested_at TEXT NOT NULL,
    delivered_at TEXT,
    FOREIGN KEY(run_uid) REFERENCES runs(uid)
);

CREATE INDEX IF NOT EXISTS run_cancellations_reconcile
    ON run_cancellations(delivery_completed, requested_at);
