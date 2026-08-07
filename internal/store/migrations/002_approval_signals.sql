ALTER TABLE approvals ADD COLUMN signal_delivered INTEGER NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS approvals_signal_reconcile
    ON approvals(signal_delivered, state, decided_at);

