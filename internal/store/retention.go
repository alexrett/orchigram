package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/alexrett/orchigram/internal/flow"
	"github.com/alexrett/orchigram/internal/resource"
)

// RetentionRun is one terminal Run that is safe to remove from the product
// store. Associated filesystem paths are returned for post-commit cleanup.
type RetentionRun struct {
	UID           string
	CompletedAt   time.Time
	ArtifactPaths []string
	ArtifactBytes int64
}

// RetentionPlugin is one inactive immutable version with no desired resource
// or retained execution-plan reference.
type RetentionPlugin struct {
	Name        string
	Version     string
	Digest      string
	InstalledAt time.Time
}

// PlanRunRetention selects old terminal Runs after preserving the newest
// keepRecent terminal Runs. Active Runs and Runs with incomplete outbox work
// are never returned.
func (s *Store) PlanRunRetention(ctx context.Context, completedBefore time.Time, keepRecent, limit int) ([]RetentionRun, error) {
	if completedBefore.IsZero() {
		return nil, errors.New("retention cutoff is required")
	}
	if keepRecent < 0 {
		return nil, errors.New("retention keepRecent must not be negative")
	}
	if limit < 1 || limit > 1000 {
		return nil, errors.New("retention limit must be between 1 and 1000")
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT uid,completed_at
		FROM runs
		WHERE phase IN ('succeeded','failed','rejected','cancelled')
		  AND completed_at IS NOT NULL
		  AND completed_at < ?
		  AND NOT EXISTS (
			SELECT 1 FROM outbox
			WHERE aggregate_uid=runs.uid AND state!='completed'
		  )
		  AND uid NOT IN (
			SELECT uid FROM runs
			WHERE phase IN ('succeeded','failed','rejected','cancelled')
			ORDER BY completed_at DESC,uid DESC LIMIT ?
		  )
		ORDER BY completed_at,uid
		LIMIT ?`, completedBefore.UTC().Format(time.RFC3339Nano), keepRecent, limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	result := make([]RetentionRun, 0)
	for rows.Next() {
		var candidate RetentionRun
		var completed string
		if err := rows.Scan(&candidate.UID, &completed); err != nil {
			return nil, err
		}
		candidate.CompletedAt, _ = time.Parse(time.RFC3339Nano, completed)
		result = append(result, candidate)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	for index := range result {
		artifactRows, artifactErr := s.db.QueryContext(ctx, `SELECT relative_path,size_bytes FROM artifacts WHERE run_uid=? ORDER BY relative_path`, result[index].UID)
		if artifactErr != nil {
			return nil, artifactErr
		}
		for artifactRows.Next() {
			var path string
			var size int64
			if scanErr := artifactRows.Scan(&path, &size); scanErr != nil {
				_ = artifactRows.Close()
				return nil, scanErr
			}
			result[index].ArtifactPaths = append(result[index].ArtifactPaths, path)
			result[index].ArtifactBytes += size
		}
		if artifactErr := artifactRows.Err(); artifactErr != nil {
			_ = artifactRows.Close()
			return nil, artifactErr
		}
		_ = artifactRows.Close()
	}
	return result, nil
}

// CollectRetainedRuns atomically removes an exact planned set after rechecking
// every Run is still terminal and has no incomplete dispatch work.
func (s *Store) CollectRetainedRuns(ctx context.Context, runUIDs []string) error {
	if len(runUIDs) == 0 {
		return nil
	}
	if len(runUIDs) > 1000 {
		return errors.New("retention collection exceeds 1000 Runs")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	for _, runUID := range runUIDs {
		var phase string
		if err := tx.QueryRowContext(ctx, `SELECT phase FROM runs WHERE uid=?`, runUID).Scan(&phase); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				continue
			}
			return err
		}
		if !terminalRunPhase(phase) {
			return fmt.Errorf("retention Run %s is no longer terminal", runUID)
		}
		var incomplete int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM outbox WHERE aggregate_uid=? AND state!='completed'`, runUID).Scan(&incomplete); err != nil {
			return err
		}
		if incomplete != 0 {
			return fmt.Errorf("retention Run %s has incomplete dispatch work", runUID)
		}
		for _, statement := range []string{
			`DELETE FROM plugin_events WHERE run_uid=?`,
			`DELETE FROM artifacts WHERE run_uid=?`,
			`DELETE FROM node_attempts WHERE run_uid=?`,
			`DELETE FROM approvals WHERE run_uid=?`,
			`DELETE FROM run_cancellations WHERE run_uid=?`,
			`DELETE FROM run_events WHERE run_uid=?`,
			`DELETE FROM outbox WHERE aggregate_uid=? AND state='completed'`,
		} {
			if _, err := tx.ExecContext(ctx, statement, runUID); err != nil {
				return err
			}
		}
		var receiptUID, planHash string
		if err := tx.QueryRowContext(ctx, `SELECT trigger_receipt_uid,plan_hash FROM runs WHERE uid=?`, runUID).Scan(&receiptUID, &planHash); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM runs WHERE uid=?`, runUID); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT OR IGNORE INTO occurrence_tombstones(trigger_uid,occurrence_id,receipt_uid,run_uid,deduplicated,accepted_at,collected_at)
			SELECT trigger_uid,occurrence_id,uid,run_uid,deduplicated,accepted_at,?
			FROM trigger_receipts WHERE uid=?`, s.timestamp(), receiptUID); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM trigger_receipts WHERE uid=? AND NOT EXISTS (SELECT 1 FROM runs WHERE trigger_receipt_uid=?)`, receiptUID, receiptUID); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM compiled_plans WHERE plan_hash=? AND NOT EXISTS (SELECT 1 FROM runs WHERE plan_hash=?)`, planHash, planHash); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// PlanPluginRetention selects old inactive versions that are not referenced by
// desired PluginInstallation resources or any retained immutable plan.
func (s *Store) PlanPluginRetention(ctx context.Context, installedBefore time.Time, limit int) ([]RetentionPlugin, error) {
	if installedBefore.IsZero() || limit < 1 || limit > 1000 {
		return nil, errors.New("plugin retention cutoff and limit are required")
	}
	referenced, err := s.referencedPluginVersions(ctx)
	if err != nil {
		return nil, err
	}
	records, err := s.ListPlugins(ctx)
	if err != nil {
		return nil, err
	}
	result := make([]RetentionPlugin, 0)
	for _, record := range records {
		if record.Active || !record.InstalledAt.Before(installedBefore) || referenced[pluginRetentionKey(record.Name, record.Version)] {
			continue
		}
		result = append(result, RetentionPlugin{Name: record.Name, Version: record.Version, Digest: record.Digest, InstalledAt: record.InstalledAt})
		if len(result) == limit {
			break
		}
	}
	return result, nil
}

// CollectRetainedPlugin removes one exact inactive version after rechecking
// all authoritative references under the store transaction.
func (s *Store) CollectRetainedPlugin(ctx context.Context, candidate RetentionPlugin) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	var digest string
	var active int
	err = tx.QueryRowContext(ctx, `SELECT p.digest,CASE WHEN a.version=p.version THEN 1 ELSE 0 END FROM plugin_installations p LEFT JOIN plugin_activations a ON a.name=p.name WHERE p.name=? AND p.version=?`, candidate.Name, candidate.Version).Scan(&digest, &active)
	if errors.Is(err, sql.ErrNoRows) {
		return tx.Commit()
	}
	if err != nil {
		return err
	}
	if active == 1 || digest != candidate.Digest {
		return errors.New("plugin retention candidate changed before collection")
	}
	referenced, err := referencedPluginVersionsTx(ctx, tx)
	if err != nil {
		return err
	}
	if referenced[pluginRetentionKey(candidate.Name, candidate.Version)] {
		return errors.New("plugin retention candidate became referenced")
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM plugin_installations WHERE name=? AND version=? AND digest=?`, candidate.Name, candidate.Version, candidate.Digest); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) referencedPluginVersions(ctx context.Context) (map[string]bool, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	result, err := referencedPluginVersionsTx(ctx, tx)
	if err != nil {
		return nil, err
	}
	return result, tx.Commit()
}

func referencedPluginVersionsTx(ctx context.Context, tx *sql.Tx) (map[string]bool, error) {
	result := map[string]bool{}
	resourceRows, err := tx.QueryContext(ctx, `SELECT spec_json FROM resources WHERE kind='PluginInstallation'`)
	if err != nil {
		return nil, err
	}
	for resourceRows.Next() {
		var raw []byte
		if err := resourceRows.Scan(&raw); err != nil {
			_ = resourceRows.Close()
			return nil, err
		}
		installation, err := resource.DecodePluginInstallation(raw)
		if err != nil {
			_ = resourceRows.Close()
			return nil, err
		}
		result[pluginRetentionKey(installation.Spec.Plugin, installation.Spec.Version)] = true
	}
	if err := resourceRows.Err(); err != nil {
		_ = resourceRows.Close()
		return nil, err
	}
	_ = resourceRows.Close()
	planRows, err := tx.QueryContext(ctx, `SELECT plan_json FROM compiled_plans`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = planRows.Close() }()
	for planRows.Next() {
		var raw []byte
		if err := planRows.Scan(&raw); err != nil {
			return nil, err
		}
		var plan flow.ExecutionPlan
		if err := json.Unmarshal(raw, &plan); err != nil {
			return nil, err
		}
		for _, node := range plan.Nodes {
			if node.Plugin != nil {
				result[pluginRetentionKey(node.Plugin.Name, node.Plugin.Version)] = true
			}
		}
	}
	return result, planRows.Err()
}

func pluginRetentionKey(name, version string) string { return name + "\x00" + version }
