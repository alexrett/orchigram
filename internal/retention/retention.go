// Package retention plans and collects bounded single-node execution evidence.
package retention

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/alexrett/orchigram/internal/store"
)

// Item is one explainable retention action.
type Item struct {
	Kind      string
	Identity  string
	SizeBytes int64
	Reason    string
}

// Report is returned for both dry-run and collection.
type Report struct {
	DryRun          bool
	CompletedBefore time.Time
	Items           []Item
	CollectedRuns   int
	CollectedFiles  int
	ReclaimedBytes  int64
}

// FinishedRunPruner removes durable framework history for one terminal Run.
type FinishedRunPruner interface {
	RemoveFinishedRun(context.Context, string) error
}

// Apply plans old terminal Runs and optionally commits database deletion before
// removing their now-unreferenced artifact and workspace trees.
func Apply(ctx context.Context, state *store.Store, stateDir string, completedBefore time.Time, keepRecent, keepRecentBackups, limit int, collect, includeInactivePlugins bool, pruners ...FinishedRunPruner) (Report, error) {
	stateDir = filepath.Clean(stateDir)
	if !filepath.IsAbs(stateDir) {
		return Report{}, errors.New("retention state directory must be absolute")
	}
	candidates, err := state.PlanRunRetention(ctx, completedBefore, keepRecent, limit)
	if err != nil {
		return Report{}, err
	}
	report := Report{DryRun: !collect, CompletedBefore: completedBefore.UTC(), Items: make([]Item, 0, len(candidates))}
	for _, candidate := range candidates {
		report.Items = append(report.Items, Item{Kind: "Run", Identity: candidate.UID, SizeBytes: candidate.ArtifactBytes, Reason: "terminal and older than cutoff"})
		for _, relative := range candidate.ArtifactPaths {
			if _, _, err := inspectArtifact(stateDir, relative); err != nil {
				return Report{}, err
			}
		}
		workspace := filepath.Join(stateDir, "workspaces", candidate.UID)
		if info, err := os.Lstat(workspace); err == nil && info.Mode()&os.ModeSymlink != 0 {
			return Report{}, errors.New("retention refuses a symlink workspace")
		} else if err != nil && !errors.Is(err, os.ErrNotExist) {
			return Report{}, err
		}
	}
	backups, err := planBackups(filepath.Join(stateDir, "backups"), completedBefore, keepRecentBackups, limit)
	if err != nil {
		return Report{}, err
	}
	for _, backup := range backups {
		report.Items = append(report.Items, Item{Kind: "Backup", Identity: filepath.Base(backup.path), SizeBytes: backup.size, Reason: "older than cutoff and outside the preserved recent set"})
	}
	plugins := []store.RetentionPlugin{}
	pluginSizes := map[string]int64{}
	if includeInactivePlugins {
		plugins, err = state.PlanPluginRetention(ctx, completedBefore, limit)
		if err != nil {
			return Report{}, err
		}
		for _, plugin := range plugins {
			path := filepath.Join(stateDir, "plugins", plugin.Name, plugin.Version)
			size, sizeErr := regularTreeSize(path)
			if sizeErr != nil {
				return Report{}, sizeErr
			}
			pluginSizes[plugin.Name+"\x00"+plugin.Version] = size
			report.Items = append(report.Items, Item{Kind: "PluginInstallation", Identity: plugin.Name + "@" + plugin.Version, SizeBytes: size, Reason: "inactive, old, and unreferenced"})
		}
	}
	if !collect || len(candidates)+len(backups)+len(plugins) == 0 {
		return report, nil
	}
	uids := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		uids = append(uids, candidate.UID)
	}
	if len(pruners) > 0 && pruners[0] != nil {
		for _, runUID := range uids {
			if err := pruners[0].RemoveFinishedRun(ctx, runUID); err != nil {
				return Report{}, fmt.Errorf("remove durable history for Run %s: %w", runUID, err)
			}
		}
	}
	if err := state.CollectRetainedRuns(ctx, uids); err != nil {
		return Report{}, err
	}
	report.CollectedRuns = len(candidates)
	for _, candidate := range candidates {
		for _, relative := range candidate.ArtifactPaths {
			path, info, inspectErr := inspectArtifact(stateDir, relative)
			if inspectErr != nil {
				return report, inspectErr
			}
			if removeErr := os.Remove(path); removeErr == nil {
				report.CollectedFiles++
				if info != nil {
					report.ReclaimedBytes += info.Size()
				}
			} else if !errors.Is(removeErr, os.ErrNotExist) {
				return report, removeErr
			}
		}
		workspace := filepath.Join(stateDir, "workspaces", candidate.UID)
		if removeErr := removeTreeWithin(filepath.Join(stateDir, "workspaces"), workspace); removeErr != nil {
			return report, removeErr
		}
		_ = removeEmptyParents(filepath.Join(stateDir, "artifacts"), filepath.Join(stateDir, "artifacts", candidate.UID))
	}
	for _, backup := range backups {
		if err := os.Remove(backup.path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return report, err
		}
		report.CollectedFiles++
		report.ReclaimedBytes += backup.size
	}
	for _, plugin := range plugins {
		if err := state.CollectRetainedPlugin(ctx, plugin); err != nil {
			return report, err
		}
		path := filepath.Join(stateDir, "plugins", plugin.Name, plugin.Version)
		if err := removeTreeWithin(filepath.Join(stateDir, "plugins"), path); err != nil {
			return report, err
		}
		report.CollectedFiles++
		report.ReclaimedBytes += pluginSizes[plugin.Name+"\x00"+plugin.Version]
	}
	return report, nil
}

type backupCandidate struct {
	path    string
	modTime time.Time
	size    int64
}

func planBackups(root string, before time.Time, keepRecent, limit int) ([]backupCandidate, error) {
	entries, err := os.ReadDir(root)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	all := make([]backupCandidate, 0)
	for _, entry := range entries {
		if entry.Type()&os.ModeSymlink != 0 || entry.IsDir() || !strings.HasSuffix(entry.Name(), ".tar.gz") {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return nil, err
		}
		if !info.Mode().IsRegular() {
			continue
		}
		all = append(all, backupCandidate{path: filepath.Join(root, entry.Name()), modTime: info.ModTime(), size: info.Size()})
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].modTime.Equal(all[j].modTime) {
			return all[i].path > all[j].path
		}
		return all[i].modTime.After(all[j].modTime)
	})
	result := make([]backupCandidate, 0)
	for index, candidate := range all {
		if index < keepRecent || !candidate.modTime.Before(before) {
			continue
		}
		result = append(result, candidate)
		if len(result) == limit {
			break
		}
	}
	return result, nil
}

func regularTreeSize(root string) (int64, error) {
	var size int64
	err := filepath.WalkDir(root, func(_ string, entry os.DirEntry, walkErr error) error {
		if errors.Is(walkErr, os.ErrNotExist) {
			return nil
		}
		if walkErr != nil {
			return walkErr
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return errors.New("retention refuses a symlink in an immutable plugin tree")
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return errors.New("retention refuses a non-regular plugin entry")
		}
		size += info.Size()
		return nil
	})
	return size, err
}

func safeStatePath(root, relative string) (string, error) {
	if relative == "" || filepath.IsAbs(relative) {
		return "", errors.New("retention artifact path is not relative")
	}
	target := filepath.Join(root, filepath.FromSlash(relative))
	rel, err := filepath.Rel(root, target)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("retention artifact path %q escapes state directory", relative)
	}
	return target, nil
}

func inspectArtifact(root, relative string) (string, os.FileInfo, error) {
	path, err := safeStatePath(root, relative)
	if err != nil {
		return "", nil, err
	}
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return path, nil, nil
	}
	if err != nil {
		return "", nil, err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return "", nil, errors.New("retention refuses a non-regular artifact entry")
	}
	return path, info, nil
}

func removeTreeWithin(root, target string) error {
	rel, err := filepath.Rel(root, target)
	if err != nil || rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return errors.New("retention tree escaped its owned root")
	}
	info, err := os.Lstat(target)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return errors.New("retention refuses a symlink workspace")
	}
	return os.RemoveAll(target)
}

func removeEmptyParents(root, start string) error {
	root = filepath.Clean(root)
	for current := filepath.Clean(start); current != root; current = filepath.Dir(current) {
		if err := os.Remove(current); err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil
		}
	}
	return nil
}
