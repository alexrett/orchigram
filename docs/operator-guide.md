# Operator guide

Orchigram has one control API and two transports: a local Unix socket or that
same Unix socket forwarded by OpenSSH. The TUI and every CLI command use named
contexts from `~/.config/orchigram/contexts.yaml`; this is the only local state.

## Install a server

Place the `orchigram` executable and the four matching first-party plugin
executables in one directory on an Ubuntu 24.04 host, then run:

```console
sudo ./orchigram install --plugin-dir .
```

The installer creates the stable `orchigram` system user and group, installs a
hardened systemd unit, starts the daemon, verifies each immutable plugin bundle,
and activates the four plugins. Re-running the command is the supported
single-node upgrade path. Before replacement it verifies every source, captures
the current core files and plugin activation set, and asks the running daemon
for a causally safe online backup. A failed restart, health check, bundle
install, or activation restores the previous binary, unit, config ownership,
bundled executables, activation set, databases, and immutable plugin state
before restarting the old service. Workspaces, artifacts, and backup archives
created around the attempt are carried forward; the failed database/plugin
state is preserved beside `/var/lib/orchigram` for operator forensics. Durable
approvals, timers, and retries then reconcile from the restored SQLite state.
The service
has default cgroup bounds (`MemoryHigh=512M`, `MemoryMax=768M`, `CPUQuota=200%`,
and `TasksMax=256`). It never opens a TCP listener unless `/etc/orchigram/config.yaml`
explicitly sets `http.listen`.

The server paths are:

- `/usr/local/bin/orchigram` and `/usr/local/lib/orchigram/plugins/`;
- `/etc/orchigram/config.yaml`;
- `/var/lib/orchigram/` for SQLite, plugins, workspaces, and artifacts;
- `/run/orchigram/orchigram.sock` for the control API;
- `/var/log/orchigram/` for operator-owned log integrations; system logs go to
  the journal by default.

An operator needs Unix permission to the socket. Add a non-root SSH account to
the `orchigram` group and start a new login session before using it:

```console
sudo usermod -aG orchigram operator
```

`install` warns when `git`, `codex`, or `claude` is unavailable. Agent CLI
login remains owned by those tools; Orchigram does not copy or print their
credentials.

The daemon scheduler is also bounded independently of systemd. The default
configuration admits at most eight non-terminal Runs, four concurrent external
activities, and two concurrent agent processes:

```yaml
operations:
  maxActiveRuns: 8
  maxConcurrentActivities: 4
  maxAgentProcesses: 2
```

Accepted occurrences beyond the Run limit remain durable in the outbox; they
are not rejected or forgotten. Saturated Run, activity, or agent capacity is
reported through aggregate health until queued work advances.

## Local and SSH contexts

The default context connects to `/run/orchigram/orchigram.sock`. Add and select
a remote context with ordinary OpenSSH destination syntax:

```console
orchigram context set production --ssh-destination operator@example.net
orchigram context use production
orchigram context get
```

An identity file is optional:

```console
orchigram context set production \
  --ssh-destination operator@example.net \
  --identity ~/.ssh/id_ed25519 \
  --remote-socket /run/orchigram/orchigram.sock
```

Orchigram executes `ssh` directly, creates a private temporary local socket,
and supervises `StreamLocal` forwarding with bounded reconnect backoff. No
gRPC TCP port is exposed on the server.

## TUI

Run `orchigram` to open the English-only interface. The core bindings are:

- `:` command palette, `/` resource filter, and `?` help;
- `Enter` drill-down and `Esc` back;
- arrows or `h/j/k/l` to move through graph nodes, `Tab`/`Shift-Tab` to traverse nodes and edges, and `H/J/K/L` to pan;
- `g` graph, `e` structured events, `t` attempts, `f` artifacts, and `l` logs;
- `y` read-only YAML and `d` describe;
- `n` create from a strict YAML template, `E` edit the schema-derived projection, and `x` CAS-delete;
- `S` start the selected Flow with JSON input and an optional idempotency key;
- `i` upload and install a plugin bundle; plugin detail activates, disables, diagnoses, or rolls back immutable versions;
- `a` approve, `r` reject, and `c` cancel the selected run;
- `q` quit.

Forms send the displayed `resourceVersion` as a compare-and-swap condition.
Conflicts are shown and never overwrite the newer resource. `SecretRef` forms
expose only backend and reference key/path; status shows `Configured` or
`Missing`, never the value.

The graph view is shared by Flow definitions, live run status, and historical
event replay. On a Flow definition, `Enter` opens the selected node or edge as
an editable projection. Scalar plugin configuration comes from the action's
pinned JSON Schema; nested configuration uses a strict JSON-object fallback.
The daemon compiles and validates the resulting Flow before a resourceVersion
CAS apply, so a stale form or invalid CEL/reference cannot partially mutate the
resource. Mouse click selects a node or edge, double-click opens it, and the
wheel pans the viewport. Every operation has a keyboard path at 80x24 and
larger.

Selecting another Context asks for confirmation, closes the current gRPC/SSH
transport, persists only the local context choice, and reconnects the stateless
TUI through the same public API. No server resource is changed by a context
switch.

The resource tree is a live projection, not a startup snapshot. The TUI loads
one revision-bound all-resource snapshot and resumes one durable global watch
from that revision. A page conflict causes a bounded-backoff full resync;
stream interruption resumes from the last applied revision. Active run phases
come from durable per-run event streams with sequence deduplication, while a
bounded list poll discovers newly accepted runs. Plugin state and aggregate
health refresh independently. The status line reports only component state and
never includes raw dependency errors.

Events, attempts, artifacts, and logs are separate run views. Historical runs
replay their durable event sequence onto the same graph primitive used for Flow
definitions and active overlays. Text artifacts have a bounded 2 MiB preview;
binary artifacts remain metadata-only in the TUI.

## Scriptable CLI

The CLI uses the same gRPC services and contexts as the TUI. Apply one or more
strict resources from a file or stdin:

```console
orchigram apply -f resources.yaml
orchigram apply -f - < resources.yaml
```

Multi-document apply validates every document before the first write. An
update uses each document's `metadata.resourceVersion`; a later compare-and-swap
conflict reports both the failing document and how many earlier documents were
committed. Export emits status-free desired-state YAML that can be applied back:

```console
orchigram export flow daily-review weekly-review > flows.yaml
orchigram apply -f flows.yaml
```

Resource lists automatically traverse revision-bound pages. Exact label
selectors are ANDed, and watches expose their durable resume revision:

```console
orchigram get flows -A --selector team=platform --limit 100
orchigram watch flows -A --after-revision 42
```

Operator inspection and reconciliation remain separate operations:

```console
orchigram flow graph daily-review
orchigram run list --flow daily-review --phase waiting
orchigram run describe RUN_UID
orchigram run reconcile RUN_UID
orchigram trigger receipts TRIGGER_UID
orchigram plugin describe github 0.1.0
```

`run describe` returns the immutable pinned plan plus attempt and artifact
metadata; it does not reconcile the workflow or inline raw artifact content.

## Verification and recovery

Useful server checks are:

```console
systemctl status orchigram.service
journalctl -u orchigram.service
systemd-analyze security orchigram.service
orchigram system health
orchigram system doctor
orchigram plugin list
orchigram plugin doctor agent-command
```

`system health` exits unsuccessfully when any required component is degraded
and prints deterministic diagnostics for outbox and durable-engine
reconciliation, schedule/provider controllers, and every active plugin. An
accepted outbox failure remains degraded until that delivery succeeds; it is
not cleared merely by an empty polling cycle. The TUI System → Health action
shows the same projection. Diagnostics deliberately omit dependency errors,
payloads, secret values, server paths, and provider coordinates; use the
service journal for privileged detail.

`system doctor` verifies writable state storage, system `git`, every active
plugin, and configured agent profiles (including executable discovery and the
runtime's own authentication probe). It uses bounded calls and emits only
generic diagnostics; credentials and resolved secret values are never printed.

A database created by a newer Orchigram schema version is rejected during
startup. The daemon never serves an apparently healthy control socket after a
configuration, migration, artifact-reconciliation, or listener failure.

The repository's `scripts/verify-ssh-context.sh` performs the manual exec plus
durable-approval tracer through an SSH context. It requires
`ORCHIGRAM_TEST_SSH_DESTINATION` and never embeds a test host address.

## Retention and garbage collection

Retention is an explicit operator action and defaults to an explainable dry-run:

```console
orchigram system retention --older-than 720h --keep-recent 100
orchigram system retention --older-than 720h --keep-recent 100 \
  --keep-recent-backups 3 --inactive-plugins --collect
```

Only terminal Runs older than the cutoff and outside the preserved recent set
are eligible. Active Runs and Runs with incomplete outbox work cannot be
selected. Collection first removes finished durable-framework history, then the
product evidence transaction, then owned artifacts and the Run workspace.
Full receipt payloads may be collected, but a minimal occurrence tombstone is
retained permanently so provider replay cannot create a second Run for an
already accepted occurrence.

Backup collection preserves the configured newest archives. Inactive plugin
versions require the explicit `--inactive-plugins` flag and remain ineligible
while active, selected by a `PluginInstallation`, or pinned by any retained
execution plan. Every invocation is limited to a bounded number of candidates.

## Backup and offline restore

Create an online snapshot while the daemon continues to serve requests:

```console
orchigram system backup
```

The daemon uses SQLite `VACUUM INTO` for both databases, snapshots durable
workflow history before product state, includes immutable plugin installations,
stores the archive below `/var/lib/orchigram/backups`, and returns its SHA-256.
If an activity finishes between the two snapshots, product evidence can only be
ahead of history; restart replays the terminal attempt by stable identity without
repeating its external call. A custom destination is accepted only when it
remains inside the configured state directory.

Restore is deliberately offline and never overwrites an existing directory:

```console
sudo systemctl stop orchigram
sudo orchigram system restore /var/lib/orchigram/backups/BACKUP.tar.gz \
  --destination /var/lib/orchigram-restored
```

The command rejects traversal, links, duplicate entries, oversized files, and
unknown archive members. Both restored databases must pass SQLite
`integrity_check` before the temporary tree is atomically renamed. Inspect the
new directory before swapping it into service; preserve the old state tree as
the rollback point.
