# Phase 7 publication status

Completed on 2026-08-08:

- public source repository and terminal-first GitHub Pages deployment;
- real TUI screenshots with a reproducible VHS capture script;
- full local and GitHub CI checks, history/worktree secret scans, dependency license inventory, SPDX SBOMs, and four-target cross-builds;
- byte-for-byte reproducible 42-file release manifest: core archives, 16
  first-party plugin bundles, core/plugin SPDX documents, license inventory,
  and checksums;
- online backup, bounded path-safe restore, active approval/retry recovery, plugin rollback, and clean remote Ubuntu installation.
- public provider-triggered Issue #7 implementation through durable approval,
  PR #11, requested changes, independent-account approval, and merge;
- one explicitly confirmed sandbox Slack delivery from the reviewed merge, with
  no credential in resource projections or runtime files;
- reviewed demo-host upgrade with a non-root Unix-socket-only daemon,
  `systemd-analyze security` exposure `1.3 OK`, bounded memory use, a
  stable-key `503 -> 200` retry, and TUI approval after service restart;
- reproducibility repair in Issue #12 / PR #13, including resolved review
  findings and two clean snapshots matching 42/42 hashes.

Still gated before `v0.1.0`:

- tagged release publication and GitHub build provenance attestations.
