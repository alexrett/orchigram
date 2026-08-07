---
phase: 05-github-sdlc
plan: 01
subsystem: github-tracer
tags: [github-api, issue-events, git, workspaces, approvals, pull-requests, reconciliation]
requires: [04-01]
provides: [github-trigger-provider, isolated-run-workspaces, issue-to-pr-reference-flow, hidden-marker-reconciliation]
affects: [06-operator-surface, 07-publication]
tech-stack:
  added: []
  patterns: [issue-event-cursor, deterministic-branch, hidden-run-marker, direct-git-argv, external-effect-reconciliation]
key-files:
  created: [internal/githubplugin/github.go, internal/workspace/workspace.go, examples/github/issue-to-pr-flow.yaml, docs/github-sdlc.md]
  modified: [cmd/orchigram-plugin-github/main.go, internal/pluginmanager/manager.go, internal/daemon/daemon_test.go]
key-decisions:
  - GitHub repository issue-event IDs identify orchigram:ready label transitions and durable provider cursors.
  - Git authentication uses an ephemeral http.extraHeader environment configuration and never embeds the PAT in argv or clone URLs.
  - Comments and PRs reconcile by hidden run/node marker; pushes reconcile by deterministic branch and clean workspace state.
requirements-completed: [GIT-01, GIT-02, GIT-03]
coverage:
  - deliverable: GitHub issue polling, pagination, rate limits, cursor order, and provider gRPC stream
    verification:
      - kind: test
        ref: TestPollingFixturesCoverPaginationRateLimitAndStableOrder and TestFirstPartyPluginConformance
        status: pass
    human_judgment: false
  - deliverable: Isolated checkout, deterministic commit, and repeated push
    verification:
      - kind: test
        ref: TestCheckoutCommitAndPushReconcileDeterministicBranch
        status: pass
    human_judgment: false
  - deliverable: Full plan/approval/implementation/test/push/PR tracer and rejection gate
    verification:
      - kind: test
        ref: TestGitHubIssueApprovalToPullRequestTracer
        status: pass
    human_judgment: false
  - deliverable: Comment and PR mutation reconciliation across retry and plugin restart
    verification:
      - kind: test
        ref: TestCommentAndPullRequestReconcileByMarkerAndBranch and TestFirstPartyPluginConformance
        status: pass
    human_judgment: false
  - deliverable: Multi-platform builds and repository quality gates
    verification:
      - kind: command
        ref: make check && make cross-build
        status: pass
    human_judgment: false
duration: 38 min
completed: 2026-08-08
---

# Phase 5 Plan 1: GitHub issue-to-PR tracer summary

Orchigram now ships a direct GitHub REST plugin and a declarative reference Flow that turns a ready-labeled issue into a reviewed, tested pull request without using `gh`, merging automatically, or pushing the default branch.

## Accomplishments

- Added stable repository issue-event polling for `orchigram:ready`, paginated fixture handling, bounded rate-limit retry, ordered event cursors, and real TriggerProvider gRPC streaming.
- Added run-owned git workspaces under daemon state, direct argv clone/fetch/checkout/test/commit/push boundaries, deterministic issue branches, and PAT injection outside argv and URLs.
- Added declarative `${input...}` and `${nodes...}` templates plus JSON Pointer mappings for data flow between agent and task nodes.
- Added direct issue fetch, reconciled issue comment, workspace checkout, workspace commit/push, and reconciled PR actions to the bundled GitHub plugin.
- Added the full English reference resources for planning, plan publication, durable approval, implementation, test gate, push, PR, and final status.
- Added a daemon-level tracer with three real plugin processes, a real local bare remote, approved completion, and rejected completion with no remote branch or PR.

## Verification

- `make check`: passed, including the complete race suite, vet, lint, Buf, and gitleaks.
- `make cross-build`: passed for core and all plugins on darwin/linux amd64/arm64.
- Recorded-shaped fixtures cover polling, pagination, a rate-limit retry, issue retrieval, hidden-marker comment/PR reconciliation, and an actual GitHub plugin process restart.
- The local Git tracer proves repeat push creates no second commit and the rejected run performs no git repository mutation.

## Commit

- `b654655` — implement GitHub issue-to-PR tracer.

## Deviations from Plan

- The real private `alexrett/orchigram-e2e` mutation gate is documented but intentionally not executed without explicit operator confirmation and a supplied fine-grained PAT. Automated acceptance uses `httptest` plus a real local bare Git remote.

## Self-Check: PASSED

All automated Phase 5 gates pass; no test depends on `gh`, a public listener, or an external GitHub mutation.
