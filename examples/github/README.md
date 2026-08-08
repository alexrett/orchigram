# GitHub issue-to-PR tracer

This reference flow polls stable repository issue-label events for
`orchigram:ready`, plans in an isolated checkout, posts the plan with a hidden
run/node marker, waits for durable TUI approval, implements and tests the
change, pushes a deterministic branch, and creates or reconciles a pull
request. It never merges and never pushes the default branch.

In v0.1, revisions are ordinary operator Git/GitHub pushes to the existing
branch and pull request; Orchigram does not automate review events. A new
provider-triggered Run has a new Run UID and therefore derives a new
deterministic branch. Only normal GitHub review and merge controls can merge a
pull request.

The example names the dedicated `alexrett/orchigram-e2e` repository from the
v0.1 acceptance plan. Create it as a private test repository before running a
real gate. Set `ORCHIGRAM_GITHUB_TOKEN` only in the daemon service environment;
use a fine-grained PAT scoped to that repository. The YAML stores only a
`SecretRef`.

Install and enable the GitHub, agent-command, and exec bundles before applying
the Flow because action capability and configuration validation happens during
compilation. Apply the resources in this order:

```console
orchigram apply -f examples/github/secret-ref.yaml
orchigram apply -f examples/github/repository.yaml
orchigram apply -f examples/github/planner-profile.yaml
orchigram apply -f examples/github/implementer-profile.yaml
orchigram apply -f examples/github/issue-to-pr-flow.yaml
orchigram apply -f examples/github/ready-trigger.yaml
```

Real execution is an explicit operator gate: first create a disposable issue,
then add the `orchigram:ready` label. Rejecting the approval prevents the
implementation, tests, commit, push, and PR nodes from running. Comments and
PRs reconcile by hidden marker; the pushed branch is
`orchigram/issue-{number}-{runShortID}`.
