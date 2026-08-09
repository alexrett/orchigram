# Self-SDLC operator workflow

This directory is a reusable, English-only description of how a repository can
operate through its own local Orchigram daemon. Replace the clearly marked
owner and repository values in the YAML. Do not add tokens, local socket paths,
home directories, or host-specific executable paths to these files.

Run the daemon on its default Unix-socket-only transport and install the
`github`, `agent-command`, and `exec` plugins. The only credential reference is
`self-sdlc-github-token`; its value exists only in the daemon environment for
the operation. The planner and implementer use the portable `codex` executable
name. A downstream repository may replace those profiles without changing the
flow.

Apply the resources in this order:

```console
orchigram apply -f examples/self-sdlc/secret-ref.yaml
orchigram apply -f examples/self-sdlc/repository.yaml
orchigram apply -f examples/self-sdlc/planner-profile.yaml
orchigram apply -f examples/self-sdlc/implementer-profile.yaml
orchigram apply -f examples/self-sdlc/issue-to-pr-flow.yaml
orchigram apply -f examples/self-sdlc/ready-trigger.yaml
orchigram apply -f examples/self-sdlc/review-trigger.yaml
```

Adding `orchigram:ready` to an issue starts one reconciled occurrence. The flow
checks out an isolated deterministic issue branch, performs read-only planning,
posts the plan, and waits for durable human approval. Approval permits the
implementation profile to edit only that checkout. `make check` is mandatory
before the GitHub provider commits and pushes the deterministic issue branch
and creates or reconciles a pull request. The review Trigger then resumes the
same durable Run. `CHANGES_REQUESTED` sends the review body and paginated inline
comments to the workspace-write agent, reruns `make check`, pushes another
commit to the same deterministic branch, and waits for a new review. An
approval applies only when the reviewed commit equals the pull request's
current head SHA; stale approvals are consumed but cannot advance the Run.

The flow never pushes the default branch, never merges, and never bypasses
human pull-request review. After a current-head approval,
`github.commit.checks.wait` evaluates the named Checks API runs and any legacy
commit statuses for that exact SHA. A reconciled issue comment marks the pull
request merge-ready only after they pass. A human still merges through normal
GitHub controls. A new provider-triggered Run has a new Run UID and derives a
new deterministic branch. Rejecting the initial Flow approval prevents
implementation, testing, push, and pull-request creation.

Create and apply the Trigger before adding the label. A new subscription uses
its durable activation timestamp plus a bounded one-minute clock-skew overlap
and ignores older matching repository events unless
`provider.config.replayExisting` is explicitly set to `true`. Stable GitHub
event IDs deduplicate the overlap. Once the first event is acknowledged, the
current Trigger generation's persisted provider cursor owns restart replay.
Both provider Triggers must be active before the issue label is added. The
review Trigger targets Runs only through strict hidden markers created by the
`github.pr.ensure` action; it ignores unrelated pull requests.
