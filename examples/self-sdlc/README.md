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
```

Adding `orchigram:ready` to an issue starts one reconciled occurrence. The flow
checks out an isolated deterministic issue branch, performs read-only planning,
posts the plan, and waits for durable human approval. Approval permits the
implementation profile to edit only that checkout. `make check` is mandatory
before the GitHub provider commits and pushes the deterministic issue branch
and creates or reconciles a pull request.

The flow never pushes the default branch, never merges, and never bypasses
human pull-request review. Rejecting approval prevents implementation, testing,
push, and pull-request creation.
