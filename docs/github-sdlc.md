# GitHub SDLC tracer

The bundled GitHub plugin polls repository issue events for the transition in
which an operator adds `orchigram:ready`. Repository issue-event IDs are the
provider cursor and event identity. Pagination is consumed before events are
sorted by ID; a rate-limited GET is retried with bounded delay. The daemon
persists the event receipt, outbox command, and cursor before acknowledging the
plugin stream.

The plugin talks to the GitHub REST API directly. It does not execute `gh` and
accepts a fine-grained PAT only as an operation-scoped SecretRef binding. The
same secret is provided to system `git` through an ephemeral `http.extraHeader`
environment configuration, not embedded in a clone URL or command argument.

## Reference run

The example under `examples/github` performs these durable nodes:

1. fetch the current issue;
2. clone into the run-owned workspace and create a deterministic branch;
3. run a read-only planning agent;
4. create or find the marked plan comment;
5. wait for durable operator approval;
6. run a workspace-write implementation agent;
7. execute the configured test argv;
8. commit and push the deterministic branch;
9. create or find its pull request;
10. create or find the final issue comment.

Comments and pull-request bodies contain
`<!-- orchigram:run=...;node=... -->`. A retry after a successful mutation but
before activity completion finds that marker. A push retry sees a clean
workspace and reconciles the same branch head. The branch is always
`orchigram/issue-{number}-{runShortID}`; the plugin rejects other branch names.

Approval rejection terminates interpretation before implementation, tests,
commit, push, and PR creation. Orchigram never merges a PR and never pushes the
default branch.

## Verification boundary

Automated tests use recorded-shaped `httptest` GitHub responses and a real
local bare Git repository. They cover pagination, a rate-limit retry, provider
identity, comment and PR reconciliation, repeated push, an approved full run,
and a rejected run with no remote branch or PR. A real private-repository gate
is intentionally manual because it mutates external state; follow the operator
steps in `examples/github/README.md` only with a disposable issue and explicit
confirmation.
