# GitHub SDLC tracer

The bundled GitHub plugin polls repository issue events for the transition in
which an operator adds `orchigram:ready`. Repository issue-event IDs are the
provider cursor and event identity. Pagination is consumed before events are
sorted by ID; a rate-limited GET is retried with bounded delay. The daemon
persists the event receipt, outbox command, and cursor before acknowledging the
plugin stream.

A new subscription receives the Trigger generation's activation time, which is
persisted atomically with the resource apply. With the default
`replayExisting: false`, the plugin ignores matching events outside a bounded
one-minute safety overlap, so installing a trigger cannot unexpectedly execute
a repository's entire label history while same-second provider timestamps and
up to one minute of clock skew cannot lose a new event. The deliberate tradeoff
is that a matching event from the preceding minute can be accepted once. Stable
event IDs make overlap replay durable and idempotent. `replayExisting: true` is
an explicit provider option for operator-controlled backfills. A Trigger
generation change resets the provider cursor atomically and rejects late events
from the superseded watch; after an event is acknowledged, the current
generation's numeric cursor takes precedence and restart replay remains
lossless.

Current issue events are decoded from their embedded `issue` object without a
second request. `issue_url` remains a compatibility fallback for older recorded
shapes; an event with neither source is rejected with a provider error.

The plugin talks to the GitHub REST API directly. It does not execute `gh` and
accepts a fine-grained PAT only as an operation-scoped SecretRef binding. The
same secret is provided to system `git` through an ephemeral `http.extraHeader`
environment configuration, not embedded in a clone URL or command argument.
GitHub smart HTTP receives a Basic credential for `x-access-token:<token>`.
HTTP(S) clone URLs containing userinfo are rejected, and the token is redacted
from child output and returned results.

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
default branch. In v0.1, revisions are ordinary operator Git/GitHub pushes to
the existing branch and PR; review-event automation is not implemented. A new
provider-triggered Run has a new Run UID and derives a new deterministic branch.
The reviewed PR is merged only through ordinary GitHub controls.

[`examples/self-sdlc`](../examples/self-sdlc/README.md) is the portable version
used to operate a repository through its own Unix-socket-only daemon. Its owner,
repository, and profile values are explicitly replaceable; it contains only
SecretRef names and portable executable defaults, never credentials or
machine-specific addresses.

## Verification boundary

Automated tests use recorded-shaped `httptest` GitHub responses and a real
local bare Git repository. They cover pagination, a rate-limit retry, provider
identity, one receipt and Run UID across provider restart replay, durable
approval, test-before-push ordering, deterministic branch reuse, and ambiguous
comment/PR successes reconciled by hidden marker. A second provider occurrence
is rejected after its reconciled planning comment and proves that no
implementation, remote branch, push, or additional PR follows. A real
private-repository gate is intentionally manual because it mutates external
state; follow the operator steps in `examples/github/README.md` only with a
disposable issue and explicit confirmation.
