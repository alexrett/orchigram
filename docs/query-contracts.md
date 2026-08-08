# Query and watch contracts

Resource list order is always `kind`, `namespace`, then `name`. An empty kind or
namespace matches all values. Every supplied label must exist with the exact
value; selectors use AND semantics.

`limit` defaults to 100 and cannot exceed 1000. A non-empty
`continue_token` is opaque and bound to the original kind, namespace, labels,
and global resource revision. Pagination uses the last returned key rather than
an offset, so an unchanged traversal returns every matching resource exactly
once. If any resource or controller status changes between pages, the daemon
returns `ABORTED` and the client restarts from the first page. It never silently
mixes two revisions. Invalid, oversized, or filter-mismatched tokens return
`INVALID_ARGUMENT`.

Resource watches replay durable events strictly after `after_revision` and
retain global revision order. Kind and namespace filters apply to `ADDED`, spec
`MODIFIED`, status-only `MODIFIED`, and `DELETED` events. A delete event has no
resource document. The v0.1 store does not compact this event log.

Export returns deterministic multi-document YAML in request-key order. Server
status is removed because it is not desired state and cannot be applied by a
client. Server metadata remains available for identity and compare-and-swap;
the normal Apply path decides which fields are authoritative.

Run list filters compose by Flow and phase. `flow` accepts either the current
default-namespace Flow name or a pinned Flow UID, which keeps historical runs
queryable after a Flow is deleted. Valid phases are `pending`, `running`,
`waiting`, `succeeded`, `failed`, `rejected`, and `cancelled`. Results are
newest-first with UID as the deterministic tie-breaker, and `limit` has the same
100/1000 default and maximum.
