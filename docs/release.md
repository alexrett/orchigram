# Release process

Orchigram releases are built from a clean tag by GoReleaser. The publication
gate is:

```console
make release-check
```

It runs generated-code checks, formatting, vet, unit and race suites, lint,
Buf lint/build, worktree and full-history secret scans, local and cross builds,
license classification, SPDX SBOM generation, and a GoReleaser snapshot.

## Artifacts

Every release contains:

- four core archives for macOS/Linux on amd64/arm64; each archive contains the
  `orchigram` client/server and matching first-party plugin executables;
- one independent bundle per first-party plugin and target. Every bundle has a
  strict `plugin.yaml`, protocol range, capabilities and payload SHA-256;
- SPDX JSON SBOMs for core archives and plugin bundles;
- `dependency-licenses.csv`, `checksums.txt`, GoReleaser metadata, and GitHub
  build provenance attestations.

Build timestamps come from the source commit. Go binaries use `-trimpath`,
`-buildvcs=false`, a cleared build ID, and fixed version/commit/date values.
Plugin tar/gzip headers are normalized to the Unix epoch. GoReleaser runs with
one packaging worker so archive member ordering is stable; the release packager
also has a byte-for-byte reproducibility test.

First-party release packaging and `orchigram plugin pack` share the canonical
bundle builder: sorted capabilities/platforms and archive members, fixed modes,
IDs, names, timestamps, tar format, and gzip headers. Community outputs are
created exclusively and atomically unless the operator supplies `--force`.
Cross-build validation covers the first-party SDK bootstraps and external echo
module conformance test.

## External tracer checkpoints

CI exercises the Teams flow only against an `httptest` receiver. Sending a
real Teams Adaptive Card requires an operator-provided `SecretRef` and explicit
confirmation. The GitHub issue-to-PR tracer uses recorded fixtures by default;
a live run requires a dedicated repository and fine-grained PAT. Orchigram
never merges the pull request or pushes to the default branch.

Publishing a tag creates a draft GitHub release. Review its checksums, SBOMs,
license inventory and attestations before making it public.
