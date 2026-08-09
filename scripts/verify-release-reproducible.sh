#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
verify_root="$(mktemp -d "${TMPDIR:-/tmp}/orchigram-release-reproducibility.XXXXXX")"
trap 'rm -rf -- "$verify_root"' EXIT

cd "$repo_root"

public_manifest() {
  local output="$1"
  local files=()
  shopt -s nullglob
  files+=(dist/orchigram_*.tar.gz)
  files+=(dist/*.sbom.json)
  files+=(dist/checksums.txt)
  files+=(.release/plugin-bundles/*.tar.gz)
  files+=(.release/plugin-bundles/*.spdx.json)
  files+=(.release/dependency-licenses.csv)
  shopt -u nullglob
  if [[ ${#files[@]} -ne 42 ]]; then
    printf 'expected 42 public release files, found %d\n' "${#files[@]}" >&2
    return 1
  fi
  LC_ALL=C shasum -a 256 "${files[@]}" | LC_ALL=C sort >"$output"
}

public_manifest "$verify_root/first.sha256"
go run github.com/goreleaser/goreleaser/v2@v2.12.7 \
  release --snapshot --clean --parallelism 1 >/dev/null
public_manifest "$verify_root/second.sha256"

if ! diff -u "$verify_root/first.sha256" "$verify_root/second.sha256"; then
  printf 'public release artifacts are not reproducible\n' >&2
  exit 1
fi
printf 'verified 42 reproducible public release artifacts\n'
