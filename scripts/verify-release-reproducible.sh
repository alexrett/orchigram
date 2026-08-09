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

verify_checksums() {
  local expected="$verify_root/expected-checksums.txt"
  local actual="$verify_root/actual-checksums.txt"
  local files=()
  shopt -s nullglob
  files+=(dist/orchigram_*.tar.gz)
  files+=(dist/*.sbom.json)
  files+=(.release/plugin-bundles/*.tar.gz)
  files+=(.release/plugin-bundles/*.spdx.json)
  files+=(.release/dependency-licenses.csv)
  shopt -u nullglob
  if [[ ${#files[@]} -ne 41 ]]; then
    printf 'expected 41 checksummed release artifacts, found %d\n' "${#files[@]}" >&2
    return 1
  fi
  LC_ALL=C shasum -a 256 "${files[@]}" |
    awk '{name=$2; sub(/^.*\//, "", name); print $1 "  " name}' |
    LC_ALL=C sort >"$expected"
  LC_ALL=C sort dist/checksums.txt >"$actual"
  if ! diff -u "$expected" "$actual"; then
    printf 'release checksum manifest does not match the 41 public payloads\n' >&2
    return 1
  fi
}

public_manifest "$verify_root/first.sha256"
verify_checksums
go run github.com/goreleaser/goreleaser/v2@v2.12.7 \
  release --snapshot --clean --parallelism 1 >/dev/null
public_manifest "$verify_root/second.sha256"
verify_checksums

if ! diff -u "$verify_root/first.sha256" "$verify_root/second.sha256"; then
  printf 'public release artifacts are not reproducible\n' >&2
  exit 1
fi
printf 'verified 42 reproducible public release artifacts\n'
