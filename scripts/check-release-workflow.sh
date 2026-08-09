#!/usr/bin/env bash
set -euo pipefail

workflow=.github/workflows/release.yaml

require_count() {
  local expected="$1"
  local pattern="$2"
  local actual
  actual="$(grep -Fxc -- "$pattern" "$workflow" || true)"
  if [[ "$actual" -ne "$expected" ]]; then
    printf 'expected %d occurrence(s) of %q in %s, found %d\n' \
      "$expected" "$pattern" "$workflow" "$actual" >&2
    exit 1
  fi
}

require_count 1 '          subject-checksums: dist/checksums.txt'
require_count 1 '          subject-path: dist/checksums.txt'

if grep -Fq 'subject-path: "dist/*"' "$workflow"; then
  printf 'release provenance must not be limited to dist/*\n' >&2
  exit 1
fi

printf 'release workflow attests checksummed payloads and the checksum manifest\n'
