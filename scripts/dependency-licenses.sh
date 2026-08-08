#!/usr/bin/env bash
set -euo pipefail

output="${1:-.release/dependency-licenses.csv}"
output_directory="$(dirname "$output")"
mkdir -p "$output_directory"
temporary="$(mktemp)"
normalized="$(mktemp)"
trap 'rm -f "$temporary" "$normalized"' EXIT

go run github.com/google/go-licenses@v1.6.0 report ./... \
  --ignore=github.com/alexrett/orchigram >"$temporary"

# v1.7.1 omits its LICENSE file from the module zip. The upstream repository
# and pkg.go.dev both classify modernc.org/mathutil as BSD-3-Clause.
sed 's#modernc.org/mathutil,Unknown,Unknown#modernc.org/mathutil,https://gitlab.com/cznic/mathutil/-/blob/master/LICENSE,BSD-3-Clause#' "$temporary" \
  | LC_ALL=C sort >"$normalized"

if grep -q ',Unknown,Unknown$' "$normalized"; then
  echo "dependency license classification is incomplete" >&2
  grep ',Unknown,Unknown$' "$normalized" >&2
  exit 1
fi

{
  echo 'package,license_url,spdx_license'
  cat "$normalized"
} >"$output"
