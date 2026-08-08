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

# Normalize known upstream metadata gaps so the inventory does not depend on
# network/cache state. mathutil omits its license file from the module zip;
# go-licenses can also intermittently omit cel.dev/expr's repository URL.
sed \
  -e 's#cel.dev/expr,Unknown,Apache-2.0#cel.dev/expr,https://github.com/cel-expr/cel-spec/blob/v0.25.2/LICENSE,Apache-2.0#' \
  -e 's#modernc.org/mathutil,Unknown,Unknown#modernc.org/mathutil,https://gitlab.com/cznic/mathutil/-/blob/master/LICENSE,BSD-3-Clause#' \
  "$temporary" \
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
