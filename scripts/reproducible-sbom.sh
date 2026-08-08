#!/usr/bin/env bash
set -euo pipefail

if (($# != 3)); then
  echo "usage: reproducible-sbom.sh ARTIFACT DOCUMENT RFC3339_DATE" >&2
  exit 2
fi

artifact="$1"
document="$2"
created="$3"

syft "$artifact" --output "spdx-json=$document" --enrich all >/dev/null
go run ../cmd/orchigram-release normalize-sbom \
  --file "$document" \
  --artifact "$artifact" \
  --date "$created"
