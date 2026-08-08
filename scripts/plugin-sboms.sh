#!/usr/bin/env bash
set -euo pipefail

bundle_directory="${1:-.release/plugin-bundles}"
created="${ORCHIGRAM_SBOM_DATE:-$(git show -s --format=%cI HEAD)}"
shopt -s nullglob
bundles=("$bundle_directory"/*.tar.gz)
if ((${#bundles[@]} == 0)); then
  echo "no plugin bundles found under $bundle_directory" >&2
  exit 1
fi

for bundle in "${bundles[@]}"; do
  output="${bundle%.tar.gz}.spdx.json"
  syft "$bundle" --output "spdx-json=$output" --enrich all >/dev/null
  go run ./cmd/orchigram-release normalize-sbom \
    --file "$output" \
    --artifact "$bundle" \
    --date "$created"
done
