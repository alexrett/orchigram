#!/usr/bin/env bash
set -euo pipefail

bundle_directory="${1:-.release/plugin-bundles}"
shopt -s nullglob
bundles=("$bundle_directory"/*.tar.gz)
if ((${#bundles[@]} == 0)); then
  echo "no plugin bundles found under $bundle_directory" >&2
  exit 1
fi

for bundle in "${bundles[@]}"; do
  output="${bundle%.tar.gz}.spdx.json"
  syft "$bundle" --output "spdx-json=$output" --enrich all >/dev/null
done
