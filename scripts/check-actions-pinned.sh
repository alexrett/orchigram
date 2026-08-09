#!/usr/bin/env bash
set -euo pipefail

failed=0
while IFS= read -r entry; do
  location="${entry%%:uses:*}"
  action="${entry#*:uses: }"
  action="${action%% *}"
  revision="${action##*@}"
  if [[ ! "${revision}" =~ ^[0-9a-f]{40}$ ]]; then
    printf 'GitHub Action is not commit-pinned: %s (%s)\n' "${location}" "${action}" >&2
    failed=1
  fi
done < <(rg -n --no-heading -o 'uses: [^[:space:]#]+' .github/workflows)

exit "${failed}"
