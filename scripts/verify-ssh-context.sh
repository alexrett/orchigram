#!/usr/bin/env bash
set -euo pipefail

: "${ORCHIGRAM_TEST_SSH_DESTINATION:?set an OpenSSH destination such as operator@example.net}"

ORCHIGRAM_BIN="${ORCHIGRAM_BIN:-orchigram}"
ORCHIGRAM_TEST_FLOW="${ORCHIGRAM_TEST_FLOW:-operator-smoke}"
verify_root="$(mktemp -d "${TMPDIR:-/tmp}/orchigram-ssh-verify.XXXXXX")"
trap 'rm -rf -- "$verify_root"' EXIT

contexts_file="$verify_root/contexts.yaml"
events_file="$verify_root/events.log"

"$ORCHIGRAM_BIN" --contexts "$contexts_file" context set remote \
  --ssh-destination "$ORCHIGRAM_TEST_SSH_DESTINATION"
"$ORCHIGRAM_BIN" --contexts "$contexts_file" context use remote >/dev/null
"$ORCHIGRAM_BIN" --contexts "$contexts_file" plugin list
"$ORCHIGRAM_BIN" --contexts "$contexts_file" get Flow "$ORCHIGRAM_TEST_FLOW" >/dev/null

run_uid="$($ORCHIGRAM_BIN --contexts "$contexts_file" run start "$ORCHIGRAM_TEST_FLOW" \
  --input '{}' --idempotency-key "ssh-verification-$(date +%s)")"
"$ORCHIGRAM_BIN" --contexts "$contexts_file" run watch "$run_uid" >"$events_file" &
watch_pid=$!

for _ in $(seq 1 200); do
  if grep -q $'\tapproval.waiting\t' "$events_file"; then
    break
  fi
  if ! kill -0 "$watch_pid" 2>/dev/null; then
    wait "$watch_pid"
  fi
  sleep 0.05
done

grep -q $'\tapproval.waiting\t' "$events_file"
"$ORCHIGRAM_BIN" --contexts "$contexts_file" run approve "$run_uid" \
  --node approval --reason "SSH context verification"
wait "$watch_pid"
grep -q $'\trun.succeeded\t' "$events_file"
printf 'SSH context tracer passed for run %s\n' "$run_uid"
