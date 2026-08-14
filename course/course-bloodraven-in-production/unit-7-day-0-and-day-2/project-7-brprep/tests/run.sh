#!/usr/bin/env bash
# The grader. Drives starter/brprep.sh over every fixture and diffs its output
# — stdout, stderr and exit code — against tests/expected/.
#
#   ./tests/run.sh              every fixture
#   ./tests/run.sh good plan    only the named ones
#
# Do not edit anything under tests/. The fixtures and expectations are the
# grader's copy; changing them changes the question rather than the answer.
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(dirname "$HERE")"
BRPREP="$ROOT/starter/brprep.sh"

command -v jq >/dev/null 2>&1 || { echo "run.sh: jq is required (apk add jq / apt install jq)" >&2; exit 2; }
[ -x "$BRPREP" ] || chmod +x "$BRPREP" 2>/dev/null || true

# case name -> the arguments brprep is invoked with, relative to $ROOT.
case_args() {
  case "$1" in
    good)          echo "tests/fixtures/good.json --target-image mysql:9.8" ;;
    plan)          echo "tests/fixtures/good.json --target-image mysql:9.8" ;;
    plan-nochange) echo "tests/fixtures/good.json --target-image mysql:9.7" ;;
    *)             echo "tests/fixtures/$1.json" ;;
  esac
}

# `plan` and `good` share a fixture and an expectation; `plan` is the name the
# rubric's test case uses, so it resolves to the same file.
expected_for() {
  case "$1" in
    plan) echo "$HERE/expected/good.txt" ;;
    *)    echo "$HERE/expected/$1.txt" ;;
  esac
}

ALL=(good both-credentials one-candidate dup-sites bad-priorities lease-too-short reader-lbip silently-wrong plan-nochange)
if [ $# -gt 0 ]; then CASES=("$@"); else CASES=("${ALL[@]}"); fi

pass=0
fail=0
for name in "${CASES[@]}"; do
  exp="$(expected_for "$name")"
  if [ ! -r "$exp" ]; then
    echo "?? $name — no expectation at $exp"; fail=$((fail + 1)); continue
  fi
  # shellcheck disable=SC2046
  actual="$( cd "$ROOT" && bash "$BRPREP" $(case_args "$name") 2>&1 )"
  code=$?
  actual="$actual
exit=$code"
  if [ "$actual" = "$(cat "$exp")" ]; then
    echo "ok   $name"
    pass=$((pass + 1))
  else
    echo "FAIL $name"
    diff -u <(cat "$exp") <(printf '%s\n' "$actual") | sed 's/^/     /' | head -30
    fail=$((fail + 1))
  fi
done

echo
if [ "$fail" -eq 0 ]; then
  echo "ALL ${pass} FIXTURES MATCH"
  exit 0
fi
echo "${fail} failing fixture(s), ${pass} passing"
exit 1
