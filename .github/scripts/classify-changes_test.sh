#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cases=0
check() {
  local name="$1" paths="$2" go="$3" site="$4" course="$5" expected actual
  expected="$(printf 'go=%s\nsite=%s\ncourse=%s\n' "$go" "$site" "$course")"
  actual="$(printf '%s' "$paths" | "$script_dir/classify-changes.sh")"
  if [ "$actual" != "$expected" ]; then
    printf 'FAIL %s\nexpected:\n%s\nactual:\n%s\n' "$name" "$expected" "$actual" >&2
    exit 1
  fi
  cases=$((cases + 1))
}

check site 'site/content/docs/x.md' false true false
check course 'course/foo.md' false false true
check course-output 'site/public/courses/a.html' false true true
check go 'internal/x.go' true false false
check root-doc 'README.md' false false false
check playground-doc 'playground/chaos-scenarios.md' true false false
check makefile 'Makefile' true true true
check ci-workflow '.github/workflows/ci.yml' true true true
check release-workflow '.github/workflows/release.yml' false false false
check module 'go.sum' true false false
check mixed $'internal/x.go\nsite/a.ts\n' true true false
check empty '' true true true
check blank-lines $'\n\n' true true true
check unknown-root 'new-build-config' true true true
check unknown-tree 'new-tool/config.yaml' true true true
check unknown-nested-doc 'new-tool/README.md' true true true
check unknown-dotfile '.new-build-config' true true true
check mixed-unknown $'site/a.ts\nnew-tool/config.yaml\n' true true true
check ci-action '.github/actions/setup/action.yml' true true true
check ci-script '.github/scripts/classify-changes.sh' true true true
check proposals 'proposals/design.md' false false false
check go-config '.golangci.yml' true false false
check removed-go-path $'site/new.go\ninternal/old.go\n' true true false
check gitignore '.gitignore' true true true
check site-go 'site/helper.go' true true false
check course-go 'course/helper.go' true false true
check ignored-tree-go 'proposals/example.go' true false false
check unknown-tree-go 'new-tool/helper.go' true true true

printf 'Passed %s classifier cases.\n' "$cases"
