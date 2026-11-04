#!/usr/bin/env bash
# Classify changed paths into Go, site, and course jobs; uncertainty selects all.
set -euo pipefail

go=false site=false course=false any=false
while IFS= read -r f || [ -n "$f" ]; do
  [ -n "$f" ] || continue
  any=true
  case "$f" in *.go) go=true ;; esac
  case "$f" in
    .github/workflows/ci.yml|.github/actions/*|.github/scripts/*|Makefile|.gitignore) go=true; site=true; course=true ;;
    site/public/courses/*) site=true; course=true ;;
    site/*) site=true ;;
    course/*) course=true ;;
    .github/*|proposals/*|skills/*|.agents/*|.claude/*|.thespider/*) ;;
    api/*|cmd/*|internal/*|test/*|config/*|charts/*|playground/*|examples/*|hack/*) go=true ;;
    go.mod|go.sum|tools.go|.golangci.yml|Dockerfile|.dockerignore) go=true ;;
    */*) go=true; site=true; course=true ;;
    *.md) ;;
    *) go=true; site=true; course=true ;;
  esac
done
$any || { go=true; site=true; course=true; }
printf 'go=%s\nsite=%s\ncourse=%s\n' "$go" "$site" "$course"
