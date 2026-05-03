#!/usr/bin/env bash
# Compatibility wrapper for the typed Go reset implementation.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# shellcheck source=playground/_guard.sh
source "$SCRIPT_DIR/_guard.sh"
require_playground_context

cd "$REPO_ROOT"
exec go run ./cmd/playground-chaos reset "$@"
