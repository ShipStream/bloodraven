#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cases=0
check() {
  local name="$1" paths="$2" go="$3" site="$4" course="$5" pitr="$6" dragonfly="$7" expected actual
  expected="$(printf 'go=%s\nsite=%s\ncourse=%s\npitr=%s\ndragonfly=%s\n' "$go" "$site" "$course" "$pitr" "$dragonfly")"
  actual="$(printf '%s' "$paths" | "$script_dir/classify-changes.sh")"
  if [ "$actual" != "$expected" ]; then
    printf 'FAIL %s\nexpected:\n%s\nactual:\n%s\n' "$name" "$expected" "$actual" >&2
    exit 1
  fi
  cases=$((cases + 1))
}

#                                                     go    site  course pitr  dragonfly
check site 'site/content/docs/x.md'                   false true  false false false
check course 'course/foo.md'                          false false true  false false
check course-output 'site/public/courses/a.html'      false true  true  false false
check go 'internal/x.go'                              true  false false false false
check root-doc 'README.md'                            false false false false false
check playground-doc 'playground/chaos-scenarios.md'  true  false false false false
check makefile 'Makefile'                             true  true  true  true  true
check ci-workflow '.github/workflows/ci.yml'          true  true  true  true  true
check release-workflow '.github/workflows/release.yml' false false false false false
check module 'go.sum'                                 true  false false true  true
check mixed $'internal/x.go\nsite/a.ts\n'             true  true  false false false
check empty ''                                        true  true  true  true  true
check blank-lines $'\n\n'                             true  true  true  true  true
check unknown-root 'new-build-config'                 true  true  true  true  true
check unknown-tree 'new-tool/config.yaml'             true  true  true  true  true
check unknown-nested-doc 'new-tool/README.md'         true  true  true  true  true
check unknown-dotfile '.new-build-config'             true  true  true  true  true
check mixed-unknown $'site/a.ts\nnew-tool/config.yaml\n' true true true true true
check ci-action '.github/actions/setup/action.yml'    true  true  true  true  true
check ci-script '.github/scripts/classify-changes.sh' true  true  true  true  true
check proposals 'proposals/design.md'                 false false false false false
check go-config '.golangci.yml'                       true  false false false false
check removed-go-path $'site/new.go\ninternal/old.go\n' true true false false false
check gitignore '.gitignore'                          true  true  true  true  true
check site-go 'site/helper.go'                        true  true  false false false
check course-go 'course/helper.go'                    true  false true  false false
check ignored-tree-go 'proposals/example.go'          true  false false false false
check unknown-tree-go 'new-tool/helper.go'            true  true  true  true  true
check pitr-download 'cmd/bloodraven/pitr_download.go' true  false false true  false
check pitr-planner 'internal/pitrplan/plan.go'        true  false false true  false
check pitr-sidecar 'internal/sidecar/binlog_txindex.go' true false false true false
check pitr-sidecar-fixture 'internal/sidecar/testdata/native/failback/a.bin' true false false true false
check restore-script 'internal/controller/restore_script.py' true false false true false
check native-test 'internal/controller/pitr_restore_script_test.go' true false false true false
check script-embedding 'internal/controller/backup.go' true false false true false
check native-python-harness 'internal/controller/script_credentials_test.go' true false false true false
check controller-go 'internal/controller/runner.go'   true  false false false false
check gtid-sets 'internal/mysql/gtid.go'              true  false false true  false
check archive-crypto 'internal/backupcrypto/stream.go' true false false true false
check pitr-fragments 'internal/controller/pitr.go'    true  false false true  false
check pitr-dump-meta-render 'internal/controller/pitr_dump_meta.go' true false false true false
check verify-script 'internal/controller/verify_script.sh' true false false true false
check mysql-client-other 'internal/mysql/sidecar_client.go' true false false true false
check own-loss-lag-proof 'internal/controller/own_transactions_lost.go' true false false true false
check own-loss-native-test 'internal/controller/own_transactions_lost_native_test.go' true false false true false
check own-loss-audit 'internal/controller/gtid_audit.go' true false false true false
check controller-restore-go 'internal/controller/restore.go' true false false false false
check mysql-launcher 'internal/controller/mysql_launcher.go' true false false true false
check mysql-launcher-native-test 'internal/controller/mysql_launcher_native_test.go' true false false true false
check encryption-launcher-steps 'internal/controller/encryption.go' true false false true false
check controller-reconciler 'internal/controller/reconciler.go' true false false false false
check e2e-workflow '.github/workflows/e2e.yml'        false false false false false
# The Dragonfly rule: the client, the controller's Dragonfly files, the
# image pins. None of them starts the PITR job.
check dragonfly-client 'internal/dragonfly/position.go' true false false false true
check dragonfly-native-test 'internal/dragonfly/restart_native_test.go' true false false false true
check dragonfly-manager 'internal/controller/dragonfly_topology.go' true false false false true
check dragonfly-manager-native-test 'internal/controller/dragonfly_restart_native_test.go' true false false false true
check dragonfly-flags 'internal/controller/dragonfly_resources.go' true false false false true
check dragonfly-planned-phase 'internal/controller/planned_failover_df.go' true false false false true
check dragonfly-planned-phase-test 'internal/controller/planned_failover_df_test.go' true false false false true
check dragonfly-image-playground 'playground/manifests/failovergroup.yaml' true false false false true
check dragonfly-image-api 'api/v1alpha1/dragonfly_types.go' true false false false true
check planned-failover-mysql 'internal/controller/planned_failover_ops.go' true false false false false
check playground-other-manifest 'playground/manifests/counter.yaml' true false false false false
check dragonfly-and-pitr $'internal/dragonfly/client.go\ninternal/pitrplan/plan.go\n' true false false true true

printf 'Passed %s classifier cases.\n' "$cases"
