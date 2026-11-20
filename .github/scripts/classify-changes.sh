#!/usr/bin/env bash
# Classify changed paths into Go, site, course, PITR-native and
# Dragonfly-native, and MySQL workload jobs; uncertainty selects all.
#
# pitr gates test-pitr-native: the replay code paths (pitr-download,
# the planner, the sidecar binlog reader, restore.py, verify_script.sh), the
# packages they link that change what is replayed (internal/mysql's GTID sets,
# internal/backupcrypto's archive decryption, the controller's PITR Job
# rendering, backup.go's embedding of restore.py, and the Python harness
# in script_credentials_test.go that the native replay test runs it with)
# and their contracts. The same job runs the native MySQL launcher test
# (TestMySQLLauncherNative, #265), so pitr also covers what renders the mysql
# container's launcher: mysql_launcher*.go and encryption.go's keyring steps.
# It also runs the native own-loss lag proof test
# (TestOwnLossLagProofNative), so pitr covers the proof and its caller:
# own_transactions_lost*.go and gtid_audit.go (internal/mysql, which reads
# the positions, is covered already).
#
# dragonfly gates test-dragonfly-native (the TestDragonflyNative* tests in
# internal/dragonfly and internal/controller, against real Dragonfly
# servers): the client, the controller's Dragonfly files (the manager, the
# rollout and upgrade paths, the planned failover's Dragonfly phase, which
# also render the flags the servers are started with), and the files that
# name the Dragonfly image the playground and the CRD examples pin.
# workload gates the native dashboard test against the example scrape config,
# the default MySQL image, and the monitor account's provisioning.
# telemetry gates the usage report server's Rust checks (telemetry-server/).
set -euo pipefail

go=false site=false course=false pitr=false dragonfly=false workload=false telemetry=false any=false
all() { go=true; site=true; course=true; pitr=true; dragonfly=true; workload=true; telemetry=true; }
while IFS= read -r f || [ -n "$f" ]; do
  [ -n "$f" ] || continue
  any=true
  case "$f" in *.go) go=true ;; esac
  case "$f" in
    cmd/bloodraven/*|internal/pitrplan/*|internal/pitrarchive/*|internal/backupmeta/*|internal/sidecar/*|internal/mysql/*|internal/backupcrypto/*|internal/controller/pitr*.go|internal/controller/mysql_launcher*.go|internal/controller/encryption.go|internal/controller/own_transactions_lost*.go|internal/controller/gtid_audit.go|internal/controller/backup.go|internal/controller/script_credentials_test.go|internal/controller/restore_script.py|internal/controller/verify_script.sh|go.mod|go.sum) pitr=true ;;
  esac
  case "$f" in
    internal/dragonfly/*|internal/controller/dragonfly_*.go|internal/controller/planned_failover_df*.go|api/v1beta1/dragonfly_types.go|playground/manifests/failovergroup.yaml|go.mod|go.sum) dragonfly=true ;;
  esac
  case "$f" in
    charts/bloodraven/dashboards/mysql-workload.json|examples/mysqld-exporter*.yaml|internal/controller/mysql_workload_native_test.go|internal/controller/own_transactions_lost_native_test.go|internal/controller/reconciler.go|internal/controller/credentials.go|internal/controller/init_users.go|internal/mysql/*|go.mod|go.sum) workload=true ;;
  esac
  case "$f" in
    .github/workflows/ci.yml|.github/actions/*|.github/scripts/*|Makefile|.gitignore) all ;;
    telemetry-server/*|.github/workflows/deploy-telemetry.yml) telemetry=true ;;
    site/public/courses/*) site=true; course=true ;;
    site/*) site=true ;;
    course/*) course=true ;;
    # Go tests pin these two workflows: the full profile's shard matrix and
    # job timeout (TestFullProfileShardsFitCIJob) and the pitr job's
    # scenario list (TestPITRScenariosInPITRE2EJob).
    .github/workflows/e2e.yml|.github/workflows/_e2e.yml) go=true ;;
    .github/*|proposals/*|skills/*|.agents/*|.claude/*|.thespider/*) ;;
    api/*|cmd/*|internal/*|test/*|config/*|charts/*|playground/*|examples/*|hack/*) go=true ;;
    go.mod|go.sum|tools.go|.golangci.yml|Dockerfile|.dockerignore) go=true ;;
    */*) all ;;
    *.md) ;;
    *) all ;;
  esac
done
$any || all
printf 'go=%s\nsite=%s\ncourse=%s\npitr=%s\ndragonfly=%s\nworkload=%s\ntelemetry=%s\n' "$go" "$site" "$course" "$pitr" "$dragonfly" "$workload" "$telemetry"
