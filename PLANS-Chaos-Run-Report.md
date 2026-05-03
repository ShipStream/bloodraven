# Chaos suite run on `bloodraven-dragonfly` k3d cluster

**Latest run command:** `GOCACHE=/tmp/go-build make chaos-run-all`  
**Latest run window:** 2026-05-03T18:39:17Z through 2026-05-03T19:18:51Z  
**Suite revision:** local `dragonfly` worktree with Dragonfly readiness, reset, RustFS, and scenario hardening changes

## Headline result

| Metric | Value |
| --- | --- |
| Scenarios attempted | 32 |
| PASS | 32 |
| FAIL | 0 |
| Reset boundaries exercised | 2 (`10-full-bootstrap-after-data-wipe`, `13-operator-kill-during-bootstrap`) |

The full playground chaos suite now passes with Dragonfly enabled. This covers the original MySQL failover matrix, the Dragonfly planned/emergency/snapshot-upgrade paths, the reset hooks, and the cleanup/precheck behavior that previously caused Dragonfly degradation to cascade into unrelated scenario failures.

## Per-scenario result

```
PASS  01-clean-primary-kill
PASS  02-operator-kill-restart
PASS  02-planned-switchover
PASS  04-data-integrity-on-failover
PASS  05-operator-kill-during-failover
PASS  05-split-brain-auto-resolve
PASS  06-self-fence-isolated-primary
PASS  08-gtid-divergence-detection
PASS  09-anti-flap-cooldown
PASS  09-network-partition-self-fence
PASS  10-full-bootstrap-after-data-wipe
PASS  11-total-loss-recovery
PASS  12-old-primary-recovery-no-divergence
PASS  12-rolling-update-healthy-state
PASS  13-operator-kill-during-bootstrap
PASS  14-failover-with-replication-lag
PASS  15-sidecar-crash-no-failover
PASS  16-mysql-process-kill
PASS  17-partition-replica-no-failover
PASS  18-rapid-cr-spec-changes-during-failover
PASS  19-reclone-interlock
PASS  20-shared-node-selector-isolation
PASS  21-noexecute-eviction-semantics
PASS  22-planned-dragonfly-switchover
PASS  22-replication-status-after-recovery
PASS  23-dragonfly-master-kill
PASS  23-failover-state-durability
PASS  24-emergency-mysql-dragonfly-down
PASS  25-operator-restart-mid-dragonfly-failover
PASS  26-planned-dragonfly-sync-timeout-proceed
PASS  27-dragonfly-rolling-image-update
PASS  29-dragonfly-snapshot-upgrade
```

## Bugs found and fixed while getting to green

- Chaos cleanup now waits for Dragonfly readiness when `spec.dragonfly.enabled=true`: `status.dragonfly.phase=Ready`, one master, and healthy replicas. This fixed the original cascade where scenario 01 completed MySQL cleanup while Dragonfly was still recovering from `REPLTAKEOVER`, causing every following precheck to fail.
- Dragonfly-only master loss now promotes the surviving replica from `DragonflyManager` without changing MySQL `status.activeSite`.
- `status.dragonfly.activeSite` self-heals from a single observed raw Dragonfly master so stale status after operator restart does not classify the real master as stale.
- Scenario 23 targets the peer of `status.dragonfly.activeSite`, not the peer of MySQL `status.activeSite`, because Dragonfly and MySQL can legitimately diverge after Dragonfly-only promotion.
- Scenario 25 creates a deterministic `WaitingForDragonflySync` window, ignores stale planned-failover status from prior runs, and waits for observable MySQL/Dragonfly active-site convergence after `Succeeded`.
- Scenario 18 now scales the old primary back up and waits for the final memory-request rollout to reach every deployment.
- Scenario 29 provisions the RustFS `dragonfly` bucket from the runner with the AWS SDK S3 client, restarts RustFS before bucket setup, and retries with fresh port-forwards so stale PVCs and startup races do not break snapshot-upgrade setup.
- `cmd/playground-chaos reset` now wipes only deterministic MySQL local-path storage, not broad `pvc-*` directories. That preserves unrelated local-path PVs such as RustFS.
- The operator RBAC now allows pod `delete`, which the Dragonfly snapshot-upgrade path needs when it restarts the active Dragonfly pod.

## Gate status after the run

The same worktree also passed:

```
GOCACHE=/tmp/go-build make generate && GOCACHE=/tmp/go-build make manifests
GOCACHE=/tmp/go-build make vet
PATH=/tmp/go-bin:$PATH GOCACHE=/tmp/go-build GOLANGCI_LINT_CACHE=/tmp/golangci-lint-cache make lint
GOCACHE=/tmp/go-build make test
KUBEBUILDER_ASSETS=$(/tmp/go-bin/setup-envtest use --bin-dir /tmp/envtest-bin -p path) GOCACHE=/tmp/go-build make test-envtest
git diff --check
```

`make test` and `make test-envtest` require loopback/listener permissions in this sandbox. `test-envtest` also requires envtest assets; the command above mirrors CI by resolving them through `setup-envtest`.
