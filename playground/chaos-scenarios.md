# Bloodraven Chaos Testing Scenarios

## Automated runner

A subset of the scenarios below are wired into a deterministic Go test runner at `cmd/playground-chaos`. The runner injects failure, polls the live cluster (CR status, sidecar HTTP, operator metrics, structured logs), and asserts the documented outcomes with deadlines. It bails on the first assertion failure and dumps a forensic capture (`cluster.yaml`, `pods.yaml`, `events.yaml`, `operator.log`, `sidecar-<site>.log`, `metrics.txt`, `scenario.log`, `failure.txt`) under `playground/chaos-results/<timestamp>/<scenario-id>/` for an operator or AI agent to triage.

Run from the repo root:

```
make chaos-list                           # list registered scenarios
make chaos-check                          # verify the playground baseline is healthy
make chaos-run SCENARIO=01-clean-primary-kill
make chaos-run-all                        # bail on first failure (default)
make test-e2e E2E_PROFILE=full E2E_SHARD=2/4   # part 2 of 4 of the full profile, on this cluster
```

`run-all --shard=<i>/<n>` runs one of `n` parts of a profile. The parts are disjoint, together they are the whole profile, and they are balanced by each scenario's `Timeout` (plus an allowance for a `ResetBeforeRunAll` reset), so the long PITR scenarios land in different parts. Each part needs its own cluster; CI runs the full profile as four such jobs because one job cannot finish it inside its timeout. `./bin/playground-chaos list --profile=full --shard=<i>/<n>` prints a part. Adding a scenario or changing a `Timeout` can move other scenarios between parts, so do not write a scenario that depends on another one having run before it. `--shard` is refused together with `--scenarios`.

The runner stamps an in-progress marker (`playground.bloodraven.shipstream.io/in-progress` annotation on the MFG) after Precheck and clears it on cleanup or on the pass path. A subsequent run that finds a leftover marker refuses to start and tells you whether the prior owner is still alive (same host + live pid), abandoned (same host + dead pid), or on another host. Override with:

- `--force` — delete any prior marker before preflight (banner printed).
- `--auto-reset` — on Precheck failure, shell out to `./playground/reset-mysql.sh && ./playground/setup.sh`, then retry the scenario once. Wipes data; 3-second confirmation pause unless `CI=1`.

A scenario can report measurements (`Capture.Measure`), which are not assertions: `run` and `run-all` print them as `measured: …` lines under the scenario's `PASS`/`FAIL` line, and `--junit-out` writes them into that test case's `<system-out>`, so a passing CI run shows them. Scenario `01-clean-primary-kill` reports the failover's RTO under write load this way (§1).

`chaos-check` runs the same structural baseline scenarios use (stuck scale-to-0 deployments, bogus `lastFailover.target`, anti-flap cooldown still ticking, all-sites-read-only `NoPrimary` symptom, replication off on a non-active candidate, a standing planned-failover automation hold) and prints `inProgress: yes/no + summary`. Each error includes the exact remediation command, so `chaos-check` is the fastest way to decide whether to re-run, `--force`, or reset.

After each scenario, standard cleanup waits for the MFG `Ready=True` condition, stable MySQL site states, and, when `spec.dragonfly.enabled=true`, `status.dragonfly.phase=Ready` with exactly one master and healthy replicas. This keeps `run-all` from starting the next scenario while Dragonfly is still recovering from an expected REPLTAKEOVER or pod restart.

Some bootstrap scenarios exercise operator branches that only exist before any failover has stamped `status.lastFailover.target`. When `run-all` reaches those scenarios (`10-full-bootstrap-after-data-wipe` and `13-operator-kill-during-bootstrap`), the runner performs an explicit `reset` first so the precondition is real rather than hidden behind a broad auto-reset. Direct `run <scenario-id>` invocations still fail precheck on a non-pristine cluster and tell you to reset manually. To run a chosen list in order with the same suite handling (resets, the baseline wait, JUnit), use `./bin/playground-chaos run-all --scenarios=<id>,<id>,...`; CI's scenario-list jobs (`encryption`, `pitr`) run that way.

Currently automated (every `runner.Register` entry in `internal/playground/scenarios/`):

- `01-clean-primary-kill` (§1 below; uses `scale --replicas=0` for determinism, asserts the failover and not the old primary's recovery; the failover runs under a write load and the scenario measures the RTO to the first write on the new primary and the promotion's `FLUSH BINARY LOGS`; that rotation is best effort, so one that is slow or not acknowledged is reported and does not fail the run, #229)
- `02-operator-kill-restart` (§2; negative-assertion — verifies activeSite stable and no SELF-FENCING during operator restart)
- `02-planned-switchover` (§S; planned-failover state machine — annotates the MFG with `bloodraven.shipstream.io/planned-failover=<peer>`, asserts `Admitting→Fencing→Draining→WaitingForLag→Promoting→Publishing→Succeeded`, outcome `Succeeded` with `lastFailover.promotionGtidExecuted` equal to `fence.gtidSet`, and the `bloodraven_planned_failovers_total{result="succeeded"}` increment)
- `04-data-integrity-on-failover` (§4; seeds rows, blocks on `WAIT_FOR_EXECUTED_GTID_SET`, kills primary, asserts `GTID_SUBSET(pre, post)=1` and full row count on the new primary)
- `05-operator-kill-during-failover` (§5; scales the active primary to 0, sleeps 1s, kills the operator pod, asserts the cluster reconverges to 1 writable + all followers read-only with `Ready=True` and no sidecar exhausts its operator-and-peer lease during the short gap; an immediate topology-mismatch self-fence is allowed because it safely closes a promotion/publication race)
- `05-split-brain-auto-resolve` (§SBR; requires `spec.splitBrainPolicy.sitePriorities` set — clears `super_read_only` on the read-only site to force both writable, asserts the operator fences the non-preferred site, increments `bloodraven_split_brain_auto_resolve_total{prefer_site=...}`, and logs `split-brain auto-resolve`)
- `06-self-fence-isolated-primary` (§6; scales operator AND every non-self peer, including the reader, to 0 — true isolation path, complements `09-`)
- `08-gtid-divergence-detection` (§8; manufactures a rogue write on the old primary, asserts `recoveryState=RecoveryBlocked` + `divergentTransactionCount>0` + `divergence detected` log; auto-reclones in cleanup)
- `09-anti-flap-cooldown` (§9; force-deletes the active primary, waits for failover + the original primary to come back as a reachable read-only candidate, then scales the new primary to 0 inside `failoverCooldown` and asserts `failovers_total` increments by exactly 1 across all sites — best-effort log scan for `failover blocked by anti-flap cooldown`)
- `09-network-partition-self-fence` (§3 of this doc, NetworkPolicy partition path)
- `10-full-bootstrap-after-data-wipe` (§10; scales a candidate replica to 0, scrubs the per-FG readonly taint from every node, deletes that replica's data PVC, and waits for the operator's `Bootstrapping` condition to cycle through `Cloning` → `WaitingForRestart` → `SetupReplication` (`True`) to `False`/`NotBootstrapping` with a transition after the wipe and the wiped replica's IO+SQL threads ON; a `BootstrapFailed` after the wipe fails the step at once)
- `11-total-loss-recovery` (§11; scales every site to 0, asserts `TOTAL LOSS: all sites are unreachable` log + reconvergence)
- `12-old-primary-recovery-no-divergence` (§7 of this doc; recovery without divergence)
- `12-rolling-update-healthy-state` (§12; patches `spec.sites[*].resources.requests.memory` and asserts `status.updatePhase` engages then is absent again, both deployments end at the new memory request, no `TOTAL LOSS` log fires during the roll)
- `13-operator-kill-during-bootstrap` (§13; wipes the read-only site's PVC, scales it back up, then kills the operator pod once `Bootstrapping=True` and asserts the replacement operator drives the clone to `Status=False Reason=NotBootstrapping` (transition after the wipe) with the wiped site's IO+SQL threads ON; a restarted operator writes `False`/`NotBootstrapping` before it starts a clone, so the replication check is what tells completion apart — runs `ResetBeforeRunAll` so `lastFailover.target` is empty when the precondition gate runs)
- `14-failover-with-replication-lag` (§14; pauses the replica SQL applier, seeds rows over 5s on the primary so they sit in the relay log unapplied, then scales the primary to 0 and asserts the new primary's relay-log drain recovered every transaction — `GTID_SUBSET(post-write, current)=1` and full row count)
- `15-sidecar-crash-no-failover` (§15; ephemeral container `kill 1` against the sidecar PID namespace, asserts restartCount increments and activeSite/SELF-FENC/failover all stay quiet)
- `16-mysql-process-kill` (§16; issues SQL `SHUTDOWN` against the active site's mysqld and asserts the mysql container restartCount increments and the cluster never enters split-brain — accepts either "no failover" or "failover" outcomes, but rejects `>1 writable`, `RecoveryBlocked`, or sustained zero-writable)
- `17-partition-replica-no-failover` (§17; asymmetric partition — asserts NO failover and NO self-fence on read-only site)
- `18-rapid-cr-spec-changes-during-failover` (§18; triggers a failover and applies five rapid memory-request JSON patches at 2s intervals; asserts the cluster converges to {1 writable, 1 read-only, no RecoveryBlocked} with the last-applied memory request landing on every site deployment)
- `19-reclone-interlock` (§19; self-contained — manufactures divergence, then exercises rejected/accepted reclone annotation cases)
- `20-shared-node-selector-isolation` (§20; labels primary node into a fake `inventory` FG and asserts that a playground failover taints only `bloodraven-playground.db-readonly.bloodraven.shipstream.io/playground`, leaving `bloodraven-playground.db-readonly.bloodraven.shipstream.io/inventory` absent and the inventory canary still Running)
- `21-noexecute-eviction-semantics` (§21; deploys tolerating + non-tolerating canaries on the primary's node, asserts the non-tolerating one is deletion-marked or gone post-failover and the tolerating one stays Running)
- `22-planned-dragonfly-switchover` (§24; planned MySQL+Dragonfly failover, session-preservation status, active Service endpoint convergence, and Dragonfly promotion metrics)
- `22-replication-status-after-recovery` (§22; WISHLIST #36 guard — clean primary kill + old-primary auto-recovery, asserts the read-only site's `status.sites[].replication` becomes `Healthy` or `Lagging` within 30s without an operator restart and cross-checks against the sidecar `/status` endpoint)
- `23-dragonfly-master-kill` (§25; Dragonfly-only master kill promotes the replica without changing MySQL activeSite)
- `23-failover-state-durability` (§23; WISHLIST #38 guard — clean failover, then kills the operator pod; asserts post-restart `lastFailover.target`, `activeSite`, and `lastFailover.time` survive, with up to 90s tolerance on the stamp to absorb a status-enrichment rewrite)
- `24-emergency-mysql-dragonfly-down` (§26; all Dragonfly StatefulSets scaled to 0, MySQL emergency failover still completes)
- `25-operator-restart-mid-dragonfly-failover` (§27; operator restart while planned failover is in Dragonfly sync/promotion phase resumes safely)
- `26-planned-dragonfly-sync-timeout-proceed` (§28; `onSyncTimeout=Proceed` marks sessions not preserved and still completes MySQL failover)
- `27-dragonfly-rolling-image-update` (§D6b; ordinary `spec.dragonfly.image` patch rolls one pod at a time and promotes the updated replica before rolling the old active pod)
- `29-dragonfly-snapshot-upgrade` (§29; D6a snapshot-restore upgrade using the playground RustFS bucket)
- `30-backup-verification-rustfs` (§30; configures a RustFS backup profile, creates a real `MysqlBackup`, pins `MysqlBackupVerification.spec.backupRef`, and asserts marker rows restore)
- `30-encrypted-backup-verification-rustfs` (§30; same round trip with `encryption.algorithm=AES-256-GCM`, so the backup Job runs the operator-image `encrypt-upload` container and verification runs the `decrypt-download` init container; asserts `status.encryption.mode=Passphrase`)
- `31-pitr-verification-rustfs` (§31; enables RustFS PITR, archives sealed binlogs, verifies timestamp replay includes pre-target rows and excludes post-target rows)
- `32-mfg-status-write-denial-emergency-promotion` (§32; denies the operator patch/update on `mysqlfailovergroups/status`, scales the active primary to 0, asserts MySQL promotes at the data layer while the CR status is frozen, then self-heals to the promoted site within 90s after RBAC restore)
- `33-scoped-dns-outage` (§33; NetworkPolicy blocking only kube-dns egress from the active MySQL pod — proven with a DNS canary first — asserts the outage is DNS-scoped, not a pod partition and not the DNSEndpoint-API denial of §38)
- `34-operator-kill-during-wait-replica` (§34; force-deletes the operator while an ordered update is held at `WaitReplica`; the replacement operator re-derives remaining drift from Deployment spec-hash mismatch and finishes with no double-roll and no `TOTAL LOSS`)
- `35-planned-switchover-lag-timeout-rollback` (§35; `SOURCE_DELAY=60` on the target + `maxLagWait=5s` drives `WaitingForLag`→`Failed{LagTimeout}`; source stays active, `bloodraven_planned_failovers_total{result="rolled-back"}` increments)
- `36-rustfs-outage-during-restore-in-place` (§36; a per-schema in-place restore whose RustFS storage is scaled to 0 mid-run terminates `Failed` with the topology frozen; the untouched canary schema survives and the terminal `Failed` holds on the same confirm — a retry needs a new monotonic confirm — `ResetBeforeRunAll`)
- `37-pitr-planned-failback-no-reclone` ([§37](#37-pitr-planned-failback-no-reclone); release profile, `ResetBeforeRunAll`; planned A→B→A without reclone or a scenario flush on b; the failback archives b's era as the outgoing primary's tail (#264), so it verifies right after the failback; every era and an `initFromBackup` restore certify exactly the fenced history)
- `38-dnsendpoint-write-denial-during-failover` (§38; denies the operator write verbs on `dnsendpoints`, forces promotion; the DNSEndpoint target stays stale during denial then heals to the promoted LBIP within 90s after RBAC restore with no re-failover)
- `39-dragonfly-master-partition` (§39; deny-all NetworkPolicy on only the Dragonfly master pod promotes the surviving replica; MySQL `status.activeSite` and failover counters are unchanged)
- `40-reader-data-loss-reclone` (§40; release-profile reader PVC replacement, continuous `Ready=True`, auto-clone donor/internal-host log assertion, direct-source/thread/lag/marker recovery, client EndpointSlice shedding, and internal endpoint publication)
- `53-deployment-hold-expiry` ([deployment coordination](#deployment-coordination); release profile, planned failover stays `Deferred/DeploymentHold` until the hold expires, then succeeds)
- `54-deployment-hold-owner-kill` ([deployment coordination](#deployment-coordination); full profile, terminate a real renewing client Pod without releasing its hold; expiry unblocks planned failover)
- `55-deployment-emergency-revocation` ([deployment coordination](#deployment-coordination); release profile, sustained primary outage bypasses live migration and hold leases, both renewals return `409`, and the MFG receives `DeploymentLeaseRevoked`)
- `56-deployment-live-renewal-fenced` ([deployment coordination](#deployment-coordination); full profile, a migration client renewing every five seconds observes `409 revoked` after a planned topology change and stops)
- `63-planned-failover-operator-restart-mid-lag` ([§63](#63-planned-failover-operator-restart-mid-lag); release profile; the planned-failover claim survives an operator kill while the run waits in `WaitingForLag`, then the same run succeeds with one claim grant and one `RESET REPLICA ALL`)
- `64-emergency-failover-during-planned-admission` ([§64](#64-emergency-failover-during-planned-admission); full profile; a planned request `Deferred{DeploymentHold}` does not obstruct emergency failover, and ends `NotStarted{AlreadyActive}` once its target is promoted)
- `62-full-restore-fails-after-drop-keeps-peers` ([§62](#62-full-instance-restore-failing-after-its-drop); full profile, `ResetBeforeRunAll`; a full-instance in-place restore requested with RustFS scaled to 0 drops the user schemas and fails; the reclone queue is `Held` and every peer keeps its replication receiver `OFF` and its marker rows for two minutes, issue #212)
- `75-full-restore-three-site-sessions-spec-removal` ([§75](#75-full-instance-restore-sessions-spec-removal); full profile, `ResetBeforeRunAll`; a three-site full-instance restore from an encrypted backup on a provisioned PVC: the Job decrypts from a read-only claim, the fence ends application sessions held through `-primary` and the per-site Service and refuses new ones, `spec.restoreInPlace` removed while the Job is held in its DROP keeps every guard until the run ends `Succeeded`, the same confirm re-applied starts nothing, and both peers are recloned, #214)
- `68-pitr-failback-with-reclone` ([§68](#68-pitr-failback-with-reclone); release profile, `ResetBeforeRunAll`; failback with reclone is exact in every era, including an era b never flushed, which the failback archives as b's tail (#264; it refused `PITRArchiveGap` before); each reclone starts only after the operator confirmed the recipient's binlog tail archived, with no `PITRArchiveTailNotArchived` Event, and gives the recipient a new `server_uuid` (#265))
- `69-pitr-encryption-binding-refusal` ([§69](#69-pitr-encryption-binding-refusal); full profile and the e2e `pitr` job, `ResetBeforeRunAll`; the encrypted archive binds to an immutable, ownerless key snapshot; refused rotations, encryption edits and aliases do not roll, restarted writers keep the accepted key, bindings survive a status wipe, and the new-prefix procedure and release work, issue #203)
- `70-pitr-lossy-failover` ([§70](#70-pitr-lossy-failover); release profile, `ResetBeforeRunAll`; lost archived set, `excludeGtids`, a backup after the loss, the returning old primary archiving the lost transactions it had not archived (#264), and a reclone and failback that leave the archive usable (#265): the recloned old primary has a new `server_uuid`, its lost rows are divergent evidence, no `PITRArchiveReissueRiskDetected`, no `PITRArchiveHistoryReset`)
- `73-paced-rollout-bad-build-halt` ([§73](#73-paced-rollout-bad-build-halt); release profile and the e2e `paced-rollout` job; a release-driven pod change across two auxiliary groups: one is admitted, a release whose pods cannot become Ready stalls it before its primary is touched and the other group keeps its pods, across an operator restart too; a release that works then completes the first group before the second is admitted; the playground group, pinned to its sidecar image, is never rolled)
- `76-dragonfly-blackhole-mysql-failover` ([§76](#76-blackholed-dragonfly-master-during-a-mysql-failover); full profile; the Dragonfly master's pod is blackholed and the MySQL primary on its site scaled to 0, with one tagged-GTID transaction on both sites: MySQL fails over, the Dragonfly target is stamped master within the bound derived from the emergency budgets (14s) of the MySQL promotion with REPLTAKEOVER skipped, the new primary holds the tagged transaction, and both old masters rejoin as replicas, #199)
- `74-upgrade-paced-rollout` ([§74](#74-upgrade-paced-rollout); quarantined: needs a cluster installed at the published v1.7.0 with a spare node, run by the e2e `upgrade` job after `./playground/upgrade-e2e.sh install`; the upgrade to the build under test rolls every MySQL pod of three groups, one group at a time)
- `77-pitr-archive-store-init-failure` ([§77](#77-pitr-archive-store-init-failure); full profile; encrypted PITR enabled while RustFS is scaled to 0: the ordered update finishes, every pod stays Ready, nothing fails over, and the group reports `PITRArchiveReady=False/ArchiveStoreInitFailed`; once RustFS is back the same pods create the archive and archive a sealed binlog as BRV1, issue #214 item 5)

Scenarios `32`–`36`, `38` and `39` (seven scenarios) are **full-profile-only** by allowlist omission — none are in the `smoke` or `release` subsets — until they accumulate broader repeated live-pass history with no destructive leakage. Scenario `37` is not among them: #206 replaced it with `37-pitr-planned-failback-no-reclone`, which is in the `release` profile and has no live-pass history yet. As of 2026-07-11 the seven full-only scenarios and the retired original scenario 37 (`37-pitr-archive-handoff-across-failover`) had passed on the k3d playground; scenarios `34`, `35`, `36`, and `38` also passed post-review reruns with the final code. Those passes belong to the retired scenario 37, not to its replacement. Scenario `36` needed three diagnosed-and-fixed failed attempts first (see §36's "Actual live result"). The seven remain full-profile-only while this new, high-risk coverage matures. Scenarios `36`, `37`, `68`, `69` and `70` additionally run `ResetBeforeRunAll` because they exercise backup/PITR/restore-in-place state that can leak into later scenarios if cleanup is interrupted.

`make chaos-list` is the authoritative inventory — when adding a scenario, also append it here so the doc and the registry stay in lock-step.

Sections marked `§S` (planned-switchover) and `§SBR` (split-brain auto-resolve) are documented as appendix-style sections below — they exist as scenarios but don't fit the §1-§39 numbered failure-mode grid. The Dragonfly rolling image update, formerly numbered §32, is now the unnumbered §D6b appendix item so §32 could open the new emergency/denial band (§32-§39).

The runner refuses to mutate any kubectl context that does not match the same allowlist as `playground/_guard.sh` (`k3d-*`, `kind-*`, `minikube*`, or names listed in `BLOODRAVEN_PLAYGROUND_CONTEXTS`). Markdown is the source of truth for hypotheses and prose; the runner's assertions are the operational ones documented under each scenario's "Verify" section.

## Prerequisites

1. Create k3d cluster: `k3d cluster create bloodraven --agents 3 --k3s-arg '--tls-san=<hostname>@server:0'`
2. Run playground setup: `./playground/setup.sh`
3. Verify healthy state: `kubectl -n bloodraven-playground get mysqlfailovergroups -o wide` shows one writable and two read-only followers

Key playground config: `pollInterval=2s`, `failureThreshold=3` (~6s detection), `failoverCooldown=30s`, sidecar `leaseTimeout=20s`, `peerCheckInterval=5s`.

## Tooling Notes

- **`kill-site` vs `scale --replicas=0`**: `kill-site` deletes the pod but Deployment recreates it in <5s. Use `scale --replicas=0` when you need a site to stay down (self-fencing, anti-flap, sustained outage tests).
- **Network partition**: `chaos.sh network-partition` uses a NetworkPolicy deny-all. This blocks at the pod network level (not host netns). Cleanup: `chaos.sh recover`.
- **Relay log drain**: Failover takes up to ~37s when the primary is dead (~6s detection + a drain of at most 30s + the promotion's binary log rotation, bounded at 1s and usually a few milliseconds; the promotion statements add milliseconds). The drain ends as soon as the replica has applied what it already received, so without a backlog it is much shorter. Plan wait times for the bound.
- **Operator binlog rotations**: The operator sends one best-effort `FLUSH BINARY LOGS` (bounded at 1 s, never failing its caller) to a promotion candidate before its writes are enabled, to the fenced primary of a planned failover or maintenance handoff, and, with PITR enabled, to a reclone's recipient. So a scenario's own flushes are not the only thing that seals a binlog. With PITR enabled a read-only site's sidecar archives the tail those rotations seal (`/archiver/status` `tail.state`), and a reclone starts only once its recipient's tail is reported archived, or after 2 minutes with a Warning Event `PITRArchiveTailNotArchived`: budget a reclone wait for that.
- **A clone changes `server_uuid`**: Every mysql container starts through the operator's launcher, which removes `auto.cnf` on the restart that installs a finished `CLONE INSTANCE` (a reclone, the fresh-deploy bootstrap, the auto-clone of an empty site, the post-restore reclone queue), so the recipient comes back with a new `@@server_uuid` and numbers its own transactions from `<new uuid>:1` (#265). Read a site's `server_uuid` after a clone, not before, and do not compare GTID sets by a UUID remembered from before one. An ordinary restart, a pod replacement and a clone that failed or was killed keep the `server_uuid`. The launcher logs one line to the mysql container when it acts (`bloodraven-launcher: …`), and the operator logs `clone recipient server_uuid rotated`, or raises a Warning Event `CloneServerUUIDNotRotated` when the recipient kept its identity. `pitrRecloneSite` and scenarios 51 and 59 assert the change.
- **Replication user**: Survives pod restarts but not PVC wipes. After `reset-mysql.sh` the init-users script recreates it on MySQL first boot.
- **Verify replication between scenarios**: After any failover, always verify replication is actually working (`SELECT SERVICE_STATE FROM performance_schema.replication_connection_status` should show `ON`) before proceeding. GTID divergence from the respawn race (see scenario 1 note) can silently break replication, causing false results in subsequent scenarios.
- **db-readonly taint blocks PVC provisioning**: The operator applies `bloodraven-playground.db-readonly.bloodraven.shipstream.io/playground:NoExecute` to non-writable nodes. The local-path-provisioner's helper pod does not tolerate this taint and gets evicted, blocking PVC creation. `reset-mysql.sh` now scales the operator to 0 before stripping taints (single-shot, no race), so a manual taint clear after a reset is no longer needed. If you wipe state by hand, mirror the same order: scale operator → 0, scale MySQL → 0, delete PVCs, strip taints, scale MySQL → 1, clear stale `status.lastFailover`/`plannedFailover`, scale operator → 1.
- **`reset-mysql.sh` dumps on timeout**: If the post-reset wait loop times out without both pods Ready, the script writes pods/events/PVC+PV/node-taint/per-container-log forensics under `playground/chaos-results/reset-<timestamp>/` and exits non-zero — no more "two commands you should have remembered" footer.
- **Distroless containers**: The sidecar and operator images use distroless base images with no shell or userspace tools. Use `kubectl debug --target=<container> --image=busybox` to get a shell with tools like `kill`, `ps`, etc.
- **JSON patch vs merge patch**: When patching the MysqlFailoverGroup CR, always use `--type json` (JSON Patch). Merge patches on the `spec.sites` array drop required fields (loadBalancerIP, storage, zone) and fail validation.

---

## Scenarios 1-10: Core Failure Modes

### 1. Clean Primary Failure
**Category**: Basic failover | **Risk**: Low | **Automated**: `01-clean-primary-kill` (smoke and release profiles)

**Hypothesis**: Kill primary pod -> operator detects in ~6s -> promotes replica -> old primary rejoins as replica after respawn. Under a write load the new primary takes its first write inside the failover budget, and the promotion's `FLUSH BINARY LOGS` costs a small, bounded part of it.

**Injection**: `./playground/chaos.sh kill-site iad`

**Verify**: activeSite flips, new primary read_only=0, DNS updated, old primary auto-recovers as replica (IO=Yes, SQL=Yes). Counter data preserved.

**Timing**: up to ~37s to failover complete (~6s detection; the relay drain waits at most 30s for received backlog to apply, and the promotion's binary log rotation at most 1s). Recovery adds ~5s after old primary pod respawns.

**Caution — GTID divergence race**: When the old primary pod respawns, MySQL commits 1-3 internal transactions (system table updates, auto-increment counters) before the operator can fence it. This may create GTID divergence that blocks auto-recovery, requiring `reset-mysql.sh`. Whether divergence occurs is timing-dependent — it doesn't happen every time. If recovery logs "GTID divergence detected" instead of "no GTID divergence, auto-recovering", the scenario was affected.

**Automated run: failover under write load, RTO measured (#229)**. The promotion sends one best-effort `FLUSH BINARY LOGS` to the candidate before its writes are enabled (bounded at 1 s, `controller.BinlogRotationTimeout`). The automated scenario measures what that costs on a live failover:

1. Before the fault it starts a write load on the primary (one single-row transaction every 50 ms into `chaos_s01.load_acked`, over its own connection) and a write probe on the failover target. The probe reads the target's `read_only`/`super_read_only` every 100 ms over a connection opened before the fault and commits one row into `chaos_s01.rto_probe` as soon as both are off; it never sends a write to a read-only server (its account is root, which `read_only` alone does not stop). The load runs for 3 s before the fault and must have acknowledged writes.
2. It scales the primary to 0 (the fault's instant is recorded just before the call) and runs the existing assertions unchanged: `activeSite` flips within 90 s, `bloodraven_failovers_total` increments, `failover complete` is logged.
3. It then reports, as a `measured:` line (CLI output and JUnit `<system-out>`) and as the runner's `failover under load measured` log line:
   - **rto**: fault to the first write the new primary acknowledged. This is the write outage at the new primary's SQL port; DNS and Service switching are not in it (the probe holds a direct connection to the pod).
   - **write outage**: the last write the old primary acknowledged to the first on the new primary.
   - **promotion FLUSH BINARY LOGS**: `durationMs` of the operator's INFO line `promotion binlog rotated` for the promoted site, read from the operator's log (JSON, or slog text), and its share of the RTO.
   - a second line with how many acknowledged writes are missing on the new primary. Replication is asynchronous, so this is reported, not asserted.
4. Assertions: the first write came within the 90 s the scenario already gave the failover, and the operator logged the rotation for the promoted site (the INFO line, or WARN `promotion binlog rotation failed`). The rotation's outcome is not asserted. It is best effort in the operator and a loaded cluster can be slow: a WARN line, or a `durationMs` at or over the 1 s bound, is reported as a `measured: WARNING:` line (with the operator's line) and in the runner's log (`promotionFlushAcknowledged`), and the run passes or fails on its failover.
5. When the rotation was acknowledged, the scenario checks the property it exists for on the real server: reading `SHOW BINARY LOGS` and each file's `Previous_gtids` from the newest back, one binary log file of the new primary starts at exactly `status.lastFailover.promotionGtidExecuted`. The result is a `measured:` line (`promotion binlog file on <site>: ...`). A proven mismatch (a file that starts before the recorded set followed by one that starts past it) is reported as `WARNING: promotion binlog file on <site>: MISMATCH: ...` and does not fail the run yet: the emergency promotion assumes, and does not prove, that nothing commits on the candidate between its GTID read and its rotation, and the check has not run on a cluster. A check that cannot be made (an empty record, a failed read, a purged file) is reported as `not checked`.

Cleanup stops the load and the probe, waits for the old primary to rejoin, reclones it if the load left it diverged (a primary killed while it takes writes can hold transactions its replica never received), and drops `chaos_s01`. A run that failed before the load started cleans up as before. Timeout 4 minutes (3 before the load was added; the existing waits keep their budgets), cleanup budget 6 minutes.

---

### 2. Operator Kill and Restart
**Category**: Operator resilience | **Risk**: Low

**Hypothesis**: Kill operator while healthy -> restarts within 20s leaseTimeout -> no spurious failover, no self-fencing.

**Injection**: `./playground/chaos.sh kill-operator`

**Verify**: activeSite unchanged, every MySQL site unchanged, zero "SELF-FENC" in sidecar logs, operator logs show clean startup.

---

### 3. Network Partition of Primary
**Category**: Network partition | **Risk**: Medium

**Hypothesis**: NetworkPolicy blocks all traffic to primary pod -> operator can't reach it -> promotes replica -> primary sidecar self-fences at ~T+20s (both operator and peer unreachable beyond leaseTimeout).

**Injection**: `./playground/chaos.sh network-partition <active-site>`

**Verify**: Replica promoted. Primary sidecar logs "SELF-FENCING" + "SELF-FENCED", primary super_read_only=1 (verified via `kubectl exec` which bypasses NetworkPolicy). Cleanup: `./playground/chaos.sh recover`.

**Note**: `kubectl exec` uses the API server, not the pod network, so you can still query MySQL on the partitioned pod.

**Note**: After partition removal, recovery may log transient errors ("invalid connection", "connection refused") as the old primary's MySQL connections re-establish. These retry and succeed — don't be alarmed by 1-2 failed recovery attempts in the logs.

---

### 4. Data Integrity Under Failover
**Category**: Data integrity | **Risk**: Low

**Hypothesis**: Write known counter value -> kill primary -> no committed writes lost on new primary.

**Injection**:
```bash
# Write test data
kubectl -n bloodraven-playground exec deploy/mysql-playground-<primary> -c mysql -- \
  mysql -uroot -pplayground-root-pw -e "CREATE DATABASE IF NOT EXISTS chaos_test; ..."
# Record value, kill primary, check value on new primary
```

**Verify**: Post-failover counter >= pre-failover counter. Compare GTID sets to quantify async replication gap (typically 0 transactions lost when replica is caught up).

**Critical**: Before running this scenario, verify replication is actually running on the replica (`SELECT SERVICE_STATE FROM performance_schema.replication_connection_status` → `ON`). If a previous scenario caused GTID divergence and recovery was blocked, the replica won't be replicating and writes on the primary won't propagate — giving a false data-loss result.

---

### 5. Operator Kill During Active Failover
**Category**: Operator resilience (advanced) | **Risk**: Medium

**Hypothesis**: Kill primary + kill operator 1s later (mid-failover). Restarted operator converges to exactly one writable site.

**Injection**: `./playground/chaos.sh kill-site iad && sleep 1 && ./playground/chaos.sh kill-operator`

**Verify**: After ~45s convergence, exactly one site has read_only=0 and the group is `Ready=True`. No sidecar may emit the "Bloodraven and every peer unreachable beyond lease timeout" self-fence during the short operator restart. A topology-mismatch self-fence is valid in this scenario: if the operator dies after changing MySQL writability but before publishing the matching active-site view, immediate fencing is the intended safety response. No split-brain.

**Observation**: In practice, the primary pod often respawns before the new operator finishes starting. The new operator discovers the original topology already restored (iad=writable, pdx=read-only) and resumes normal monitoring without triggering a failover. Replication may be in CONNECTING state briefly while the IO thread reconnects to the restarted primary's new pod IP via the Service DNS.

---

### 6. Self-Fencing Validation
**Category**: Split-brain prevention | **Risk**: Medium

**Hypothesis**: Fully isolate primary (scale operator=0, scale every non-self peer including the reader=0). Sidecar self-fences at ~T+20s.

**Injection**:
```bash
NS=bloodraven-playground
kubectl -n $NS scale deployment bloodraven --replicas=0
kubectl -n $NS scale deployment mysql-playground-pdx --replicas=0
# Wait 25s
```

**Verify**: Primary super_read_only=1, sidecar logs "SELF-FENCED", writes fail. Cleanup: scale both back to 1.

**Important**: Must use `scale --replicas=0`, not `kill-site`. Deployment recreates killed pods in <5s, which refreshes the sidecar's peer timer and prevents self-fencing.

**After cleanup**: When the operator and replica come back, the operator will typically promote the OTHER site (not the self-fenced one) as primary. Expect the active site to flip. The self-fenced site becomes a replica.

---

### 7. Recovery/Rejoin After Failover (No Divergence)
**Category**: Recovery | **Risk**: Low

**Hypothesis**: After failover, old primary returns read-only with no replication. Operator compares GTIDs, finds no divergence, auto-recovers as replica.

**Injection**: Run scenario 1, then verify recovery happened. To re-test: `STOP REPLICA; RESET REPLICA ALL;` on old primary to re-trigger recovery.

**Verify**: Old primary shows Replica_IO_Running=Yes, Source_Host=<new-primary>. Data replicates. No RecoveryBlocked condition.

**Note**: This also works across operator restarts now that `lastFailover.target` is persisted in CR status.

---

### 8. GTID Divergence Detection
**Category**: Data integrity / split-brain aftermath | **Risk**: Medium

**Hypothesis**: Inject rogue writes on old primary -> operator detects divergence -> blocks recovery.

**Injection**: With new primary active and old primary not replicating:
```bash
kubectl exec ... -e "SET GLOBAL read_only=OFF; INSERT INTO divergence_test.rogue VALUES (1); SET GLOBAL read_only=ON;"
```

**Verify**: Status shows RecoveryPending with reason DivergentTransactions, operator logs "GTID divergence", exact divergent GTID set reported. Cleanup: run scenario 19 (Reclone Safety Interlock) to exercise the reclone path against the divergent state this scenario leaves behind, or skip straight to `./playground/reset-mysql.sh`.

**Important**: `reset-mysql.sh` is **required** after this scenario (or after scenario 19 if you chain) — divergent GTID state cannot be auto-recovered without a reclone. Clear db-readonly taints on both nodes after reset if PVCs fail to provision.

---

### 9. Rapid Successive Failures (Anti-Flap)
**Category**: Flap detection | **Risk**: High

Automated as `09-anti-flap-cooldown`.

**Hypothesis**: After one emergency failover, a second primary loss inside `failoverCooldown` is not promoted while the cooldown runs, and is promoted once it expires, with no new state transition to prompt it.

**Injection**: `make chaos-run SCENARIO=09-anti-flap-cooldown`. The scenario raises `spec.failoverCooldown` to 90s (restored in cleanup) and waits for the topology manager restart that the config change causes. It scales the primary to 0, waits for the first failover, scales the old primary back up, and waits until it is a caught-up replica. Then it scales the new primary to 0 and holds it there.

**Verify**:
- The operator logs `failover blocked by anti-flap cooldown` with `retryAt` = `lastFailover.time + failoverCooldown`.
- `status.lastFailover.time` does not move before the cooldown expires.
- A second failover back to the original primary completes within 45s of expiry, and `bloodraven_failovers_total` rises by exactly 2 across sites.

**History**: Until 2026-10-15 a failover refused by the cooldown was never retried. Cross-site actions ran only on state transitions, and a dead primary is a stable state, so the group stayed without a primary until the dead site came back (4m40s past expiry in the reproduction). The old version of this scenario asserted "no second failover within 90s" against a 30s cooldown, which was asserting that bug.

**Pitfalls the scenario works around**:
- A pod delete is not a reliable first fault. The pod respawns in ~8s, and while its stale endpoint black-holes probes the operator can collect fewer than `failureThreshold` failures, so no failover happens.
- A cooldown change restarts the topology manager. A fault injected before the new manager has observed every site is judged without history.
- The second fault loses whatever the rejoined site had not yet received (asynchronous RPO). The scenario waits for it to catch up, and cleanup reclones any site left diverged.

---

### 10. Full Bootstrap After Data Wipe (CLONE INSTANCE)
**Category**: Bootstrap / disaster recovery | **Risk**: High

**Hypothesis**: Wipe replica PVC -> operator detects fresh-deploy state -> CLONE INSTANCE -> replication setup.

**Injection**: `./playground/reset-mysql.sh` (wipes both sites and bootstraps from scratch)

**Verify**: Bootstrap phases progress (Cloning -> WaitingForRestart -> SetupReplication, `Bootstrapping=True`), then `Bootstrapping` returns to `False`/`NotBootstrapping`. Replica ends up read-only with replication running, data matches primary. GTID sets aligned. Typically takes 30-60s.

---

## Scenarios 11-15: Advanced Failure Modes

### 11. Simultaneous Both-Site Kill
**Category**: Total loss / graceful degradation | **Risk**: High

**Hypothesis**: Every MySQL deployment scaled to 0 -> TOTAL LOSS alert fires, no panic -> all come back -> operator re-establishes topology without manual intervention.

**Injection**:
```bash
NS=bloodraven-playground
kubectl -n $NS scale deployment mysql-playground-iad mysql-playground-pdx mysql-playground-reader --replicas=0
# Wait 15s for TOTAL LOSS alert
kubectl -n $NS scale deployment mysql-playground-iad mysql-playground-pdx mysql-playground-reader --replicas=1
```

**Verify**: Operator logs a total-loss alert during the outage. After recovery, one site is writable and two followers are read-only with replication running. No operator crash or permanent degraded state.

---

### 12. Rolling Update During Healthy State
**Category**: Zero-downtime operations | **Risk**: Medium

**Hypothesis**: Spec change (e.g. resource limits) triggers ordered update: standby rolls first, waits for replication health, then primary rolls with failover for zero write downtime.

**Injection**:
```bash
kubectl -n bloodraven-playground patch mysqlfailovergroup playground --type json \
  -p '[{"op":"replace","path":"/spec/sites/0/resources/requests/memory","value":"300Mi"},{"op":"replace","path":"/spec/sites/1/resources/requests/memory","value":"300Mi"}]'
```

**Verify**: Both pods end up with new resource spec. Replication running. One site writable, one read-only.

**Current behavior**: The `UpdateController` (`internal/controller/updater.go`) performs an ordered replica-first → failover → old-primary roll. Healthy case produces a brief writer flip but no TOTAL LOSS window.

**Issue #46 safety net (April 2026)**: If the standby is not actually replicating (`super_read_only=ON` but threads stopped / no source configured), the updater now **refuses to start** via an `isHealthyReplica()` debounce in the topology poll. If the new standby pod comes up writable with no source mid-update, `waitForReplicaReady` **fails fast** after ~30s instead of holding `isUpdating()=true` for the full 5-minute deadline; cross-site split-brain recovery then takes over. Expect either (a) a clean ordered update, or (b) an aborted update followed by reclone/fence on the next reconcile — not a 5-minute split-brain hang.

**Cleanup**: Revert the resource change after testing:
```bash
kubectl -n bloodraven-playground patch mysqlfailovergroup playground --type json \
  -p '[{"op":"replace","path":"/spec/sites/0/resources/requests/memory","value":"256Mi"},{"op":"replace","path":"/spec/sites/1/resources/requests/memory","value":"256Mi"}]'
```

---

### 13. Operator Kill During Bootstrap
**Category**: Bootstrap resilience | **Risk**: High

**Hypothesis**: Kill operator while CLONE INSTANCE is running -> new operator detects partial state -> either resumes waiting for clone completion or retries cleanly.

**Injection**:
```bash
# Trigger a fresh bootstrap (reset-mysql.sh or delete replica PVC)
# Watch for "cloning from primary" in operator logs
# Kill operator immediately: ./playground/chaos.sh kill-operator
```

**Verify**: New operator either waits for the in-progress clone to finish (MySQL tracks clone state in `performance_schema.clone_status`) or starts a fresh clone. End state: healthy primary/replica pair. No stuck bootstrap phase.

**Caution**: Before deleting the replica PVC, clear the `bloodraven-playground.db-readonly.bloodraven.shipstream.io/playground` taint from the replica's node. Otherwise the local-path-provisioner's helper pod gets evicted by the NoExecute taint and the replacement PVC never provisions, leaving the replica pod stuck in Pending.

---

### 14. Failover With Replication Lag (Async Data Loss Window)
**Category**: Data loss quantification | **Risk**: Medium

**Hypothesis**: With artificial replication lag, killing the primary loses exactly the lagged transactions. This quantifies the real-world data loss window of async replication.

**Injection**:
```bash
NS=bloodraven-playground
PRIMARY=<active-site>  # iad or pdx
REPLICA=<other-site>

# Stop the SQL applier thread on the replica (IO thread keeps fetching)
kubectl -n $NS exec deploy/mysql-playground-$REPLICA -c mysql -- \
  mysql -uroot -pplayground-root-pw -e "STOP REPLICA SQL_THREAD;"

# Write 20 transactions on primary
for i in $(seq 1 20); do
  kubectl -n $NS exec deploy/mysql-playground-$PRIMARY -c mysql -- \
    mysql -uroot -pplayground-root-pw -e \
    "INSERT INTO chaos_test.lag_test VALUES ($i, NOW())"
done

# Verify lag: replica has fetched relay logs but not applied them
kubectl -n $NS exec deploy/mysql-playground-$REPLICA -c mysql -- \
  mysql -uroot -pplayground-root-pw -N -e \
  "SELECT COUNT(*) FROM chaos_test.lag_test"
# Expected: 0 (SQL thread stopped)

# Kill primary
./playground/chaos.sh kill-site $PRIMARY
# Wait for failover (up to ~37s)

# Check how many transactions survived on the new primary
kubectl -n $NS exec deploy/mysql-playground-$REPLICA -c mysql -- \
  mysql -uroot -pplayground-root-pw -N -e \
  "SELECT COUNT(*) FROM chaos_test.lag_test"
```

**Verify**: After failover, the relay log drain phase (30s timeout) attempts to apply pending relay logs before promotion. If drain succeeds: all 20 rows survive. If drain times out (old primary unreachable, relay logs incomplete): some rows lost. Compare GTID sets to quantify exact loss.

**Testing challenge**: `kill-site` causes the primary pod to respawn in <5s. The operator detects the respawned pod before completing the failover, and the system may restore the original topology via split-brain recovery instead of completing the failover to the replica. This means the 20 rows survive via normal replication from the restarted primary, not via relay log drain. To force the primary to stay dead, try: scale operator to 0, scale primary to 0, then scale operator back to 1. The reconciler will eventually scale the primary back to 1, but the failover should complete before that.

**Alternative — SOURCE_DELAY**: Instead of stopping the SQL thread, use `CHANGE REPLICATION SOURCE TO SOURCE_DELAY=10` to add a fixed 10-second lag. Then write transactions over 5 seconds and kill the primary. The last ~10s of writes should be in the relay log but unapplied. Relay log drain should recover them if the relay logs are intact (they are — only the primary is dead, not the replica).

```bash
# Set 10-second replication delay
kubectl -n $NS exec deploy/mysql-playground-$REPLICA -c mysql -- \
  mysql -uroot -pplayground-root-pw -e \
  "STOP REPLICA; CHANGE REPLICATION SOURCE TO SOURCE_DELAY=10; START REPLICA;"

# Write transactions for 5 seconds, then kill primary
# The relay log has all transactions but SQL thread is 10s behind
# Relay log drain should apply them all (up to 30s timeout)
```

---

### 15. Sidecar Crash (Container Kill, Not Pod Kill)
**Category**: Degraded pod handling | **Risk**: Low

**Hypothesis**: Killing just the sidecar container (not the pod) leaves MySQL running but health checks stop. Kubernetes restarts the sidecar container. Operator should see brief unreachability on the sidecar HTTP endpoint but MySQL remains connectable on port 3306.

**Injection**:
```bash
NS=bloodraven-playground
SITE=<active-site>
POD=$(kubectl -n $NS get pods -l bloodraven.shipstream.io/site=$SITE -o name)

# Kill PID 1 in the sidecar container (triggers container restart, not pod restart)
# Distroless container has no kill binary — use kubectl debug:
kubectl -n $NS debug $POD --target=sidecar --image=busybox -- kill 1
```

**Verify**: MySQL stays up (port 3306 connectable throughout). Sidecar container restarts (check `RESTARTS` count). Operator may log brief health check failures but does NOT trigger failover (MySQL itself is still reachable). Sidecar re-initializes fencing timers on restart (grace period).

**Note**: `kubectl exec $POD -c sidecar -- kill 1` does NOT work on distroless images (no `kill` binary). Use `kubectl debug --target=sidecar --image=busybox` to get an ephemeral container that shares the sidecar's PID namespace.

---

## Scenarios 16-18: Additional Failure Modes

### 16. MySQL Process Kill (Not Pod Kill)
**Category**: Process-level failure | **Risk**: Medium

**Hypothesis**: Killing the mysqld process (via `mysqladmin shutdown`) causes the container to exit and restart. If MySQL recovers within failureThreshold (~6s), no failover. If slower, failover triggers correctly as fail-fast behavior.

**Injection**:
```bash
NS=bloodraven-playground
PRIMARY=$(kubectl -n $NS get mysqlfailovergroups playground -o jsonpath='{.status.activeSite}')
POD=$(kubectl -n $NS get pods -l bloodraven.shipstream.io/site=$PRIMARY -o jsonpath='{.items[0].metadata.name}')

# Record restart count
kubectl -n $NS get pod/$POD -o jsonpath='{.status.containerStatuses[?(@.name=="mysql")].restartCount}'

# Kill mysqld (container will restart, pod stays)
kubectl -n $NS exec pod/$POD -c mysql -- mysqladmin -uroot -pplayground-root-pw shutdown
```

**Verify**: MySQL container restarts (RESTARTS count increments). Check whether failover triggered — with 2s poll interval and failureThreshold=3, even a 2-4 second outage may trigger failover. Either outcome is acceptable: no failover (MySQL recovered in time) or failover (correct fail-fast). Document which occurs and the timing.

**Note**: `kill -9 1` via `kubectl debug --target=mysql` does NOT work — PID 1 (init process) cannot be killed from within its PID namespace. Use `mysqladmin shutdown` instead.

---

### 17. Network Partition of Replica (Not Primary)
**Category**: Asymmetric failure — replica isolation | **Risk**: Medium

**Hypothesis**: Partitioning the replica causes the operator to log an alert but NOT trigger failover (primary is healthy). The replica sidecar does NOT self-fence (already read-only — `evaluate()` skips read-only nodes). After partition removal, replication resumes and data written during the partition catches up.

**Injection**:
```bash
NS=bloodraven-playground
PRIMARY=$(kubectl -n $NS get mysqlfailovergroups playground -o jsonpath='{.status.activeSite}')
REPLICA=$([ "$PRIMARY" = "iad" ] && echo "pdx" || echo "iad")

# Write test data before partition
kubectl -n $NS exec deploy/mysql-playground-$PRIMARY -c mysql -- \
  mysql -uroot -pplayground-root-pw -e "UPDATE chaos_test.counter SET val = 200 WHERE id = 1;"

# Partition the replica
./playground/chaos.sh network-partition $REPLICA

# Wait 15s for detection, then write more data during partition
kubectl -n $NS exec deploy/mysql-playground-$PRIMARY -c mysql -- \
  mysql -uroot -pplayground-root-pw -e "UPDATE chaos_test.counter SET val = 300 WHERE id = 1;"

# Remove partition
./playground/chaos.sh recover
```

**Verify**: Active site unchanged (no failover). Replica sidecar logs show zero "SELF-FENC" entries. After partition removal, replica catches up — val=300 visible on replica. MySQL IO thread auto-reconnects via Service DNS.

---

### 18. Rapid CR Spec Changes During Active Failover
**Category**: Reconciler contention / state machine stress | **Risk**: High

**Hypothesis**: Concurrent CR spec patches during an active failover should not break convergence. The reconciler may log conflict errors ("Operation cannot be fulfilled on deployments.apps") but retries succeed. The system converges to exactly one writable site.

**Injection**:
```bash
NS=bloodraven-playground
PRIMARY=$(kubectl -n $NS get mysqlfailovergroups playground -o jsonpath='{.status.activeSite}')

# Kill primary to start failover
./playground/chaos.sh kill-site $PRIMARY

# Immediately hammer the CR with 5 spec changes
for i in $(seq 1 5); do
  sleep 2
  MEM=$((256 + i * 10))
  kubectl -n $NS patch mysqlfailovergroup playground --type json \
    -p "[{\"op\":\"replace\",\"path\":\"/spec/sites/0/resources/requests/memory\",\"value\":\"${MEM}Mi\"},{\"op\":\"replace\",\"path\":\"/spec/sites/1/resources/requests/memory\",\"value\":\"${MEM}Mi\"}]"
done
```

**Verify**: After ~60s, exactly one site has read_only=0. Check operator logs for "cannot be fulfilled" conflict errors (expected, should be retried). No stuck state or split-brain. Revert resource patch after test.

**Note**: Use JSON patch (`--type json`) instead of merge patch — merge patch drops required fields from the sites array.

---

### 19. Reclone Safety Interlock
**Category**: Admin-action safety / divergent-GTID confirmation | **Risk**: Medium

**Hypothesis**: The `bloodraven.shipstream.io/reclone-site` annotation rejects a value without a `gtidPrefix`, and a mismatched GTID prefix, when the target site has a non-empty `status.sites[].divergentGtid`, preventing a fat-fingered `reclone-site=<wrong-site>` from destroying the wrong replica. Both forms carry the group's name and uid (`<site>:confirm=<group>:uid=<uid>`, plus `:gtidPrefix=<prefix>` on a diverged site); a cold reclone (no divergence recorded) is accepted in the form without `gtidPrefix`.

**Prerequisite**: Scenario 8 (GTID Divergence Detection) — it creates a site with `divergentGtid` and `recoveryState=RecoveryBlocked` that the interlock gates can be exercised against. Do NOT run `reset-mysql.sh` until this scenario completes; the interlock needs the divergent state scenario 8 leaves behind.

**Injection (four sub-cases)**:

```bash
NS=bloodraven-playground
FG=playground

# Identify the divergent site from scenario 8's aftermath.
SITE=$(kubectl -n $NS get mfg $FG -o jsonpath='{range .status.sites[?(@.recoveryState=="RecoveryBlocked")]}{.name}{end}')
DG=$(kubectl -n $NS get mfg $FG -o jsonpath="{.status.sites[?(@.name==\"$SITE\")].divergentGtid}")
GROUP_UID=$(kubectl -n $NS get mfg $FG -o jsonpath='{.metadata.uid}')
echo "divergent site=$SITE gtid=$DG uid=$GROUP_UID"

# --- A) Rejected: a bare site, then the confirmed cold form against a divergent site ---
kubectl -n $NS annotate --overwrite mfg $FG bloodraven.shipstream.io/reclone-site=$SITE
sleep 5
kubectl -n $NS get events --field-selector involvedObject.name=$FG --sort-by=.lastTimestamp | tail -3
kubectl -n $NS annotate --overwrite mfg $FG bloodraven.shipstream.io/reclone-site=$SITE:confirm=$FG:uid=$GROUP_UID
sleep 5
kubectl -n $NS get events --field-selector involvedObject.name=$FG --sort-by=.lastTimestamp | tail -5
kubectl -n $NS get mfg $FG -o jsonpath='{.metadata.annotations.bloodraven\.shipstream\.io/reclone-site}'; echo

# --- B) Rejected: mismatched GTID prefix ---
kubectl -n $NS annotate --overwrite mfg $FG bloodraven.shipstream.io/reclone-site=$SITE:confirm=$FG:uid=$GROUP_UID:gtidPrefix=deadbeef
sleep 5
kubectl -n $NS get events --field-selector involvedObject.name=$FG --sort-by=.lastTimestamp | tail -5

# --- C) Accepted: matching 8-char prefix of divergentGtid's source UUID ---
PREFIX=$(echo -n "$DG" | cut -c1-8)
kubectl -n $NS annotate --overwrite mfg $FG bloodraven.shipstream.io/reclone-site=$SITE:confirm=$FG:uid=$GROUP_UID:gtidPrefix=$PREFIX
# Watch for RecloneRequested and the Bootstrapping condition entering Cloning.
kubectl -n $NS get events --field-selector involvedObject.name=$FG --sort-by=.lastTimestamp -w &
EVENT_WATCH_PID=$!
sleep 90
kill $EVENT_WATCH_PID 2>/dev/null

# --- D) Cold reclone still works (no divergence) ---
# After sub-case C completes the site has rejoined as replica and no longer
# has divergentGtid in status. The cold form (no gtidPrefix) is accepted.
kubectl -n $NS annotate --overwrite mfg $FG bloodraven.shipstream.io/reclone-site=$SITE:confirm=$FG:uid=$GROUP_UID
sleep 5
kubectl -n $NS get events --field-selector involvedObject.name=$FG --sort-by=.lastTimestamp | tail -3
```

**Verify**:

| Sub-case | Expected outcome |
|---|---|
| A. bare site, then cold form, divergent | The bare `<site>`: a `RecloneRejected` Warning event saying the `confirm=` pair is missing (the value must be confirmed); annotation removed. The cold form: a `RecloneRejected` Warning event saying the value must include `gtidPrefix=` and printing the full form (`…:confirm=<group>:uid=<uid>:gtidPrefix=<prefix>`); annotation removed from CR; no `Bootstrapping` condition transition; `divergentGtid` unchanged. |
| B. mismatched prefix | `RecloneRejected` Warning event with "does not match the observed divergentGtid" text; annotation removed; site still fenced; no clone executed. |
| C. matching prefix | `RecloneRequested` Normal event with site name; `Bootstrapping` condition cycles through `Cloning` → `WaitingForRestart` → `SetupReplication` (`True`) and returns to `False`/`NotBootstrapping`, with the recloned site replicating; `RecoveryPending` clears; site comes back as replica with `SERVICE_STATE=ON`. Wall-clock: 30–60s total. |
| D. cold form | `RecloneRequested` Normal event (no rejection); another full bootstrap cycle runs. This confirms the non-divergent case needs no `gtidPrefix`. |

**Timing**: Sub-cases A and B resolve within one sync cycle (~30s; the runner's `sync()` ticker). Sub-case C takes 30-60s for the clone + restart. Sub-case D is the same.

**Important — sync interval**: The runner re-reads CRs every 30s, so the annotation may sit briefly before being processed. If you see no event after 5s, wait up to 35s before concluding rejection isn't firing.

**Cleanup**: `./playground/reset-mysql.sh` is advisable after sub-case D to restore a clean matrix for subsequent scenarios. Clear the `bloodraven-playground.db-readonly.bloodraven.shipstream.io/playground` taint from both nodes after the reset.

---

### 20. Shared-Node Selector Isolation
**Category**: Placement / multi-tenant isolation | **Risk**: Low

**Hypothesis**: A node can advertise membership in multiple failover groups at the same site, and a failover for `playground` only applies `bloodraven-playground.db-readonly.bloodraven.shipstream.io/playground`. Workloads for an unrelated group that tolerate `playground`'s taint are not evicted.

**Injection**:
```bash
NS=bloodraven-playground
FG=playground
PRIMARY=$(kubectl -n $NS get mfg $FG -o jsonpath='{.status.activeSite}')
NODE=$(kubectl get nodes -l "bloodraven.shipstream.io/failover-group.playground=true,bloodraven.shipstream.io/site.playground=$PRIMARY" -o jsonpath='{.items[0].metadata.name}')

# Mark the same physical node as also serving an unrelated inventory group.
kubectl label node $NODE \
  bloodraven.shipstream.io/failover-group.inventory=true \
  bloodraven.shipstream.io/site.inventory=$PRIMARY \
  --overwrite

# Simulated inventory workload: it tolerates playground's taint because
# playground failovers should not evict inventory pods on shared nodes.
cat <<YAML | kubectl -n $NS apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: inventory-shared-node-canary
  labels:
    app: inventory-shared-node-canary
spec:
  nodeSelector:
    bloodraven.shipstream.io/failover-group.inventory: "true"
    bloodraven.shipstream.io/site.inventory: "$PRIMARY"
  tolerations:
    - key: bloodraven-playground.db-readonly.bloodraven.shipstream.io/playground
      operator: Exists
      effect: NoExecute
  containers:
    - name: sleep
      image: busybox:1.36
      command: ["sh", "-c", "sleep 3600"]
YAML

kubectl -n $NS wait --for=condition=Ready pod/inventory-shared-node-canary --timeout=60s
./playground/chaos.sh kill-site $PRIMARY
sleep 45
```

**Verify**:
```bash
# The old-primary node should have the playground taint only.
kubectl get node $NODE -o jsonpath='{.spec.taints[*].key}{"\n"}'
# Expected includes: bloodraven-playground.db-readonly.bloodraven.shipstream.io/playground
# Expected excludes: bloodraven-playground.db-readonly.bloodraven.shipstream.io/inventory

# The unrelated inventory pod should still be running on the shared node.
kubectl -n $NS get pod inventory-shared-node-canary -o wide
# Expected: STATUS=Running, NODE=$NODE
```

**Cleanup**:
```bash
kubectl -n $NS delete pod inventory-shared-node-canary --ignore-not-found
kubectl label node $NODE bloodraven.shipstream.io/failover-group.inventory- bloodraven.shipstream.io/site.inventory- 2>/dev/null || true
./playground/chaos.sh recover
```

---

### 21. NoExecute Eviction Semantics
**Category**: Placement / eviction contract | **Risk**: Low

**Hypothesis**: During failover, the old active site's selected nodes receive `bloodraven-playground.db-readonly.bloodraven.shipstream.io/playground=true:NoExecute`. Pods without that toleration are evicted; pods with the toleration remain.

**Injection**:
```bash
NS=bloodraven-playground
FG=playground
PRIMARY=$(kubectl -n $NS get mfg $FG -o jsonpath='{.status.activeSite}')

cat <<YAML | kubectl -n $NS apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: noexecute-evict-canary
  labels:
    app: noexecute-evict-canary
spec:
  nodeSelector:
    bloodraven.shipstream.io/failover-group.playground: "true"
    bloodraven.shipstream.io/site.playground: "$PRIMARY"
  containers:
    - name: sleep
      image: busybox:1.36
      command: ["sh", "-c", "sleep 3600"]
---
apiVersion: v1
kind: Pod
metadata:
  name: noexecute-tolerate-canary
  labels:
    app: noexecute-tolerate-canary
spec:
  nodeSelector:
    bloodraven.shipstream.io/failover-group.playground: "true"
    bloodraven.shipstream.io/site.playground: "$PRIMARY"
  tolerations:
    - key: bloodraven-playground.db-readonly.bloodraven.shipstream.io/playground
      operator: Exists
      effect: NoExecute
  containers:
    - name: sleep
      image: busybox:1.36
      command: ["sh", "-c", "sleep 3600"]
YAML

kubectl -n $NS wait --for=condition=Ready pod/noexecute-evict-canary --timeout=60s
kubectl -n $NS wait --for=condition=Ready pod/noexecute-tolerate-canary --timeout=60s
./playground/chaos.sh kill-site $PRIMARY
sleep 45
```

**Verify**:
```bash
kubectl -n $NS get pods noexecute-evict-canary noexecute-tolerate-canary -o wide
# Expected: noexecute-evict-canary is gone/Failed or has a deletion timestamp.
# Expected: noexecute-tolerate-canary remains Running on the old-primary node.

kubectl -n $NS get pod noexecute-evict-canary >/dev/null 2>&1 && \
  kubectl -n $NS get pod noexecute-evict-canary -o jsonpath='{.metadata.deletionTimestamp}{"\n"}' || \
  echo "noexecute-evict-canary removed as expected"

kubectl -n $NS get pod noexecute-tolerate-canary -o jsonpath='{.status.phase}{"\n"}'
# Expected: Running
```

**Cleanup**:
```bash
kubectl -n $NS delete pod noexecute-evict-canary noexecute-tolerate-canary --ignore-not-found --grace-period=0 --force
./playground/chaos.sh recover
```

---

## Scenarios 22-23: Status durability regressions (from WISHLIST)

These scenarios were extracted from `WISHLIST.md` items #36 and #38 — both flag specific contracts that the chaos runner was not previously asserting. They are registered with the same `playground-chaos` runner as 1–21 and follow the same Inject → Observe → Verify shape.

Fail-back behavior is intentionally GTID-freshest/current-state-driven. When an original primary returns, it is not treated specially as "the old primary that must rejoin as a replica"; it can become writable again only by winning the same promotion candidate selection used for any failover, subject to anti-flap cooldown and configured site-priority tie-breaking. Scenario assertions should therefore track the current writable site and current healthy replica, not fixed site identities.

### 22. Replication Status After Recovery
**Category**: CR-status enrichment regression | **Risk**: Low

**Hypothesis**: After a clean primary kill and old-primary auto-recovery, the read-only site's `status.sites[].replication` becomes `Healthy` or `Lagging` within 30s — without requiring an operator restart. During recovery, `status.sites[].recoveryState=RecoveryInProgress` may be visible until MySQL reports healthy replication. This is the contract that `internal/controller/planned_failover.go`'s `TargetUnhealthy` safety check depends on.

**Why this regression test exists**: WISHLIST #36 documented that the replication field (then the `replicating` bool) and `gtidExecuted` stopped being populated on a post-recovery read-only site even though the sidecar `/status` endpoint reported replication threads running. The bug silently broke any planned switchover after the first chaos event in the same operator lifecycle. This scenario now guards that status-enrichment contract.

**Injection**: `make chaos-run SCENARIO=22-replication-status-after-recovery`. The runner scales the active primary to 0, waits for failover, scales it back up, waits for re-convergence, then reads the CR.

**Verify**: Within 30s of `writable=1 read-only=N-1`, `mfg.Status.Sites[i].Replication.Running()` (`Healthy` or `Lagging`) for the recovered candidate follower. The scenario also probes the sidecar `/status` endpoint as a cross-check; if the sidecar reports replication running but the CR field is not `Healthy` or `Lagging`, that is exactly the bug WISHLIST #36 describes.

**Cleanup**: Standard runner cleanup. No manual steps required.

---

### 23. Failover State Durability
**Category**: CR-status persistence | **Risk**: Low

**Hypothesis**: After a clean failover, killing and restarting the operator pod must NOT clear `status.lastFailover.target`, `status.lastFailover.time`, or an in-flight old-primary `RecoveryInProgress` marker. The post-restart operator must rehydrate those fields from the CR; `activeSite` must remain at the post-failover primary, and old-primary recovery must either clear when replication is healthy or retry when it is not.

**Why this regression test exists**: WISHLIST #38 noted that failover history and old-primary-recovery dispatch keys were partly in-memory. The operator now persists and rehydrates failover timestamps plus `RecoveryInProgress`/`RecoveryBlocked`, so a restart during recovery keeps the CR-visible lifecycle and retries safely instead of waiting for another failover.

**Injection**: `make chaos-run SCENARIO=23-failover-state-durability`. The runner scales the primary to 0, waits for failover, snapshots `lastFailover.target` / `lastFailover.time` / `activeSite`, kills the operator pod, sleeps 30s for the new pod to come up and reconcile, then re-reads the CR.

**Verify**: Post-restart values match the pre-restart snapshot. `lastFailover.time` is allowed to advance by up to 90s within tolerance (a status-enrichment loop may rewrite the field with a slightly later timestamp); a regression to zero or a "fresh failover happened just now" jump is rejected.

**Cleanup**: Standard runner cleanup. The operator's auto-recovery brings the original primary back up as a replica.

---

## Scenarios 24-30: Dragonfly integration

These scenarios cover the Bloodraven-owned Dragonfly topology enabled by `spec.dragonfly`. They assume the playground MFG has Dragonfly enabled and that the baseline check reports `status.dragonfly.phase=Ready`, one master, linked replicas, and no unreachable sites.

Live k3d status from May 3, 2026: scenarios 22, 23, 24, 25, 26, 27, and 29 pass in isolation after runner cleanup was updated to wait for Dragonfly reconvergence. Scenario 29 provisions the RustFS bucket and temporary `spec.dragonfly.snapshot` config itself; the default playground still does not enable snapshots at baseline.

### 24. Planned Dragonfly Switchover
**Category**: Coordinated planned failover | **Risk**: Medium

**Hypothesis**: A planned failover promotes the target Dragonfly replica with `REPLTAKEOVER`, preserves the seeded session key, flips `status.dragonfly.activeSite`, and converges the active Service endpoints to the new master pod.

**Injection**: `make chaos-run SCENARIO=22-planned-dragonfly-switchover`

**Verify**: `plannedFailover.phase=Succeeded`, `plannedFailover.dragonfly.PromotionMethod=REPLTAKEOVER`, `SessionsPreserved=true`, the seeded key is readable on the target, active Service endpoints select only the target Dragonfly pod, and `bloodraven_dragonfly_promotions_total{result="success"}` increments.

---

### 25. Dragonfly Master Kill
**Category**: Dragonfly-only HA | **Risk**: Low

**Hypothesis**: Force-deleting the active Dragonfly pod promotes the surviving replica and leaves MySQL untouched.

**Injection**: `make chaos-run SCENARIO=23-dragonfly-master-kill`

**Verify**: `status.dragonfly.activeSite` flips to the peer, MySQL `status.activeSite` remains unchanged, the seeded key survives on the promoted replica, the respawned old master rejoins with `status.dragonfly.sites[].role=Replica` and `linkStatus=up`, and the Dragonfly promotion metric increments.

The kill closes the replication connection, so the replica holds the seeded key and reports its link down. That does not shorten the wait. The replica is promoted by whichever comes first: `spec.failureThreshold` consecutive missed polls of the master (4 to 6 s with the defaults when the dial is refused, 9 to 11 s when the deleted pod's endpoint swallows the dial), or the StatefulSet's replacement pod answering as a new, empty master, which the replica is not re-attached to (`dragonfly: active master restarted without the cache; promoting the replica that holds it`). Either way the promotion is a `REPLICAOF NO ONE` (`result="sessions_lost"`) and the seeded key survives. On the second path the replacement pod is deleted once more by the promotion before it rejoins as a replica. About 30 s after the promotion Dragonfly follows MySQL back to the original site with `REPLTAKEOVER`; the rejoin assertion is made before that. A master that is only partitioned (scenario 39) is promoted after `spec.failureThreshold` missed polls, about 9 to 11 s with the defaults.

Not run since the single failure threshold and the restarted-master handling replaced the promotion on the first missed poll; the assertions were read against the new behaviour and left as they are.

---

### 26. Emergency MySQL Failover With Dragonfly Down
**Category**: Emergency fallback | **Risk**: High

**Hypothesis**: Scaling every Dragonfly StatefulSet to 0 does not block emergency MySQL promotion.

**Injection**: `make chaos-run SCENARIO=24-emergency-mysql-dragonfly-down`

**Verify**: `status.dragonfly.phase` leaves `Ready`, the active MySQL Deployment is scaled to 0 (a deleted pod returns before the failure threshold), and MySQL `status.activeSite` flips to the peer within the normal emergency budget. Dragonfly recovery is handled by cleanup and global recovery, not by the MySQL critical path.

---

### 27. Operator Restart Mid-Dragonfly Failover
**Category**: Operator resilience | **Risk**: Medium

**Hypothesis**: If the operator restarts while planned failover is waiting for Dragonfly sync, the replacement operator resumes from CR status and converges without swapping targets or leaving split-brain Dragonfly state.

**Injection**: `make chaos-run SCENARIO=25-operator-restart-mid-dragonfly-failover`

The scenario temporarily scales the target Dragonfly StatefulSet to 0 and patches the sync budget to 45s so `WaitingForDragonflySync` is deterministic, kills the operator after observing that fresh phase, then restores the target Dragonfly pod so the replacement operator can complete the same planned failover.

**Verify**: Planned failover reaches `Succeeded` with the original target, MySQL and Dragonfly active sites both equal that target, and `status.dragonfly.phase` returns to `Ready` with exactly one master.

---

### 28. Planned Dragonfly Sync Timeout Proceeds
**Category**: Degraded planned failover | **Risk**: Medium

**Hypothesis**: With `maxSyncWait=1s`, target Dragonfly scaled to 0, and `onSyncTimeout=Proceed`, the planned failover does not claim session preservation but still completes MySQL promotion.

**Injection**: `make chaos-run SCENARIO=26-planned-dragonfly-sync-timeout-proceed`

**Verify**: Planned failover reaches `Succeeded`, MySQL `status.activeSite` flips to the target, `plannedFailover.dragonfly.SessionsPreserved=false`, the reason is `DragonflySyncTimeout` or `DragonflyPromotionFailed`, and `bloodraven_dragonfly_promotions_total{result="failed"}` increments. The settle step force-respawns the old Dragonfly source and waits up to 4 minutes for it to rejoin as a replica, because this timeout path can briefly lag status convergence after MySQL recovery.

---

### 29. Dragonfly Snapshot-Restore Upgrade (D6a)
**Category**: Planned maintenance | **Risk**: Medium

**Hypothesis**: With `spec.dragonfly.snapshot.dir` configured to an S3-compatible bucket and the Dragonfly pods running under a ServiceAccount that can read/write it, Bloodraven can perform a short-outage planned upgrade by saving a snapshot, replacing the active Dragonfly pod on the target image, waiting for restore, then replacing and reattaching replicas.

**Current status**: Live-passing in k3d as of May 3, 2026: `PASS 29-dragonfly-snapshot-upgrade`, duration 4m26.292s. The baseline playground manifest intentionally omits `spec.dragonfly.snapshot` so normal Dragonfly pods do not startup-fail when RustFS or credentials are unavailable. Scenario 29 provisions and validates its snapshot backend itself before requesting the upgrade.

**Injection**:

Set the strategy and the new image in one patch, so the Rolling render never sees the new image:

```bash
kubectl -n bloodraven-playground patch mysqlfailovergroup playground --type=json -p '[
  {"op": "add", "path": "/spec/dragonfly/upgradeStrategy", "value": "SnapshotRestore"},
  {"op": "replace", "path": "/spec/dragonfly/image", "value": "docker.dragonflydb.io/dragonflydb/dragonfly:<target>"}
]'
```

**Verify**: `status.dragonfly.upgrade.phase` walks `Pending → SavingSnapshot → UpdatingActive → WaitingForActiveRestore → ReattachingReplicas → Succeeded`; the active Service has no endpoints while restore is in progress; `SAVE` completes before the active pod is deleted; final Dragonfly pods run the target image; `spec.dragonfly.image` still reads the requested target (the operator never writes spec); `status.dragonfly.phase` returns to `Ready`; the seeded key is present after restore.

**Automation status**: Registered as `29-dragonfly-snapshot-upgrade`. The playground deploys RustFS during setup; the scenario creates the `dragonfly` bucket on demand, temporarily patches `spec.dragonfly.snapshot`, waits for Dragonfly pods to run with `--dir=s3://dragonfly/playground`, and then sets `upgradeStrategy: SnapshotRestore` and moves `spec.dragonfly.image` to the image the pods already run, by digest (no new pull). The reverter restores the strategy and the original image in one patch, which rolls back the ordinary way.

---

### 30. Backup Verification Against RustFS
**Category**: Backup/restore verification | **Risk**: Medium

**Hypothesis**: A one-off `MysqlBackup` written to the in-cluster RustFS bucket can be restored by a pinned `MysqlBackupVerification`, and the restored database contains deterministic marker rows from the full dump.

**Injection**: `make chaos-run SCENARIO=30-backup-verification-rustfs`

The scenario creates/ensures bucket `bloodraven-backup-e2e`, patches only `MysqlFailoverGroup.spec.backup` with profile `rustfs-e2e`, and isolates objects under `e2e/30-backup-verification-rustfs/<run-stamp>/`. It seeds `chaos_s30_backup.marker`, waits for replica GTID catch-up, creates one named `MysqlBackup`, then creates one `MysqlBackupVerification` with `spec.backupRef.name` pinned to that backup instead of relying on latest-success resolution.

**Verify**: The backup reaches `Succeeded` with `status.artifactStorage.type=S3`, `status.artifactStorage.location=<prefix>/<backup>`, and a Job name. The verification reaches `Succeeded`, reports `status.backupRef.name` for the created backup, runs the sanity query, returns marker count `2`, and has `Verified=True`.

**Timing**: Scenario timeout is 18 minutes; backup and verification waits each have a 12-minute sub-budget. Cleanup deletes verification then backup CRs, drops `chaos_s30_backup`, restores the original `spec.backup`, and waits for a healthy baseline.

**Artifact deletion check** (scenarios 30, 30-encrypted, 31, 36 and 37, through the shared backup-deletion helper): before deleting the `MysqlBackup`, the helper reads the bucket and exact artifact location from `status.artifactStorage`, lists `<artifact>/` in RustFS through a port-forward and requires at least one object. After the CR is gone (up to 5 minutes, which covers the first pull of the operator image that runs `artifact-cleanup`), the same listing must be empty. The listing uses 5-key pages, so a dump spans several. A cleanup that exits successfully without deleting anything fails the scenario.

**Encrypted variant** (`make chaos-run SCENARIO=30-encrypted-backup-verification-rustfs`): creates a per-run passphrase Secret `bloodraven-backup-e2e-passphrase-<run-stamp>` with a random passphrase (it refuses to reuse an existing one), sets `encryption.algorithm=AES-256-GCM` on the `rustfs-e2e` profile, and uses objects under `e2e/30-encrypted-backup-verification-rustfs/<run-stamp>/` and schema `chaos_s30_encrypted_backup`. The backup must also report `status.encryption.mode=Passphrase`, and `status.artifactStorage.location` is `s3://bloodraven-backup-e2e/<prefix>/<backup>/`, the URL form `encrypt-upload` writes. This is the only scenario that renders the operator-image `encrypt-upload` and `decrypt-download` containers. Before it existed, every scenario ran unencrypted backups, so two encrypted-path bugs shipped without an E2E failure, both fixed in 1.3.2. The first was a bare `bloodraven` command (`exec: "bloodraven": executable file not found in $PATH`). The second passed the `s3://` location to `decrypt-download` as the object-key prefix, which RustFS rejects with `InvalidArgument`. Cleanup deletes that Secret by UID, and only if this run created it.

---

### 31. PITR Verification Against RustFS
**Category**: Backup/PITR verification | **Risk**: Medium

**Hypothesis**: With PITR enabled against RustFS, Bloodraven archives sealed binlogs and a pinned `MysqlBackupVerification` can replay to a timestamp so baseline and before-target rows exist while after-target rows are absent.

**Injection**: `make chaos-run SCENARIO=31-pitr-verification-rustfs`

The scenario uses the same RustFS bucket/profile/credential Secret as scenario 30 but isolates objects under `e2e/31-pitr-verification-rustfs/<run-stamp>/` and enables `spec.backup.pitr` (`profileName=rustfs-e2e`, `maxBinlogSize=1M`, `archivePollInterval=5s`). It waits for `Ready=True` from the patched MFG generation with no ordered update in progress, then requires the active sidecar `/archiver/status` endpoint to report `enabled=true`, `primary=true`, `storageType=S3`, and manifest prefix `<prefix>/binlogs` before taking the baseline backup. The generation gate prevents stale pre-patch readiness from starting a backup during the PITR sidecar rollout.

**Verify**: After the full backup succeeds, the scenario inserts a before-target marker row, captures a MySQL UTC timestamp, sleeps 2 seconds, inserts an after-target marker, executes `FLUSH BINARY LOGS`, and waits for sidecar archiver coverage with zero backlog. The pinned PITR verification uses `spec.pointInTime.mode=Timestamp` and a sanity scalar that returns `1` only when baseline and before-target rows are present and the after-target row is absent. Success also requires `status.pointInTime.newestReplayedCommitTime` and `Verified=True`.

**Timing**: Scenario timeout is 24 minutes; PITR rollout/archiver readiness has a 6-minute sub-budget, archive coverage has 3 minutes, and backup/verification waits each have 12 minutes. Cleanup deletes CRs, drops `chaos_s31_pitr`, restores the original `spec.backup` so PITR does not leak to later scenarios, and waits for a healthy baseline.

---

## Scenarios 32-39: Denial, restore, and Dragonfly-partition band

These eight scenarios add RBAC-denial, scoped-DNS, ordered-update-kill, planned-lag-timeout, restore-in-place, cross-site PITR, and Dragonfly-partition coverage. All are **full-profile-only** (omitted from `smoke`/`release`) except §37, which #206 replaced with a new scenario in the `release` profile; §36 and §37 also run `ResetBeforeRunAll`. The scenarios below other than §37 record their live-run history on the k3d playground (2026-07-11) under "Actual live result" — prior failure evidence is preserved for the scenarios that needed a fix before their final pass. §37's replacement has no live run yet; the retired original's 2026-07-11 pass does not carry over to it.

Three of these (§32, §36, §38) exposed a real operator defect and shipped a minimal fix; see the **Observed defect and fix** notes under those sections. §33 exposed a real playground/tooling defect (a NetworkPolicy that this CNI evaluates post-DNAT), also documented under **Observed defect and fix**. §35 and §36's first live attempt each hit a scenario-only assertion-timing race (not an operator bug), fixed in the scenario's wait/verify logic — see the per-scenario notes below.

### 32. MFG Status-Write Denial During Emergency Promotion
**Category**: RBAC denial / status durability | **Risk**: High | **Profile**: full-only

**Hypothesis**: Denying the operator `patch`/`update` on `shipstream.io/mysqlfailovergroups/status` and then scaling the active primary to 0 still completes MySQL promotion (the promotion is SQL, not a CR write). The CR status stays stale during the denial; after the ClusterRole is restored, status catches up to the promoted site within 90s with no second failover.

**Injection**: Resolve the ClusterRole bound to ServiceAccount `bloodraven-playground/bloodraven`; remove `patch`/`update` from only `mysqlfailovergroups/status` (siblings like `mysqlbackups/status` keep their verbs via rule-splitting); scale the active MySQL Deployment to 0.

**Verify**: The peer becomes writable at the MySQL layer (direct probe, not the frozen CR); `bloodraven_failovers_total{target_site=<peer>}` increments; the operator logs a forbidden `update fg status` error. After RBAC restore: `status.activeSite`, `lastFailover.target`, and `lastFailover.promotionGtidExecuted` heal to the promoted site within 90s and Ready returns True; the old primary rejoins without `RecoveryBlocked`.

**Timing**: 8-minute timeout; 90s status-heal budget after RBAC restore.

**Cleanup**: LIFO RBAC restore (idempotent closure), scale old primary back to 1, global recover, wait out anti-flap cooldown to a healthy baseline.

**Observed defect and fix**: The operator wrote CR status only on transition events; a status write denied mid-failover was logged and dropped with nothing re-attempting it once the cluster stabilized. Fix (`internal/controller/runner.go`, `topology.go`): `updateCRStatus` now returns its error, the `StatusCallback` records it, and `Poll` re-fires the status callback every cycle while a prior write is known-failed (the writer already `DeepEqual`-skips no-ops, so a healthy cluster pays only a diff). Covered by `TestUpdateCRStatus_DeniedWriteReturnsErrorThenHeals` and `TestPoll_StatusRetryReFiresCallbackUntilCleared`.

**Actual live result**: Passed on the k3d playground on the first live attempt (`20260711T004152Z`) with the status-write-retry fix above already in place: the ClusterRole edit denied `patch`/`update` on `mysqlfailovergroups/status` only, the active primary was scaled to 0, and MySQL promoted the peer at the data layer while the CR status stayed frozen during the denial. After RBAC restore, `status.activeSite`, `lastFailover.target`, and `lastFailover.promotionGtidExecuted` healed to the promoted site inside the 90s budget with no second failover and no `RecoveryBlocked` on rejoin.

---

### 33. Scoped Cluster-DNS Outage
**Category**: Network / DNS isolation | **Risk**: Medium | **Profile**: full-only

**Hypothesis**: A NetworkPolicy that blocks only kube-dns egress from the active MySQL pod (allowing all other egress) is a DNS-resolution outage, not a pod partition: the operator's DNSEndpoint API writes still succeed, no split-brain occurs, and MySQL either stays stable or the sidecar self-fences cleanly.

**Injection**: Discover the `kube-system/kube-dns` ClusterIP *and* its backend pod IPs (from the `kube-system/kube-dns` Endpoints); run a busybox **canary** pod with a `nslookup` probe loop and apply the policy shape (egress `0.0.0.0/0` with `except: [<kube-dns ClusterIP>/32, <pod IP>/32, ...]`) to prove this CNI enforces the exception; only then apply the same policy to the active MySQL pod.

**Verify**: Across >2× `leaseTimeout`, writable count never exceeds 1 and no site is `RecoveryBlocked`; the operator log shows **no** forbidden `apply DNSEndpoint` error (that is §38); the DNSEndpoint stays readable via the Kubernetes API. Sidecar DNS-resolution failures are captured as best-effort evidence. Both a stable-activeSite and a clean self-fence-then-failover are acceptable end states.

**Timing**: 6-minute timeout (canary ~90s + 45s hold + recovery).

**Cleanup**: Remove the DNS NetworkPolicy (canary pod and policy torn down in the same step); global recover verifies no chaos NetworkPolicy remains and waits for Ready with a replicating peer.

**Observed defect and fix**: The first live run's canary proved this CNI (k3d's default) enforces NetworkPolicy *after* Service DNAT: the deny rule excepted only the kube-dns ClusterIP, but the packet's destination is already a CoreDNS pod IP by the time the CNI evaluates it, so the exception never matched and DNS kept resolving — the outage silently never happened. Fix (`internal/playground/kube/networkpolicy.go`): `BuildDNSEgressDenyPolicy`/`BuildDNSEgressDenyPolicyForSelector` now accept a list of deny IPs and except all of them; a new `DiscoverKubeDNSEndpointIPs` resolves the live CoreDNS backend pod IPs from the `kube-system/kube-dns` Endpoints, and the scenario excepts the ClusterIP plus every backend pod IP so the policy blocks DNS regardless of whether the CNI enforces pre- or post-DNAT. Covered by `TestBuildDNSEgressDenyPolicy`, `TestBuildDNSEgressDenyPolicyForSelector_DedupesAndSkipsEmpty`, `TestDiscoverKubeDNSEndpointIPs(_NoBackends)`, and `TestDenyDNSEgressAppliesAndReverts`.

**Actual live result**: First live attempt (`20260711T004244Z`) failed precheck's own canary step after 50.294s — the canary kept resolving DNS (`dns=ok`) through the full 45s hold, confirming the post-DNAT root cause above rather than silently proceeding against a no-op policy (see `live-33-final.md`). After the pod-IP-except fix (`DiscoverKubeDNSEndpointIPs` plus the multi-IP `Except` list), the scenario passed live on `20260711T005348Z`: the canary reached `dns=fail`, the outage stayed DNS-scoped across the hold window (writable count never exceeded 1, no `RecoveryBlocked`), and the operator logged no forbidden `apply DNSEndpoint` error.

---

### 34. Operator Kill During Ordered-Update WaitReplica
**Category**: Ordered update / operator resilience | **Risk**: High | **Profile**: full-only

**Hypothesis**: Killing the operator while an ordered update is held at `WaitReplica` does not double-roll all MySQL pods or cause `TOTAL LOSS`. The replacement operator, whose in-memory update phase was lost, re-derives remaining drift from the Deployment spec-hash mismatch and completes the roll.

**Injection**: Patch `spec.sites[*].resources.requests.memory`; wait for `status.updatePhase` to engage (preferably `WaitReplica`); apply an **ingress-only** deny NetworkPolicy to the standby MySQL pod so the operator's health check of the freshly-rolled standby fails (egress/replication stays open, satisfying the updater's replicating-standby precondition once lifted); force-delete the operator pod; after the replacement is Available, remove the hold.

**Verify**: Never 0 writable (no double-roll / `TOTAL LOSS` window) and never >1 writable throughout; final `status.updatePhase` absent; both Deployments run the patched memory request; one writable + one read-only; no `TOTAL LOSS` operator log. Then restore original memory and wait for the revert roll to settle.

**Timing**: 12-minute timeout; 8-minute completion sub-budget.

**Cleanup**: Remove the standby hold policy (also swept by global recover), restore memory, wait for a healthy baseline.

**Actual live result**: Passed on the k3d playground on the first live attempt (`20260711T005458Z`) with no operator defect surfaced. The operator pod was force-deleted while `status.updatePhase` was held at `WaitReplica` behind the standby's ingress-only deny; the replacement operator re-derived the remaining drift from the Deployment spec-hash mismatch and completed the roll — writable count never dropped to 0 or rose above 1, `status.updatePhase` returned to empty, both Deployments landed on the patched memory request, and no `TOTAL LOSS` log fired. Re-run live on `20260711T033652Z` after the post-review scenario fix (threading the step context into `s34DeploymentsAt` instead of `context.Background()`): passed again with the same outcome.

---

### 35. Planned Switchover Lag-Timeout Rollback
**Category**: Planned failover state machine | **Risk**: Medium | **Profile**: full-only

**Hypothesis**: With `SOURCE_DELAY=60` on the target replica and a `maxLagWait=5s` annotation override, a planned switchover fences the source, waits for the target GTID to cover the fenced GTID, times out, and rolls back: `plannedFailover.phase=Failed` with `result.outcome=RolledBack` and `result.reason=LagTimeout`, source stays active, and the failover history does not advance to the target.

**Injection**: Set `SOURCE_DELAY=60` on the target (both threads still running, so the operator's replica-health check stays true); write a marker on the source **after** the delay is active (so the fenced GTID cannot be covered in time); annotate `bloodraven.shipstream.io/planned-failover=<target>:maxLagWait=5s`.

**Verify**: `plannedFailover.phase` reaches terminal `Failed` with outcome `RolledBack` and reason `LagTimeout` (the intermediate `Admitting` and `Fencing` phases are not asserted — they may not persist long enough to observe); `bloodraven_planned_failovers_total{result="rolled-back"}` increments; `status.activeSite` stays the source; `lastFailover.target` does not advance to the target; the source becomes writable again (eventual — unfencing clears `super_read_only` first); the target stays read-only and replicating.

**Timing**: 8-minute timeout.

**Cleanup**: Clear `SOURCE_DELAY` on the read-only site, wait for the marker to replicate, drop `chaos_s35`, clear the annotation (already consumed by the terminal state machine).

**Actual live result**: First live attempt (`20260711T005733Z`) reached the correct `plannedFailover.phase=Failed{LagTimeout}` outcome in 7.007s, then failed the very next verify check 21ms later on `status.activeSite=""` — a scenario-side assertion-timing race, not an operator bug: rollback unfences the source in two MySQL-level steps (`super_read_only` clears before `read_only`), and `activeSiteLocked()` reports no active site in the gap between them (see `live-35-final.md`). After replacing the one-shot check with the `s35RollbackConverged` poll (90s budget, hard-fails immediately if `activeSite`/`lastFailover.target` actually advance to the target), the scenario passed live on `20260711T011112Z`: the source settled back to sole-writable/active, failover history never advanced to the target, and the target stayed read-only and replicating. Re-run live on `20260711T033630Z` after the post-review cleanup fix (clearing `SOURCE_DELAY` on the stashed rollback target rather than the observed read-only site): passed again with the same outcome.

---

### 36. RustFS Outage During restoreInPlace
**Category**: Restore-in-place / storage outage | **Risk**: High | **Profile**: full-only, `ResetBeforeRunAll`

**Hypothesis**: A per-schema in-place restore whose RustFS storage is scaled to 0 mid-run terminates `Failed` with the topology frozen (no emergency failover, no reclone). The untouched safe schema survives on the active primary. A genuine execution failure records `confirmTokenUsed` (the executed confirm), so the terminal `Failed` **holds** on the same confirm — the operator does not auto-re-arm the destructive restore; a retry requires a new monotonic confirm. An invalid `confirm` (not an RFC 3339 timestamp) is refused at admission, so it never reaches the operator.

**Injection**: Configure an isolated RustFS profile under `e2e/36-…/<run-stamp>`; seed `chaos_s36_restore` and `chaos_s36_safe`; take a valid `MysqlBackup`; mutate only `chaos_s36_restore`; **confirm-token gate substep** — patch an invalid `confirm` and assert admission refuses it on `spec.restoreInPlace.confirm`, `spec.restoreInPlace` is unchanged and no run starts (no new Job, the status never records that confirm); apply a fresh RFC3339 `confirm` with `source.backupRef` and `loadOptions.includeSchemas=[chaos_s36_restore]`; wait until the restore Job is active (`phase=Restoring`, `jobName` set); scale the `rustfs` Deployment to 0.

**Verify**: Terminal `phase=Failed`; `targetSite` equals the active site at preflight; `status.activeSite` unchanged; no `bloodraven.shipstream.io/reclone-site` annotation; exactly one writable; no `RecoveryBlocked`; `chaos_s36_safe.canary` still reads `must-survive`; `confirmTokenUsed` equals the executed confirm and the terminal `Failed` holds across a sampling window (no auto-re-arm on the same confirm). Restore Job pod logs are captured as evidence of the S3/RustFS read failure.

**Timing**: 24-minute timeout.

**Cleanup**: Reverter scales RustFS back to 1 and waits available; remove `spec.restoreInPlace`; delete the restore Job; sweep the fixed-name in-place restore Job on every site (capturing pod logs first) so nothing leaks into the next run; delete backup CRs; restore the original backup spec; drop the scenario schemas. The inject step also sweeps a retained restore Job before arming a fresh `confirm`. If cleanup cannot prove the safe canary survived, `playground-chaos reset` is the documented remediation; `ResetBeforeRunAll` isolates the next attempt.

**Observed defect and fix**: The live runs exposed three real operator defects that compounded. (1) **Stale fixed-name Job inheritance** — the in-place restore Job name is fixed (`mysql-<fg>-<site>-inplace-restore`), so a terminal Job from a prior attempt survives across runs (and the playground reset). `inPlaceRestoring` read that leftover Job's phase and marked the *fresh* confirmed request `Failed` (with the prior backup path) without ever running it. (2) **Loader never enabled `local_infile`** — `util.loadDump()` loads via `LOAD DATA LOCAL INFILE`, which the server rejects unless `@@GLOBAL.local_infile=ON`; live primaries run the hardened MySQL-8 default `OFF`, so a per-schema restore DROPped the target schema in preflight and then failed inside the load — losing the schema. (3) **Auto-re-arm on failure** — once (1)/(2) let the Job genuinely fail, the terminal `Failed` left `confirmTokenUsed` empty (`stampTerminalFailure` never set it), so `confirmAdvances()` treated the unchanged `confirm` as fresh and re-armed the destructive restore (`Failed→Preflight→Fencing→Restoring`); the verify step then observed `Preflight`. This also violated the `RestoreInPlaceFailed`/`ConfirmTokenUsed` API contract, which says a failed restore holds until `confirm` advances. Fixes: `internal/controller/restore_inplace.go` stamps the `confirm` token on the Job (`bloodraven.shipstream.io/restore-confirm`) and binds each run to the confirm its Job was **accepted** under. A Job that is not this run's is removed only when it is *terminal* — an in-flight destructive Job is never deleted, whoever it belongs to, because killing a pod mid-`DROP`/mid-load would leave the schema half-gone and race a second loader over the wreckage; it is allowed to finish, and its terminal status is recorded against the confirm it actually ran with (so a `confirm` the user changed mid-run is not marked consumed by a run that never requested it — it re-arms its own run once the first is terminal). `stampTerminalFailure` records the executed `confirm` on a genuine execution failure (creds/build/Job) while leaving it empty for a pure validation rejection, so a failed-but-executed restore holds terminal and only a strictly-newer `confirm` retries (invalid-timestamp behavior preserved). `internal/controller/restore_script.py` treats `local_infile` as a **precondition of the load, not a best-effort nicety**: `_prepare_local_infile` enables and verifies it *before* the destructive preflight and aborts (exit 2, nothing dropped) if it cannot — the drop-then-fail hole that lost the schema is closed at the source — and the prior value is restored in a `finally` (a no-op on the verification path, whose throwaway mysqld already runs `--local-infile=1`), preserving the hardened posture outside the load window. Covered by `TestReconcileInPlaceRestore_StaleJobNotInherited`, `TestReconcileInPlaceRestore_MatchingFailedJobIsHonored`, `TestReconcileInPlaceRestore_RunningJobSurvivesConfirmChange`, `TestReconcileInPlaceRestore_TerminalTokenComesFromTheJob`, `TestReconcileInPlaceRestore_FailedJobRecordsTheJobsConfirm`, `TestInPlaceRestoreJobIsForConfirm`, `TestReconcileInPlaceRestore_FailedJobRecordsConfirmAndHolds`, `TestReconcileInPlaceRestore_TerminalFailedStaysIdleOnSameConfirm`, and `TestReconcileInPlaceRestore_TerminalFailedRearmsOnNewerConfirm`; the scenario samples the phase for 20s after `Failed` to prove no auto-re-arm.

**Actual live result**: Four live attempts, three diagnosed-and-fixed iterations before the final pass:

- `20260711T011139Z` failed in the inject step after 83ms with a stale `Failed` status from the *previous* substep's deliberately-invalid confirm — a scenario-side wait-helper race (`waitRestoreInPlaceActive` treated any observed terminal status as the fresh request's outcome, not just a fresh one), not an operator bug. Fixed by making the wait require positive evidence (`restoreInPlaceStatusChanged`) that the observed status post-dates the stale snapshot before honoring a terminal phase; see `live-36-final.md`.
- `20260711T012539Z` failed the same inject step after 4.092s, this time on a genuine terminal `Failed: Job has reached the specified backoff limit` — two compounding operator defects: (1) the in-place restore Job's fixed name let a leftover terminal Job from an earlier attempt be inherited and misattributed to the fresh confirmed request without ever running it, and (2) the loader never enabled `local_infile`, so every restore Job — leftover or fresh — failed inside `util.load_dump()` after already dropping the target schema. Fixed by stamping a `restore-confirm` annotation on the Job and binding each run to the confirm its Job was accepted under, and by making `local_infile` a verified precondition of the load; see **Observed defect and fix** above for the shipped behavior (review replaced the first cut's delete-on-mismatch with terminal-only removal, so an in-flight destructive Job is never killed) and `live-36-job-final.md` for the live diagnosis.
- `20260711T015238Z` reached the intended `Failed` outcome from the RustFS outage in 29.035s, then failed verify 12ms later: the terminal `Failed` had already auto-re-armed to `Preflight` because `stampTerminalFailure` never stamped `confirmTokenUsed` on a failure path, so `confirmAdvances(confirm, "")` treated the unchanged confirm as fresh. Fixed by recording the executed confirm on genuine execution failures (holding terminal on the same confirm) while leaving it empty on pure validation rejections; see `live-36-rearm-final.md`.
- `20260711T021732Z` passed live with all three fixes in place: the restore terminated `Failed` because of the RustFS outage, `targetSite`/`activeSite` and writable count were unaffected, no reclone annotation appeared, the safe canary (`chaos_s36_safe.canary`) still read `must-survive`, and `confirmTokenUsed` matched the executed confirm and held `Failed` across the 20s post-terminal sampling window with no auto-re-arm.
- `20260711T033531Z` re-ran live after review replaced the first cut's delete-on-mismatch Job handling with terminal-only removal: the restore Job log shows `BLOODRAVEN_LOCAL_INFILE_ENABLED prev=OFF` immediately before the drop (precondition verified ahead of any destructive step), then `BLOODRAVEN_LOAD_FAILED: While 'Opening dump': Failed to connect to rustfs...svc.cluster.local port 9000: Connection refused` from the RustFS outage, and `BLOODRAVEN_LOCAL_INFILE_RESTORED value=OFF` on the way out — the reviewed fix passed live with the same outcome as the prior final pass.

---

### 37. PITR Planned Failback Without Reclone {#37-pitr-planned-failback-no-reclone}
**Category**: PITR across failover (#206) | **Risk**: Medium | **Automated**: `37-pitr-planned-failback-no-reclone` (release profile and the label-gated e2e `pitr` job, `ResetBeforeRunAll`)

**Hypothesis**: A planned failover A→B and a planned failback B→A, with no reclone and no scenario `FLUSH BINARY LOGS` on b, restore every era exactly (DESIGN §4.15 Example 3). Until the failback b's short era is in no sealed file, so a target inside it is not covered. The failback archives it (#264, #229): the operator rotates the fenced b's binary log and b's sidecar, by then read-only, uploads the sealed file (b's own transactions); the operator also rotates a's binary log before a's writes are enabled, which seals a's replica copies of the era for a to upload once writable. A target inside b's era is covered from then on, before a flushes anything of its own.

**Setup**: an encrypted RustFS archive under `e2e/37-pitr-planned-failback-no-reclone/<run-stamp>` (`maxBinlogSize=64M`, so nothing rotates by size: a binlog is sealed only by a scenario flush or by one of the operator's rotations, at a promotion and at a planned failover's fence). The playground counter app writes only on request, so the scenario drives its own load: a background writer commits about 20 rows a second into `chaos_s37_pitr.load` on the era's primary across each era's marker, fence and witness marker. A window starts only after the writer's first commit (its first insert runs at once), and the writer is stopped, with its last insert committed, before every flush, failover, restore and backup, so the archive boundaries the scenario asserts stay exact; each window fails if the writer committed nothing.

**Fences**: each expected set comes from a fence on the active primary: `FLUSH TABLES WITH READ LOCK`, read `@@gtid_executed` and the server clock under the lock, hold the lock until the clock has passed the next whole second (the target T; `stopTime` admits no fraction), optionally `FLUSH BINARY LOGS` under the lock, then `UNLOCK TABLES`. Every transaction committed before the lock is in the set and older than T; every later one is newer. So the expected history is exact while the writer commits concurrently. A flushing fence's set is exactly the sealed file's exec set (the latest-mode expectation). A flushing fence (and every plain flush) reads the file being sealed (`SHOW BINARY LOG STATUS`) on the same connection just before `FLUSH BINARY LOGS`, and waits until the site's manifest in RustFS holds a row for that file that was not there before the flush; the archiver uploading some earlier file does not count.

**Backups**: after a backup Succeeds, the scenario waits until the server clock is a second past both the clock it read once the backup reported Succeeded (which bounds the stamp's `serverClock.end`) and the dump's `@.done.json` end rounded up to the second, so no later target refuses `PITRBackupAfterTarget` (D-52). Each timestamp verification asserts its T is past that bound.

**Injection and verification**:

1. Seed the marker table, take backup β1, and read its baseline B from the dump's `@.json` (decrypted).
2. Era A: marker M1, fence T1, witness M1b, flushing fence on a.
3. Planned A→B (after the anti-flap cooldown). Wait until b's sidecar reports `promotionRecordedAt` for this promotion, observed no earlier than the request time read on b's server clock (the clock b's sidecar shares; the runner's clock is not compared). Every planned failover of the PITR scenarios also waits, up to 3 minutes, until the outgoing primary's sidecar reports `tail.state=Drained` in `/archiver/status` from a scan that saw the operator's rotation (its `tail.activeFile` is no longer the file the primary was writing before the failover); here a's tail holds nothing new, since step 2 flushed it. Then wait until b's server clock is past `promotionRecordedAt` + δ (5 s) + 1 s: a replica-copy witness is anchored by b's record only when it committed more than δ after the record. Marker M2, fence T2 (asserted more than δ after the record), witness M2b. No scenario flush.
4. Between the failover and the failback: a timestamp verification at T2 refuses `PITRTargetNotCovered` (b's era is in b's active binlog only, and as replica copies in a's), and a latest verification is Verified with outcome `ArchiveHead` and the step-2 flush set.
5. Planned B→A, with no scenario flush on b. b's executed set is read just before the request (the writer is stopped, so it is the whole of era B). The failover wait additionally requires a **new row in b's manifest for the file b was writing before the failover**: era B was never flushed while b was writable, so the file the operator's rotation seals at the fence is its only sealed copy on b, and b's read-only sidecar must upload it.
6. Right after the failback, before a flushes anything: a timestamp verification at T2 is Verified (`TargetCovered`), and a latest verification is Verified with outcome `ArchiveHead` and exactly b's executed set from step 5. Before #264 neither held at this point: b's era reached the archive only when a's next flush sealed a's replica copies.
7. Era A': marker M3, fence T3, witness M3b, flushing fence on a.
8. Verifications of β1: T1, T2 inside b's era, T3, and latest.
9. An `initFromBackup` restore into a new group `s37-restored` (the source's two primary candidates, no Dragonfly, PITR off, seeded on the source's active site) at T2. The other site moves to the reader's zone: the source's standby node carries the source's `NoExecute` read-only taint, which the restored pods do not tolerate, and the operator seeds a new group only when every site is reachable. The CRD requires `loadBalancerIP` and `taintNodeSelector` on a primary candidate, so the restored sites get documentation-range LB IPs and a selector on a group-scoped label no node carries, which keeps the restored group's taint off the source's nodes.

**Verify** (every verification and the restore): `status.pointInTime.outcome` (`TargetCovered`, or `ArchiveHead` for latest); `gtidExecuted` **equals** the fenced set as GTID sets; `replayedTransactionCount` = |expected ∖ B|; `archive.archiveID` equals the source group's `status.pitr.archive.archiveID`; and the marker rows are exactly the ones committed before T (a verification sanity query, or a direct query on `s37-restored`). Nothing is asserted on segment counts or sites.

**Timing**: 60-minute timeout.

**Cleanup** (32-minute budget plus the group deletion): deletes `s37-restored` (its Deployments and PVCs are owned by it) only if it carries this run's `playground.bloodraven.shipstream.io/run` annotation, with a UID precondition, so a group left by an earlier `--no-cleanup` run is kept; a `Create` that returns `AlreadyExists` fails the step and marks nothing for deletion. Its archive and dump key snapshots have no owner (#203), so after it is gone they are deleted by its UID label (`bloodraven.shipstream.io/failover-group-uid`). Then it deletes the verifications and backups (asserting their RustFS artifacts are removed), drops `chaos_s37_pitr`, restores the original `spec.backup`, deletes the run's passphrase Secret, and waits for a healthy baseline past the anti-flap cooldown. It then waits up to 3 minutes until no non-terminal pod in the namespace (a terminating pod of an old ReplicaSet, a deleted verification's pod) mounts a key snapshot under the run's prefixes, since the operator refuses to release a snapshot a pod still mounts, and releases every archive binding of the run's prefixes, one `bloodraven.shipstream.io/release-pitr-archive-binding=<id>:confirm=<group>:uid=<uid>` annotation per id, waiting up to 2 minutes each (a failed release is a cleanup warning, and the annotation is taken back), and deletes any key snapshot under those prefixes that no binding names. Finally it deletes every object under each archive prefix the run configured (`e2e/<scenario>/<run-stamp>[-<suffix>]/`). Archive objects are always decrypted with the passphrase bytes stashed when the run created its Secret, never with the live Secret.

**Status**: written and compiled against the #206 series-2 API. Its first live runs are the e2e `pitr` job on the #206 PR (label `e2e`), which runs 31, 37, 68 and 70 through `run-all --scenarios`, so each `ResetBeforeRunAll` reset happens; record the run stamps here once they pass. The pre-#206 version of this scenario (`37-pitr-archive-handoff-across-failover`, which expected `PITRCrossSiteOrder`) is gone. The #264 changes (the outgoing-tail waits and steps 5 and 6) are written and compiled; they have not run on a cluster yet, and their first live run is the same job.

---

### 38. DNSEndpoint Write Denial During Failover
**Category**: RBAC denial / DNS failover | **Risk**: High | **Profile**: full-only

**Hypothesis**: Denying the operator `create`/`patch`/`update`/`delete` on `externaldns.k8s.io/dnsendpoints` (keeping `get`/`list`/`watch`) then forcing an emergency promotion still promotes the peer and writes CR status normally (status is not denied), but the DNSEndpoint target stays stale. After RBAC restore the DNSEndpoint flips to the promoted site's LBIP within 90s with no second failover.

**Injection**: Capture the current DNSEndpoint target; remove the four write verbs from `dnsendpoints` only; scale the active MySQL Deployment to 0.

**Verify**: `status.activeSite` and `lastFailover.target` flip to the peer; the DNSEndpoint target stays stale (never the peer LBIP) during denial; the operator logs `DNS flip failed after successful promotion` / forbidden `apply DNSEndpoint`. After RBAC restore: the DNSEndpoint target becomes the promoted LBIP within 90s and neither `activeSite` nor `lastFailover.time` advances (no re-failover). Finally the old primary rejoins and the DNS target matches the final active site.

**Timing**: 10-minute timeout; 90s DNS-heal budget.

**Cleanup**: LIFO RBAC restore, scale old primary back to 1, wait for recovery, verify DNSEndpoint equals the final active site, healthy baseline.

**Observed defect and fix**: The operator applied the DNSEndpoint only at promotion/re-assert time; a promotion-time apply denied by RBAC was logged and dropped with no per-poll reconcile to retry it once the group stabilized. Fix (`internal/controller/topology.go`, `internal/platform/dns.go`): DNS is now reconciled level-triggered. `reconcileDNS` runs on every poll, derives the desired target from live topology (the single writable site, or a promotion this process just performed), compares it against the live DNSEndpoint (read back through the new optional `platform.DNSRecordReader`), and re-applies only on a real divergence — healing DNS without re-running promotion or touching MySQL. Deriving the target rather than memoizing it buys two properties a pending-target retry could not: the heal **survives an operator restart** (nothing needs to be remembered — the live record and the live topology are enough), and it can **never replay a superseded target** at a site that is now read-only. Every DNS write goes through one `applyDNS` helper, so `bloodraven_dns_flips_total{site}` counts exactly the target applied, and only when the record's value really changed. Covered by `TestReconcileDNS_HealsStaleRecordAfterOperatorRestart`, `TestReconcileDNS_NeverReplaysSupersededTarget`, `TestReconcileDNS_MatchingRecordIsNeverRewritten`, `TestReconcileDNS_DeniedPromotionFlipHealsWithoutRefailover`, `TestReconcileDNS_HealsWithoutRecordReadCapability`, and `TestDNSEndpointUpdater_CurrentDNSRecord`.

**Actual live result**: Passed on the k3d playground on the first live attempt (`20260711T022237Z`), with a first-cut version of the heal in place (a promotion-time pending target re-applied every poll): the four write verbs were denied on `dnsendpoints`, the active primary was scaled to 0, and `status.activeSite`/`lastFailover.target` flipped to the peer while the DNSEndpoint target stayed stale and the operator logged the forbidden `apply DNSEndpoint` error. After RBAC restore the DNSEndpoint healed to the promoted LBIP inside the 90s budget with no second failover, and the old primary rejoined with the DNS target matching the final active site. That first cut was superseded in review by the level-triggered `reconcileDNS` described above — same observable behavior for this scenario, plus restart-safety and no stale-target replay. The replacement was re-run live on `20260711T033413Z` (RBAC deny/restore captured in `s38-clusterrole-original.json`/`s38-clusterrole-patched.json`): passed again with the same outcome — DNS stayed stale during the denial and healed to the promoted LBIP within the 90s budget after RBAC restore, with no second failover.

---

### 39. Live Dragonfly Master Partition
**Category**: Dragonfly HA / partition | **Risk**: Medium | **Profile**: full-only

**Hypothesis**: A deny-all NetworkPolicy on only the Dragonfly master pod makes Bloodraven see it `Unreachable` and promote the surviving replica: `status.dragonfly.activeSite` flips, the active Dragonfly Service endpoints move to the promoted pod, `bloodraven_dragonfly_promotions_total` increments, and MySQL `status.activeSite` and failover counters are unchanged.

**Injection**: Seed a key on the current Dragonfly master and confirm it replicated to the peer; apply a deny-all (ingress+egress) NetworkPolicy selecting only the master site's Dragonfly pod labels. MySQL pods and Services are untouched.

**Verify**: The old master goes `Unreachable`; `status.dragonfly.activeSite` flips to the peer; the seeded key reads back on the new master; the active Dragonfly Service endpoints converge to only the promoted pod; `dragonfly_promotions_total{result="success"}` increments; MySQL `status.activeSite` is unchanged and no MySQL failover metric increments.

**Timing**: 8-minute timeout.

**Cleanup**: Remove the partition; wait for the old master to rejoin as a healthy replica (`linkStatus=up`). If it returns a stale master and does not reconfigure within the settle budget, force-delete only the old Dragonfly pod (documented conservative recovery) so the StatefulSet recreates it cleanly.

**Actual live result**: Passed on the k3d playground on the first live attempt (`20260711T022509Z`) with no operator defect surfaced. The deny-all NetworkPolicy on the master's Dragonfly pod drove it `Unreachable`, `status.dragonfly.activeSite` flipped to the peer, the seeded key read back on the new master, the active Dragonfly Service endpoints converged to only the promoted pod, and `bloodraven_dragonfly_promotions_total{result="success"}` incremented — while MySQL `status.activeSite` and failover counters stayed unchanged throughout.

---

### D6b. Dragonfly Rolling Image Update
**Category**: Zero-downtime operations | **Risk**: Medium

**Hypothesis**: A normal `spec.dragonfly.image` change rolls one Dragonfly pod at a time: the non-active site updates first, Bloodraven promotes that updated replica, and the old active site updates only after traffic has moved.

**Current status**: Live-passing in k3d as of May 3, 2026: `PASS 27-dragonfly-rolling-image-update`, duration 16.159s.

**Injection**: `make chaos-run SCENARIO=27-dragonfly-rolling-image-update`

The scenario patches `spec.dragonfly.image` to the digest reference already reported by a running Dragonfly pod. That changes the pod template and exercises the image rollout path without depending on a new external image pull.

**Verify**: No more than one Dragonfly pod is unavailable at any observed point; both StatefulSets and pods reach the target image; `status.dragonfly.phase` returns to `Ready`; and the runner cleanup restores the original tag image.

---

## Appendix scenarios (state-machine coverage)

These two scenarios cover state-machine paths that don't fit the §1-§30 emergency-failover grid. They are registered with the same `playground-chaos` runner and follow the same Inject → Observe → Verify shape; their cited `§S` and `§SBR` anchors are referenced from the "Currently automated" list above.

### S. Planned Switchover (`02-planned-switchover`) {#planned-switchover}
**Category**: Planned-failover state machine | **Risk**: Low

**Hypothesis**: Annotating the MFG with `bloodraven.shipstream.io/planned-failover=<peer>` walks `status.plannedFailover.phase` through `Admitting → Fencing → Draining → WaitingForLag → Promoting → Publishing → Succeeded`, and the promotion watermark `status.lastFailover.promotionGtidExecuted` equals the fenced set `status.plannedFailover.fence.gtidSet` (the target held every acknowledged transaction when it was promoted, by construction).

**Injection**: `make chaos-run SCENARIO=02-planned-switchover`. The scenario picks the peer of the current active site as the target and patches the annotation.

**Verify**:

- `status.plannedFailover.phase` reaches `Succeeded` with `targetSite=<peer>` (stale plannedFailover blocks left over from prior runs are ignored via the `startTime` check, so this assertion is robust under rerun).
- `result.outcome` is `Succeeded`; the `fence` record carries `gtidSet` and `sourceServerUUID`, the `promotion` record carries `confirmationTime` and `targetServerUUID`, and `status.lastFailover.promotionGtidExecuted` equals `fence.gtidSet`.
- Direct `SHOW REPLICA STATUS` on every non-active follower reports both threads running and `Source_Host=mysql-playground-<new-active>-internal.bloodraven-playground.svc.cluster.local`; this explicitly covers the demoted old primary and the reader.
- `bloodraven_planned_failovers_total{target_site=<peer>,result="succeeded"}` increments.

**Note**: Single-scenario `run` will fail precheck if the cluster already has an in-flight `plannedFailover` in a non-terminal phase. The runner's standard cleanup waits for `Ready=True` before the next scenario starts, so `run-all` does not need to reset between this and the next entry.

---

### SBR. Split-Brain Auto-Resolve (`05-split-brain-auto-resolve`) {#split-brain-auto-resolve}
**Category**: Split-brain auto-resolve | **Risk**: Medium

**Hypothesis**: With `spec.splitBrainPolicy.sitePriorities` set, forcing both sites simultaneously writable (clear `super_read_only` on the standby) makes the operator fence the non-preferred site within one reconcile, increment `bloodraven_split_brain_auto_resolve_total{prefer_site=<preferred>}`, and emit the canonical `split-brain auto-resolve` log line.

**Precondition**: `spec.splitBrainPolicy.sitePriorities` is empty on the baseline `playground` MFG. The scenario's `Precheck` returns a specific error when it is missing — set it before running:

```bash
kubectl -n bloodraven-playground patch mysqlfailovergroup playground --type json \
  -p '[{"op":"add","path":"/spec/splitBrainPolicy","value":{"sitePriorities":["iad","pdx"]}}]'
```

**Injection**: `make chaos-run SCENARIO=05-split-brain-auto-resolve`. The scenario also briefly scales the operator to 0 to clear `status.lastFailover` so the cooldown branch does not suppress the auto-resolve.

**Verify**:

- The non-preferred site stops being writable; only the priority site remains `state=Writable`.
- `status.activeSite` equals `spec.splitBrainPolicy.sitePriorities[0]`.
- The `bloodraven_split_brain_auto_resolve_total{prefer_site=...}` counter increments above the pre-injection baseline.
- The operator log contains `split-brain auto-resolve` (canonical msg from `internal/controller/topology.go`).

**Cleanup**: None scenario-level — the runner's `GlobalRecover` and the operator's next reconcile re-assert `super_read_only` on the standby.

---

### 40. Reader Data Loss and Auto-Clone {#40-reader-data-loss-and-auto-clone}
**Category**: Reader recovery and endpoint safety | **Risk**: High

**Hypothesis**: Losing only the dedicated `ReadOnly` reader's local data does not affect failover-group readiness or the writable primary. The operator removes the unhealthy reader from its client Service, recreates it from the confirmed active primary, converges it to a direct source, and republishes it only after threads and lag are healthy.

**Injection**: `make chaos-run SCENARIO=40-reader-data-loss-reclone`. The scenario verifies that the reader is on the `zone-reader` worker, scales its Deployment to zero, deletes and observes replacement of its PVC UID, then scales it back to one. This deterministic local-PV replacement is the playground interpretation of reader node/storage loss; it avoids relying on literal node deletion and cloud volume behavior.

**Verify**:

- Exactly one spec site has effective role `ReadOnly`; its pod is on a dedicated `zone-reader` worker.
- A marker written to the active primary is visible on the reader before loss and after recovery.
- A continuous observer records any group `Ready` invariant violation during scale-down, empty-datadir detection, clone, or catch-up; `stopAndCheck` reports the first recorded error before the step completes.
- Operator logs contain `starting bootstrap` with `source=auto-clone`, `donor=<captured-active>`, `recipient=reader`, and `donorHost=mysql-playground-<captured-active>-internal.bloodraven-playground.svc.cluster.local`.
- The active site never changes. Final reader status is `State=read-only`, `SourceConvergenceState=Converged`, direct `SourceHost`, known lag at or below `readOnlyMaxLagSeconds`, and live MySQL IO/SQL threads running.
- The reader client Service exposes only MySQL and has no serving EndpointSlice target while unhealthy. After convergence it has exactly the replacement reader pod.
- The reader internal Service remains `ClusterIP`, exposes MySQL and sidecar, has `publishNotReadyAddresses=true`, does not use the healthy selector, and publishes the replacement pod while it is unready.

**Cleanup**: The PVC is intentionally replaced. The Deployment scale is restored immediately and the scenario does not finish until direct healthy replication and the client endpoint return. Standard runner forensics and cleanup remain active on every failure path.

---

### 41. Reader Availability During Unplanned Failover {#41-reader-availability-during-unplanned-failover}
**Category**: Reader recovery and endpoint safety | **Risk**: Medium

Issue #115 chaos proposal R3.

**Hypothesis**: During the unplanned-failover window the reader keeps serving (stale) reads — staleness is allowed, availability is required. After promotion, source convergence repoints the reader directly at the new primary; at no point does the reader apply events relayed through the demoted primary.

**Injection**: `make chaos-run SCENARIO=41-reader-availability-during-failover`. Seeds a marker on the active primary and confirms it reaches the reader, then scales the active primary's Deployment to 0 (sustained outage — pod-kill respawns in seconds and tests nothing).

**Verify**:

- A continuous observer answers a marker `SELECT` against the reader every 500ms throughout the window. One reconnect is allowed per tick (idle port-forward tunnels can drop); two consecutive failures fail the scenario. At least 20 successful reads are required so the observer provably covered the window.
- `status.activeSite` flips to the promotable standby and nowhere else.
- The reader's `SourceHost` history only ever moves old-primary → new-primary: any third host, or a return to the demoted primary after the repoint, fails the run.
- The reader never enters `SourceConvergenceState=Blocked` or `RecoveryBlocked`.
- Final reader status passes the serving contract against the new primary (`ReadOnly`, replicating, `Converged`, lag ≤ `readOnlyMaxLagSeconds`), a marker written on the new primary replicates to the reader, and the client Service publishes exactly the reader pod again.

**Cleanup**: The executor restores the old primary's scale; scenario cleanup reuses the scenario-08 auto-reclone path if the returning primary is divergent.

---

### 42. Reader Stall Does Not Degrade the Group {#42-reader-stall-does-not-degrade-the-group}
**Category**: Reader recovery and endpoint safety | **Risk**: Low

Issue #115 chaos proposal R4. Smoke-profile member: fast, no clone wait.

**Hypothesis**: A wedged OLAP reader is invisible to the failover machinery — lag grows unbounded with zero group-level effect. The only reactions are endpoint shedding and alertable (not page-able) per-site status and metrics.

**Injection**: `make chaos-run SCENARIO=42-reader-stall-no-group-degradation`. Applies `CHANGE REPLICATION SOURCE TO SOURCE_DELAY=600` on the reader in a single `STOP REPLICA; ...; START REPLICA` batch. SOURCE_DELAY — not `STOP REPLICA SQL_THREAD` — is the one lag injection the operator will not heal: both threads keep running on the correct source, so convergence sees a converged-but-lagging reader instead of a stopped one (same reasoning as scenario 14).

**Verify** (soak of 3× `maxLagSeconds`, one primary write per second to grow applied lag):

- Group `Ready` stays `True` every tick; `status.activeSite` and `status.lastFailover` never change (no failover, no anti-flap cooldown consumed).
- The promotable standby keeps replicating.
- The reader stays `ReadOnly` and never enters `SourceConvergenceState=Blocked` or `RecoveryBlocked`.
- Observed reader lag exceeds `readOnlyMaxLagSeconds` and the client Service sheds the reader endpoint.
- `bloodraven_replication_lag_seconds{site=reader,role="read-only"}` exceeds the reader threshold while `bloodraven_replication_source_state{site=reader,state="converged"}` stays 1 — the metrics report the stall honestly. A core RPO alert on `{role=~"primary-candidate|disaster-recovery"}` must stay silent.

**Rollback**: `SOURCE_DELAY=0`; the reader must catch up, pass the serving contract, and rejoin its client Service. Cleanup always clears the delay and drops the soak database through the primary.

---

### 43. Writable Reader Fence {#43-writable-reader-fence}
**Category**: Reader recovery and endpoint safety | **Risk**: Medium

Issue #115 chaos proposal R5 — the regression gate for role semantics under the worst input (spec gap #2).

**Hypothesis**: A reader that somehow becomes writable is fenced like a `DisasterRecovery` loser without debounce, its errant GTID trips the convergence containment gate instead of a silent repoint, and it is never a promotion target — not even by explicit admin request.

Two independent actors can close the writable window and both are correct: the operator's poll loop (`pollInterval`, 2s in the playground) fences any writable non-promotable site without debounce, and the reader's own sidecar fencing monitor (`PEER_CHECK_INTERVAL`, 5s) self-fences on topology mismatch. Whichever ticks first wins, so the scenario asserts the invariant plus a fence from *either* actor rather than racing one specific log line (issue #119).

Note the coverage consequence: only the sidecar's fence kills connections, so the replication-preservation assertion below exercises that path only on the runs the sidecar wins — roughly one in five. Forcing the actor deterministically was tried and abandoned; it requires holding a three-way race (operator poll, sidecar tick, scenario) still from outside, and every mechanism for doing so proved unsound or pulled the sidecar's internal state machine into the harness. The unit tests carry the filter logic; the nightly accumulates the end-to-end runs.

**Injection**: `make chaos-run SCENARIO=43-writable-reader-fence`. One multi-statement batch on the reader: `super_read_only=OFF`, `read_only=OFF`, then an errant `CREATE DATABASE`/`CREATE TABLE`/`INSERT` (reader-scoped split-brain). Later, `STOP REPLICA` on the reader forces the convergence invariant to act on the diverged follower.

**Verify**:

- `super_read_only` is back `ON` on the reader within 60s, and the fence was deliberate: either the operator logged `fenced writable non-promotable site` with `site=<reader>`, or the reader's sidecar logged `SELF-FENCING: topology mismatch`. Completion is already established by the `super_read_only` check, so the log only has to attribute the fence to a deliberate actor — which is why the sidecar is matched on its prompt decision line rather than the terminal `SELF-FENCED`, whose emission trails the connection eviction. Group `Ready` and `activeSite` untouched.
- The fence costs the reader nothing but its writability: replication is sampled immediately and again 25s later, past the sidecar's whole-fence deadline (`fenceTimeout`, 20s), so whichever actor fenced, every `KILL` it was going to issue has happened or been abandoned before the last look under any realistic ordering. Treat this as a backstop rather than a proof: the sidecar is an independent process, so no sampling window is provably race-free against it, and the deterministic coverage for this regression is the `killableConnection` unit test. A fence that kills the site's own `system user` threads strands it — permanently, once it is diverged enough for source convergence to be `Blocked`.
- Annotating a planned failover targeting the reader lands `status.plannedFailover.phase=Failed` carrying the "only PrimaryCandidate sites may be promoted" role error, and `bloodraven_planned_failovers_total{target_site=<reader>,result="not-started"}` increments.
- After `STOP REPLICA`, the operator logs `replication source convergence blocked`, the reader reaches `SourceConvergenceState=Blocked` with reason `GTIDDiverged` (never a silent restart), `bloodraven_replication_source_state{site=reader,state="blocked"}` flips to 1, and the reader is shed from its client Service.

**Cleanup**: The scenario reconciles the errant transactions by committing them as empty transactions on the active primary (refusing if the errant set carries any UUID other than the reader's — that demands `kubectl bloodraven reclone`), waits for the convergence invariant to restart the reader, then drops the scenario database through the primary so replication removes the errant row itself.

The replica GTID audit also reports the errant write: within one audit interval (30s) the reader shows `recoveryState=RecoveryBlocked` with `divergenceDetectedBy=ReplicaAudit` and the errant set in `divergentGtid`, and `Degraded` is `True` with reason `DivergentTransactions` while `Ready` stays `True`. The empty-transaction reconcile is exactly what clears it: the next audit proves the primary contains the set and logs `divergence cleared: replica holds no transactions the primary lacks`, which the run-all baseline (no `RecoveryBlocked` site) then sees.

---

### 44. Reader Source Convergence Invariant {#44-reader-source-convergence-invariant}
**Category**: Reader recovery and endpoint safety | **Risk**: Low

Issue #115 chaos proposal R9.

**Hypothesis**: Direct-source convergence is a periodic invariant of the poll loop, not a one-shot switchover event — *any* wrong-source state (operator drift, a partially-applied runbook, a pre-fix chained reader left over from an upgrade) heals with no failover. The divergent-wrong-source counterpart (must go `Blocked`, never silently repoint) is scenario 43's final phase.

**Injection**: `make chaos-run SCENARIO=44-reader-source-convergence-invariant`. Repoints the reader's replication at the promotable standby (`CHANGE REPLICATION SOURCE TO SOURCE_HOST=<standby-internal>` in one batch; credentials and GTID auto-positioning carry over), producing a working chained topology.

**Verify**:

- Operator log contains `replication source convergence started` with `site=<reader>`, `currentSource=<standby-internal>`, `expectedSource=<active-internal>`, followed by `replication source convergence complete` back onto the active primary — the documented log-schema events.
- Reader serving status and live `SHOW REPLICA STATUS` converge back onto the active primary; `bloodraven_replication_source_state{site=reader,state="converged"}` is 1.
- Group `Ready` stays `True`; `status.activeSite` and `status.lastFailover` are untouched.
- A marker written on the primary replicates to the reader over the repaired direct channel, and the client Service publishes exactly the reader pod.

**Cleanup**: None needed for the wrong-source state itself — repairing it is exactly what the invariant does. Defensive cleanup restarts the reader's replication channel if the scenario failed with it stopped.

---

### Planned reader scenarios (not yet automated)

Issue #115 proposals R6-R8 remain manual/full-profile follow-ups:

- **45 (R6) — Bootstrap ordering with reader present**: fresh 3-site deploy must clone the promotable standby and reach failover-capable `Ready=True` *before* the reader clones; a mid-clone reader failure retries without blocking bootstrap. Run manually via `./playground/reset-mysql.sh` + `./playground/setup.sh` while watching `starting bootstrap` ordering in operator logs.
- **46 (R7) — Primary dies mid-reader-clone**: trigger scenario 40's re-clone, hard-kill the donor primary while `CLONE INSTANCE` is in flight; failover must proceed un-wedged and the clone state machine must retry against the new primary. The playground's small dataset makes the in-flight window sub-second, so automation needs an artificial data volume first.
- **47 (R8) — `externalTrafficPolicy: Local` endpoint semantics**: probe the reader NodePort (30306) from reader and non-reader nodes across pod-down/up; requires per-node canary pods and is infra-dependent (kube-proxy implementation).

---

## 48. Keyring Seal And Rotation

**Automated**: `make chaos-run SCENARIO=48-keyring-seal-and-rotation`

**Quarantined from every batch profile.** It needs a playground brought up
with TLS and `spec.encryptionAtRest` enabled, which is not the baseline
the other scenarios assume:

```bash
BLOODRAVEN_SETUP_TLS=1 ./playground/setup.sh
./playground/enable-encryption.sh --fresh   # wipe + encrypt from birth
make chaos-run SCENARIO=48-keyring-seal-and-rotation
```

TLS is configured before the failover group is first observed so the
encryption adoption does not overlap a second restart-sensitive rollout.

`--fresh` wipes MySQL first so every tablespace is encrypted from birth.
Without it the script converts in place, which leaves pre-existing tables
plaintext — fine for exercising the keyring lifecycle, not representative
of a production adoption.

**Hypothesis**: every site reports `phase=Sealed` with a read-only keyring
component; `ALTER INSTANCE ROTATE INNODB MASTER KEY` is rejected on a
sealed site; annotating the replica for rotation mints escrow version N+1
and returns it to `Sealed` with data intact; annotating the active primary
is refused with a `KeyringRotationRejected` event.

**What it actually proves**, in order:

1. **Sealed means sealed.** Checks `performance_schema.keyring_component_status`
   on each site for `Read_only=Yes`, not just the operator's status field.
   A rendering bug could otherwise leave a writable keyring behind a Secret
   mount and the operator would call it sealed.
2. **The key is not on the data volume.** MySQL's own `Data_file` must be
   outside `/var/lib/mysql`, and the rendered pod must project the keyring
   from a Secret rather than an emptyDir. This is the whole at-rest claim:
   a stolen PVC must not carry the key that decrypts it.
3. **The seal is enforced by the engine.** An ad-hoc
   `ALTER INSTANCE ROTATE INNODB MASTER KEY` must fail. This is what makes
   "no unescrowed key can exist in the steady state" true rather than
   aspirational.
4. **Rotation on the primary is refused.** Rotation is the one lifecycle
   operation whose failure window would cost data rather than a re-clone,
   and only on the primary.
5. **Rotation on a replica is safe and complete.** A new immutable escrow
   version is minted, the previous version survives for rollback, the
   site returns to `Sealed`, and the data still decrypts.

**Expected duration**: ~5-8 minutes (dominated by the pod roll for the
unseal and the roll back to sealed).

**Manual observation** while it runs:

```bash
watch -n2 "kubectl -n bloodraven-playground get mysqlfailovergroup playground \
  -o jsonpath='{range .status.encryptionAtRest.sites[*]}{.name}{\"\t\"}{.phase}{\"\t\"}{.keyringSecret}{\"\n\"}{end}'"

./playground/enable-encryption.sh --status
```

**Known follow-ups** not automated here:

- **Clone into an encrypted recipient**: now scenario 51 below.
- **Escrow Secret loss**: deleting a sealed site's escrow Secret should
  raise `KeyringEscrowMissing` and phase `Failed`. Destructive and
  easy to get wrong by hand; see
  `site/content/docs/6.operations/4.runbooks.md#keyring-escrow-lost`. The operator-side
  behaviour — `Failed`, the sealed rendering retained so the live
  keyring is not thrown away, and automatic recovery once the Secret is
  restored — is pinned by unit tests in
  `internal/controller/encryption_chaos_test.go`.
- **Node loss with an unsealed site**: verifies the bounded-loss claim
  (a lost keyring costs a re-clone, never data). Needs real node
  eviction, not a pod kill.
- **Escrow endpoint outage with the operator alive**: needs a
  NetworkPolicy that denies only the operator's `:8443` escrow port
  without also cutting the sidecar's lease traffic to the operator —
  a chaos primitive that does not exist yet. The state-machine
  behaviour (stay `Unsealed`, surface the sidecar's own reason in
  `.status`, go `Failed` after `escrowTimeout`, never seal) is
  covered by unit tests.

---

## 50. Encrypted Pod Replace

**Automated**: `make chaos-run SCENARIO=50-encrypted-pod-replace`

**Quarantined from every batch profile** — same encryption baseline as
scenario 48 (`BLOODRAVEN_SETUP_TLS=1 ./playground/setup.sh` then
`./playground/enable-encryption.sh`).

**Hypothesis**: every encrypted site pairs encryption `my.cnf` with
keyring wiring; deleting the sealed replica's pod destroys its tmpfs
keyring, and the replacement pod comes back Ready without an InnoDB
keyring abort, on the *same* escrow version, with the data still
decrypting.

**What it actually proves**:

1. **Adopt atomicity (#136), checked live.** For every site, the
   ConfigMap the Deployment actually references must carry encryption
   settings if and only if the pod spec carries keyring wiring. A site
   that violates this looks healthy until the moment it restarts, then
   aborts with `Check keyring fail`.
2. **The sealed rendering is self-sufficient.** The replacement pod has
   no in-memory or tmpfs keyring to inherit — only the escrow Secret. If
   the Secret projection or the component manifest copy is wrong, the pod
   never becomes Ready. The failure path greps the mysql container for
   `Check keyring fail` so a brick is reported as a brick rather than a
   timeout.
3. **No silent re-bootstrap.** The escrow version must be unchanged. A
   version bump would mean the site created fresh keys, which do not match
   what its tablespaces were written with.
4. **The data actually decrypts.** Rows written before the kill read back,
   the keyring component is `Read_only=Yes`, and `Data_file` is still
   outside `/var/lib/mysql`.

**Expected duration**: ~3-6 minutes (dominated by pod recreation and the
replica catching up).

**Cleanup**: none — the Deployment controller recreates the pod.

---

## 51. Encrypted Reclone

**Automated**: `make chaos-run SCENARIO=51-encrypted-reclone`

**Quarantined and destructive** — it wipes and rebuilds the replica's
datadir. Same encryption baseline as scenarios 48 and 50.

**Hypothesis**: annotating `reclone-site` on a `Sealed` replica moves it
to `Unsealed`/`Clone` with a memory-backed keyring *before* any
`CLONE INSTANCE` runs; the clone completes and the recipient has a new
`server_uuid`; the site returns to `Sealed` against an escrow version no
older than the pre-clone one that holds the new server's binary log
encryption key, with the keyring still off the data PVC and the donor's
rows readable.

**Why the ordering matters**: a clone recipient re-encrypts the donor's
tablespace keys under its own new master key. A read-only keyring cannot
accept them, so MySQL fails the clone — or, worse, a half-configured
recipient ends up with a datadir nobody can read. The operator therefore
has to roll the pod onto the unsealed rendering and wait for it to be
Ready before starting the clone. `RequestKeyringUnseal` returning `false`
is what defers it.

**What it actually proves**:

1. **The cold-reclone interlock accepts the documented form.** The
   recipient has no divergent GTID, so the annotation must carry
   `:confirm=<group>:uid=<uid>` and no `gtidPrefix`.
2. **The unseal really flips the rendering.** When the transient
   `Unsealed`/`Clone` window is caught, the Deployment's keyring volume
   must be an `emptyDir`, not the Secret. A status field alone is not
   evidence. If the window is missed because the whole cycle completed
   inside one poll, that is noted and the end-state assertions carry the
   proof instead.
3. **The escrow never goes backwards.** A *lower* version would mean the
   site resealed onto superseded key material.
4. **The recipient is a new server, and its keyring followed (#265).**
   `@@server_uuid` differs from the one read before the reclone, and no
   `CloneServerUUIDNotRotated` Event was raised. `CLONE INSTANCE` itself
   writes nothing to the recipient's keyring: a recipient seeded from its
   own escrow already holds a usable master key, and the clone rewraps the
   donor's tablespace keys under that key. The restart that installs the
   clone does write: the launcher gives the recipient a new `server_uuid`,
   and a server with `binlog_encryption=ON` stores a binary log encryption
   key named after its `server_uuid` (`MySQLReplicationKey_<uuid>`) when it
   starts. That start runs on the unsealed keyring. With binary log
   encryption on, the scenario requires the escrow digest the site is
   re-sealed against to differ from the pre-clone one: sealed against the
   pre-clone bytes, mysqld could not start (`MY-013288`, verified against
   MySQL 9.7).
5. **The window closes.** `Read_only=Yes` again, `Data_file` outside
   `/var/lib/mysql`, donor rows present, and a non-zero count of
   encrypted tablespaces.

**Expected duration**: ~6-15 minutes (pod roll for the unseal, the clone
itself, then the roll back to sealed).

**Cleanup**: clears the reclone annotation if the scenario bailed out
before the operator consumed it.

The recipient stays `Unsealed`/`Clone` until bootstrap finishes
(`CLONE INSTANCE`, the post-clone mysqld restart, and replication
setup). `Escrowed`/`Clone` is the old livelock and fails the scenario.

---

## 52. Rotation Promotion Refused

**Automated**: `make chaos-run SCENARIO=52-rotation-promotion-refused`

**Quarantined from every batch profile** — same encryption baseline as
scenarios 48–51, and it holds the primary down. CI's encryption job
runs it explicitly.

**Hypothesis**: a replica with `UnsealReason=Rotation` is refused as a
promotion target on both paths. Planned failover onto that site fails
immediately with `reason: KeyringRotation` and does not change
`activeSite`. Scaling the primary to 0 then leaves the group
`Degraded/NoPrimary` naming `UnsealReason=Rotation` rather than
promoting the rotating replica; `bloodraven_failovers_total` does not
increment for that site.

**What it actually proves** (the #160 / EXP-01 + EXP-16 inverse
regression, against real MySQL):

1. **The planned-failover validator is wired.** Rejection happens
   before health checks, so a replica that is rolling for rotation is
   labelled `KeyringRotation`, not `TargetUnhealthy`. The source stays
   writable.
2. **Emergency failover will not make a mid-rotation keyring the sole
   authoritative copy.** The refused counter increments and
   `bloodraven_failovers_total` does not. `KeyringPromotionRefused` is
   emitted when the refused set changes (a fresh operator); re-runs
   against the same process rely on the counter. `Degraded` is not
   asserted — replication errors overwrite that single condition
   once the replica notices the source is gone.
3. **The cluster can leave the window.** Restoring the primary (or,
   if rotation finished while it was down, promoting the now-sealed
   replica) returns the group to one writable + healthy replicas, and
   the rotated site reseals.

Component coverage for the same gate lives in
`test/component/keyring_promotion_gate_test.go`. This scenario does
not repeat those cases.

**Expected duration**: ~8-15 minutes (dominated by the rotation pod
rolls and the up to ~37s emergency failover).

**Cleanup**: scales the primary back if the scenario bailed out while
it was at 0; clears a leftover `rotate-keyring` annotation.

---

## Deployment coordination

Scenarios 53-56 exercise the real `/deploy/v1` API, not a fake lease store. Each creates a unique `MysqlDatabase` with `spec.failoverGroupRef.name` matching the tested group and `spec.deploymentClients` matching its client ServiceAccount and namespace. The database must reach Ready before the client starts.

The client is a `python:3.13-alpine` Pod with a projected ServiceAccount token. Kubernetes obtains the token through TokenRequest with audience `bloodraven-deploy`; the ordinary API-audience token is not automounted. The client verifies the operator's certificate against the playground CA and calls `https://bloodraven.<namespace>.svc.cluster.local:8443/deploy/v1/groups/<group>`. No request uses plaintext or skips certificate verification. The embedded client logs status, expiry, and topology generation, never bearer or lease tokens.

`playground/setup.sh` enables the existing escrow TLS listener and `BLOODRAVEN_DEPLOY_API_ENABLED=true` by default for release-profile coverage. This prepares TLS material without enabling MySQL TLS or encryption at rest. `openssl` and access to pull `python:3.13-alpine` are prerequisites. Set `BLOODRAVEN_SETUP_DEPLOY_API=0` for a playground without these scenarios; their precheck then refuses to run. The installed CRDs and operator image must include the deployment API implementation.

```bash
BLOODRAVEN_SETUP_DEPLOY_API=1 ./playground/setup.sh
make chaos-check
make chaos-run SCENARIO=53-deployment-hold-expiry
make chaos-run SCENARIO=54-deployment-hold-owner-kill
make chaos-run SCENARIO=55-deployment-emergency-revocation
make chaos-run SCENARIO=56-deployment-live-renewal-fenced
```

| Scenario | Injection | Required evidence | Profile |
| --- | --- | --- | --- |
| 53 | Acquire migration then failover-hold, each with `ttlSeconds: 30`; request planned failover and deliberately do not renew | Current planned request has phase `Deferred`, `admission.deferReason` `DeploymentHold`, `admission.retryTime` matching hold expiry, and a message naming operation and instance. The active site and deferred state stay unchanged before expiry; afterward the plan succeeds without a DELETE, with outcome `Succeeded` (the promotion watermark is the fenced set, so nothing acknowledged is lost). | release, full |
| 54 | Renew both leases every five seconds; request planned failover, observe `DeploymentHold`, then force-delete the client Pod without releasing leases | A successful renewal precedes the kill. The hold remains effective until its recorded expiry, then planned failover succeeds (outcome `Succeeded`) despite the absent owner. | full |
| 55 | Keep renewing migration and hold leases with `ttlSeconds: 120` every five seconds; scale the active primary to zero for a sustained outage | Emergency promotion proceeds and its metric increments. Both live leases return `409` with `error: revoked`, an allowed revocation reason, and a higher topology generation. A fresh `DeploymentLeaseRevoked` Event belongs to the MFG. | release, full |
| 56 | Renew a migration lease every five seconds without a hold; request planned failover | At least one successful renewal precedes the topology change. The client receives `409 revoked` with a higher generation while its last renewal is still unexpired and stops renewing. Planned failover succeeds (outcome `Succeeded`). | full |

Every scenario checks that the MFG's persisted `status.topologyGeneration` increased from the grant generation. A `404`, an unexpected HTTP status, an invalid revocation reason, or a generation mismatch on a successful renewal fails the client; none is accepted as successful fencing.

During promotion, `423 unstable` with reason `TopologyPersistencePending` permits diagnostic polling on the five-second cadence until the last confirmed lease expiry. It is not a successful renewal and never extends that deadline. The emergency client's 120-second TTL keeps both leases unexpired through the relay-drain wait; other scenarios retain 30-second TTLs.

**Safety and cleanup:** The existing runner context allowlist, healthy-baseline check, in-progress marker, deadlines, and forensic capture remain in force. Only scenario 55 stops MySQL, and only the selected group's active Deployment; its registered reverter restores replicas on success or failure. No scenario deletes a MySQL PVC. Cleanup terminates the uniquely named client, then waits only while a lease could still be live: no wait once the operator has answered `409 revoked` for every lease and the client stopped (55, 56); until the recorded grant expiries plus 2 seconds of clock skew pass for the non-renewing client (53); and the full lease TTL plus one second (120 seconds for 55, 30 seconds otherwise) for the crash scenario (54) or whenever a failed run left that evidence missing. The wait is noted in `scenario.log`. Cleanup then deletes its `MysqlDatabase` and waits for the database/user deletion finalizer before removing its Secret, CA ConfigMap, and ServiceAccount. Failed finalization preserves the credentials and reports cleanup failure. `--no-cleanup` deliberately preserves fixtures for inspection; the client has a ten-minute active deadline to bound abandoned renewal. Client evidence is copied into `scenario.log` at observation points; while the Pod exists, inspect it with `kubectl -n bloodraven-playground logs <chaos-deploy-pod> -c client`.

**Validation status:** Go helper/profile tests and client protocol tests can run without a cluster. Real scenarios require a prepared playground and are not claimed as live-passing until executed against the completed API.

## Scenarios 57-61: Chaos round 2026-10-15 {#chaos-round-2026-10-15}

Found on a k3d playground with the `playground/ledger` workload, which records every write MySQL acknowledged and audits each site against that record (`ledger audit`). 57-59 and the rewritten 09 pin fixed bugs. 60 and 61 are quarantined: they assert the behaviour a fix must provide for two open design decisions and fail today. Two more findings are recorded at the end without automation, because the expected signal is not designed yet.

### 57. Hung Primary {#57-hung-primary}
**Category**: Gray failure | **Risk**: High | **Automated**: `57-hung-primary-detected-and-fenced`

**Hypothesis**: With the primary's mysqld frozen by SIGSTOP for 50s, the operator promotes the peer within 35s, the frozen site's sidecar is not restarted, and the old primary is `super_read_only` within 10s of resuming.

**Injection**: `FreezeSiteMysqld` runs a privileged hostPID pod on the site's node. The pod SIGSTOPs the mysqld whose cgroup carries the target pod's UID, then thaws it after the hold or on SIGTERM. SIGSTOP cannot be sent from inside the pod, because mysqld is the container's PID 1. A frozen server accepts TCP connections and blocks every query until the client's read timeout. No pod-kill or scale-to-0 scenario exercises that.

**History**: Before the fixes, every poll re-probed the hung site in several follow-up checks, and promotion took ~48s (now ~25s). The sidecar's liveness probe followed MySQL, and its startup safety net blocked unbounded before the HTTP server started. The sidecar crash-looped during the hang, and the old primary stayed writable for 21-43s after resuming (now 0.5s).

### 58. Hung Reader {#58-hung-reader}
**Category**: Gray failure / RTO | **Risk**: Medium | **Automated**: `58-hung-reader-does-not-slow-failover`

**Hypothesis**: With the reader's mysqld frozen and already declared unreachable for 30s, scaling the primary to 0 promotes the candidate within 15s. The healthy-reader baseline is ~7s.

**History**: The poll loop's backoff used to be shared, sized by the worst site's failure count. A reader or DR site that stayed down slowed probing of every site to a 30s cap. Measured: 24s to promote with a hung reader, and 65.6s with the reader scaled to 0 for 90s. Backoff is now per site; the same measurements are 11s and 6.0s.

**Still open**: During a hung site's first `failureThreshold` polls, before it is declared unreachable, every poll still waits for its probe timeout. A primary lost in that window is detected more slowly (~15s instead of ~6s). Fixing that means decoupling poll completion from the slowest probe.

### 59. Killed Clone {#59-killed-clone}
**Category**: Data safety | **Risk**: High | **Automated**: `59-killed-clone-never-promoted`

**Hypothesis**: Killing a candidate reclone's CLONE INSTANCE session after it has dropped the recipient's data fails the bootstrap: the operator logs `bootstrap failed`, never `bootstrap completed successfully`, and the recipient stays `super_read_only`. With the primary then scaled to 0, the operator refuses the candidate and promotes nothing for 45s: it logs `failover picker: refusing candidate whose data regressed`, or, when the recipient's `server_uuid` had issued transactions (it was the primary earlier), `promotion refused: candidate lacks transactions it issued itself`, which the own-loss check logs before the picker ranks candidates (#285). When the primary returns it is active again with its data, and the candidate is recloned. The killed clone leaves the candidate's `server_uuid` unchanged (it was rolled back inside the running mysqld and installed nothing); the repair clone that completes gives the candidate a new one, with no `CloneServerUUIDNotRotated` Event (#265).

**Injection**: 120 MiB of incompressible filler, `clone_max_data_bandwidth=1` on the recipient, a `reclone-site=<candidate>:confirm=<fg>:uid=<uid>` request, then `KILL` of the CLONE session once `performance_schema.clone_progress` shows FILE COPY in progress. The session is found through `performance_schema.clone_status.PID`. Its processlist `info` is NULL, because MySQL hides statements that carry a password.

**History (total data loss)**: The operator cleared `read_only` on the recipient for CLONE. The recipient's own sidecar then saw a writable non-active site, fenced it and killed the CLONE session. The operator treated the dropped connection as the expected post-clone restart and reported success on an empty site. This happened unprompted in 2 of about 8 reclones. When the primary later died, the picker promoted the empty site. The sidecar's first fix matched CLONE by processlist `info`, which MySQL hides. It now uses `clone_status`.

**Still open**: the operator does not retry a failed reclone. The recipient keeps the replication metadata it had before the clone, so empty-site auto-bootstrap skips it, and the group runs without that replica (`Bootstrapping=False/BootstrapFailed`, `Degraded=True/BootstrapFailed`, `Ready=False`) until someone sets `reclone-site` again. The scenario's settle step does exactly that.

### 60. Operator-Only Partition {#60-operator-only-partition}
**Category**: Partition / data safety | **Risk**: High | **Automated (quarantined)**: `60-operator-only-partition-no-acked-loss`

**Hypothesis (what a fix must provide)**: With only operator-to-primary traffic blocked for 60s, while clients, replication and peer sidecars still reach the primary, every write the primary acknowledged is on the active site after the partition heals, and no site is left blocked.

**Today (F5, open)**: The operator promotes the candidate although the candidate's IO thread is still streaming from that primary, which proves the primary is alive. Writes the old primary acknowledged between the promotion GTID and its sidecar's fence are lost: 66 in the original run. The reader, still replicating from the old primary, holds them as errant transactions, and both need a reclone. Clients that reconnect through the write Service then get `super_read_only` errors for 2+ minutes (F7). The emergency path does not strip the old primary's role label at failover, and the sidecar's eviction runs once. The leading fix is a replica-witness check before declaring the primary dead.

### 61. Reader Ahead of the New Primary {#61-reader-ahead-of-new-primary}
**Category**: Reader recovery | **Risk**: Medium | **Automated (quarantined)**: `61-reader-ahead-of-new-primary-recovers`

**Hypothesis (what a fix must provide)**: When the reader has applied transactions the promoted candidate never received, the reader converges on the new primary and serves again within 3 minutes, without a manual reclone.

**Injection**: An egress NetworkPolicy cuts only the candidate's replication link for 10s while the primary keeps committing. The policy excludes the active site's Service ClusterIPs as well as its pod, and the candidate's IO thread is restarted, because a NetworkPolicy does not cut an established connection. Then the primary is scaled to 0. The scenario fails fast unless the reader really holds transactions the new primary lacks. Losing those 10s of writes is the documented asynchronous RPO and is not asserted.

**Today (F6, open)**: The reader keeps the transactions as errant GTIDs, its source convergence stays `Blocked`, and its Service has no endpoints until someone reclones it. Rewriting scenario 09 hit the same thing unprompted: a reader that received writes the newly promoted site lacked was left `Blocked`.

### 62. Full-Instance Restore Failing After Its DROP {#62-full-instance-restore-failing-after-its-drop}
**Category**: Backup/restore | **Risk**: High | **Automated**: `62-full-restore-fails-after-drop-keeps-peers` (full profile, `ResetBeforeRunAll`)

**Hypothesis**: A full-instance in-place restore stops every peer's replication receiver before its Job runs (#212). When the Job drops the user schemas and then fails, `status.restoreReclones` is `Held`, every peer's receiver stays `OFF` across a two-minute window with the topology unfrozen, and every peer still holds the marker rows written before the restore (one in the backup, one after it).

**Injection**: Seed `chaos_s62_marker`, take a backup, write a second marker row, scale RustFS to 0, then start a full-instance restore (nothing before the Job reads object storage; cutting it once the status read `Fencing` raced a restore that finished in seconds), so the Job passes its `offline_mode` check, logs `BLOODRAVEN_DROP_ALL_USER_SCHEMAS_DONE`, and fails reading the dump. The scenario fails if the Job did not reach its DROP step.

**Cleanup**: removes the spec and Job, and removes `status.restoreReclones`, which resumes the peers' replication from the damaged primary; run-all resets before the next destructive scenario.

**Status**: written and compiled, not yet run on a live cluster.

### 63. Planned Failover Operator Restart Mid-Lag {#63-planned-failover-operator-restart-mid-lag}
**Category**: Planned-failover claim durability | **Risk**: Medium | **Automated**: `63-planned-failover-operator-restart-mid-lag` (release profile)

**Hypothesis**: A planned run that holds its claim survives an operator restart (#235). With the target's SQL thread stopped mid `WaitingForLag`, killing the operator leaves the run claimed: the replacement keeps the same `runID` in `WaitingForLag`, promotes nothing (no emergency promotion of the fenced group's only candidate), keeps the source fenced, and logs `cross-site action deferred: planned failover in progress`. Restarting the SQL thread ends the same run `Succeeded`, with exactly one `PlannedFailoverClaimGranted` Event, exactly one `RESET REPLICA ALL` on the target, and `fence.sourceServerUUID` and `promotion.targetServerUUID` matching the live servers.

**Injection**: Turn the general log on at the target, set `SOURCE_DELAY=600` on its channel to build a backlog, write a marker on the source, and annotate the planned failover with `maxLagWait=8m`. Once the run reaches `WaitingForLag`, `STOP REPLICA SQL_THREAD` on the target, then kill the operator pod and wait for its replacement.

**Verify**:

- The replacement logs the cross-site deferral; for a 30s soak the same `runID` stays `WaitingForLag`, `status.activeSite` does not move and the source stays `super_read_only=ON`.
- After `SOURCE_DELAY=0` and `START REPLICA SQL_THREAD`, the same run ends `Succeeded`.
- `fence.sourceServerUUID` and `promotion.targetServerUUID` are present and match `@@server_uuid` on the source and the target.
- Exactly one `PlannedFailoverClaimGranted` Event for the run: the replacement rehydrated the claim instead of re-granting it.
- Exactly one `RESET REPLICA ALL` on the target in its general log: no recovery path re-pointed the stopped replica behind the claim's back.
- Every follower replicates directly from the new primary.

**Cleanup**: Clears `SOURCE_DELAY` where the target still has a channel (a promoted target has none), restores the target's `general_log` and `log_output`, and drops the marker schema. The runner's reconvergence gate then also waits for the planned block to be terminal and for no automation hold, the same conditions the next scenario's precheck refuses.

### 64. Emergency Failover During Planned Admission {#64-emergency-failover-during-planned-admission}
**Category**: Planned-failover admission vs emergency failover | **Risk**: High | **Automated**: `64-emergency-failover-during-planned-admission` (full profile; needs the deploy API, see [Deployment coordination](#deployment-coordination))

**Hypothesis**: A planned request waiting in admission owns nothing, so it must not obstruct emergency failover (#235). A deployment client holds a renewing failover-hold lease (120s TTL, renewed every 5s), so the planned request to the peer is `Deferred` with `admission.deferReason: DeploymentHold`. Holding the primary down then promotes the peer within the emergency window (90s), revokes both deployment leases (`409 revoked` at a newer topology generation, and a `DeploymentLeaseRevoked` Event), which releases the hold. The same planned run then ends `Failed` with outcome `NotStarted` and reason `AlreadyActive`, because its target is now the primary; it never claims the dead source.

**Injection**: Create the renewing failover-hold client, request a planned failover to the peer, wait for `Deferred{DeploymentHold}`, then scale the primary's Deployment to 0 (a sustained outage, not a pod kill the Deployment would undo).

**Verify**:

- While the request is pending, it never owns sites with the lost primary as its source.
- `status.activeSite` moves to the planned target within 90s of the primary going down, and the failover metric increments. The playground has exactly one promotable peer, so the emergency successor is the planned target; the scenario fails if another site was promoted.
- Both leases are revoked and the client stops; `DeploymentLeaseRevoked` is recorded on the MFG.
- The same `runID` ends `NotStarted{AlreadyActive}` within 3 minutes, with `status.activeSite` still the new primary. Any other terminal result fails.

**Cleanup**: The deployment-coordination cleanup (lease settle, `MysqlDatabase` finalizer, credentials), then the old primary rejoins.

### 68. PITR Failback With Reclone {#68-pitr-failback-with-reclone}
**Category**: PITR across failover (#206) | **Risk**: Medium | **Automated**: `68-pitr-failback-with-reclone` (release profile and the e2e `pitr` job, `ResetBeforeRunAll`)

**Hypothesis**: A failback with a reclone of the old primary restores every era exactly, whether or not b's era was flushed before the reclone (DESIGN §4.15 Example 4 and variant 4g). An era b never flushed exists, once a is recloned, only in b's active binlog; the failback archives it as b's tail (#264). Before #264 nothing sealed it and a verification after the failback refused `PITRArchiveGap` (the unarchived-tail limitation FU-1). Each reclone starts only after the operator confirmed its recipient's binlog tail archived, and gives the recipient a new `server_uuid` (#265), under which a numbers the era after its failback.

**Injection and verification**: plaintext RustFS archive, fences as in §37.

Every era's marker, fence and witness run with the §37 background writer on the era's primary; the writer is stopped before the era's flush.

Every reclone (`bloodraven.shipstream.io/reclone-site=<a>:confirm=<group>:uid=<uid>`, once a is read-only with no recorded divergence; a `RecloneRejected` Event or a failed attempt fails the step at once) also asserts the operator's step before CLONE wipes the recipient's binary logs. With PITR enabled the operator rotates the recipient's binary log and waits, up to 2 minutes, until the recipient's sidecar reports its tail `Drained` from a scan that saw the rotation; only then does it clone. The step fails as soon as the group gets a new Warning Event `PITRArchiveTailNotArchived` naming the site or the operator logs WARN `pre-reclone archive tail not archived` for it (both are printed), and it fails when the reclone completes without the operator's INFO `pre-reclone archive tail archived` for the site (read from the operator's log from the request on; up to 30 s are allowed for the line to arrive). The clone wait is 10 minutes: the 8 it had, plus the 2 the tail wait may take. Every reclone also reads the recipient's `@@server_uuid` before the request and after the clone completed, and fails when it is the same or when the group got a `CloneServerUUIDNotRotated` Event for the site (#265).

1. Cycle 1: era A (marker, fence T1, witness, flush a); planned A→B; era B (marker, fence T2, witness, **flush b**; the writer is already stopped, so nothing commits on b between the flush and the failback); reclone a; planned B→A; era A' (marker, fence T3, witness, flush a). Verifications of β1 at T1, T2 and T3 are each Verified with the exact fenced set, the replay count against the dump's `@.json`, the archive ID and the marker rows. Every era of cycle 1 is archived by a scenario flush.
2. Cycle 2: planned A→B; era B (marker M4, fence T4, witness M4b, with the writer) and **no flush**; reclone a, which wipes a's binlogs and with them a's replica copies of that era, so it now exists only in b's active binlog; planned B→A, whose wait requires a new row in b's manifest for the file b was writing (the operator's rotation at the fence seals it and b's read-only sidecar uploads it, as in §37 step 5); era A'' (marker M5, fence T5, witness, flush a). Verifications of β1 at T4, inside b's unflushed era, and at T5, after it, are each Verified with the exact fenced set and marker rows. Before #264 the T5 verification refused `PITRArchiveGap`, naming b's `server_uuid`, "unarchived tail" and exactly b's era as a GTID set.
3. Take backup β2 after the failback, then marker, fence T6, witness, flush a. A verification of β2 at T6 is Verified exactly (a target before β2's dump end would refuse `PITRBackupAfterTarget`, so the covered check uses a new target).

**Not covered here**: the cases that still leave a tail unarchived, and so still refuse `PITRArchiveGap`: a primary that dies and never comes back, a tail whose upload did not finish within the reclone's 2-minute wait (announced by `PITRArchiveTailNotArchived`), and a sidecar whose gates refuse the upload (for example binlog encryption). No automated scenario produces one; the refusal's message is covered by the planner's unit tests and `test/component`.

**Timing**: 75-minute timeout. **Cleanup** (32-minute budget): as §37, without a restored group. **Status**: written and compiled; first live runs in the e2e `pitr` job on the #206 PR (see §37). The #264 changes (the reclone's tail assertions and cycle 2's verifications) and the #265 `server_uuid` assertion have not run on a cluster yet.

### 69. PITR Encryption Binding Refusal {#69-pitr-encryption-binding-refusal}
**Category**: PITR archive encryption binding (#203) | **Risk**: High | **Automated**: `69-pitr-encryption-binding-refusal` (full profile and the e2e `pitr` job, last, after 31, 37, 68 and 70, so a failure of 69 never stops the run before them; not in release; `ResetBeforeRunAll`)

**Hypothesis**: An encrypted PITR archive is bound (`status.pitrArchiveBindings`) to an immutable key snapshot the operator made from the profile's passphrase. Pods mount the snapshot, never the user's Secret, so a rotated passphrase, an encryption change made while PITR was off, and a same-storage alias with other bytes are refused on `PITRArchiveBindingReady` without a roll, and writers restarted afterwards keep archiving with the accepted key (#200, #203). The documented procedure (a new profile on a new prefix with its own passphrase, a full backup, a release) works end to end.

**Setup**: encrypted RustFS profile A (`rustfs-e2e`) on prefix P1 = `e2e/69-…/<run-stamp>-p1`. The passphrase bytes are stashed when the run creates the Secret; every archive object is decrypted with stashed bytes, never with the live Secret.

1. `PITRArchiveBindingReady=True`; P1's entry is `Bound/Bound` with `encryption.keySecretName` (not the user Secret). Each PrimaryCandidate site Deployment mounts that snapshot with `optional: true`, never the user Secret, renders `BLOODRAVEN_PITR_KEY_REVISION` = the snapshot and no `BLOODRAVEN_PITR_PROFILE_NAME`, and its `bloodraven.shipstream.io/pitr-render` annotation names the snapshot; `status.pitr.archive.keyRevision` equals it. The snapshot is `immutable`, has no `ownerReferences`, carries the group-UID and binding-id labels and a `bloodraven.shipstream.io/pitr-accepted` annotation with a `keyCheck`, and holds the original bytes.
2. Marker table and baseline; backup β1. Its `status.artifactStorage.passphraseSecretKeyRef` is its own immutable, ownerless `mysqlbackup-…-dump-key-…` snapshot (label `mysqlbackup-uid`) holding the original bytes. Marker M1, flush.
3. Admission, as `dryRun=All` patches: removing A's encryption, renaming its `passphraseSecretKeyRef` to a same-bytes copy, and changing its `key` are each refused with the root rule's message.
4. Rotate the user Secret in place: `PITRArchiveBindingReady=False/KeyChangeRefused` and Warning `PITRArchiveKeyChangeRefused`; P1's entry is unchanged and no Deployment `spec-hash` or pod changes for two operator polls (60 s).
5. Restart the replica's pod, then (once it replicates) the primary's; the group may fail over, and the step waits until it is Ready, the failover target replicates and the active archiver archives P1. Both pods started after step 4. Marker M2 and a flushing fence on the current primary: the sealed file is in that site's manifest, its archiver reports `Verified` with the snapshot as `keyRevision`, and the archived object decrypts with the original bytes. Then the witness M2b, which is after the fence's T, flushed and archived: the fence's flush archives only the history before T, and a target is covered only once a transaction at or after it is archived. Every expectation at M2's fence stays the fenced history (baseline, M1, M2).
6. A verification of β1 at M2's fence is Verified exactly: the old dump decrypts with its own snapshot after the Secret edit.
7. Set the user Secret to the original bytes plus a newline: `True`, no roll.
8. **A real refused encryption edit (K11)**: `pitr.enabled=false` (one roll); remove A's encryption (the rule admits it with PITR off); `pitr.enabled=true`. `False/EncryptionChangeRefused` and Warning `PITRArchiveEncryptionChangeRefused`; the re-enabled render is the binding's encrypted identity (the snapshot, `optional`, the same revision). Restart the primary's pod; M3 is archived by a pod started in this step and decrypts with the original bytes. A's encryption is then put back without a roll: the rule refuses adding it while A archives, so the run switches `pitr.profileName` to an alias of A with A's encryption (an identical render: the profile name is not rendered), restores A's encryption, and switches back (`True`, no roll).
9. Profile B: the same storage as A with a Secret holding other bytes; switch `pitr.profileName` to B. `False/KeyChangeRefused` naming both profiles, Warning, no roll; M4 is archived with the accepted key. Switch back: `True`, no roll.
10. **Read binding with the wrong key (C1)**: profile D reads P1 through another spelling of the RustFS endpoint (`http://rustfs.bloodraven-playground.svc:9000`, a distinct destination over the same objects) with a Secret holding wrong bytes. A verification of β1 with `archiveProfileName: D` starts a `pitr-archive-check` Job (a Reader check, carrying annotation `bloodraven.shipstream.io/pitr-key-secret-uid`), D's entry ends `Refused/KeyMismatch` (Warning `PITRArchiveBindingRefused`; the message says the refusal was "reported by the reader check of ..."), and the verification waits, reporting `PITRArchiveBindingNotReady`. Putting the right bytes in D's Secret runs a new check, D's entry is `Bound` and the verification is Verified exactly.
11. **Status wipe and operator restart**: scale the operator to 0, JSON-patch away `/status/pitrArchiveBindings`, scale it back. P1's and D's entries come back `Bound` with the same snapshots, with the operator line `pitr archive binding changed` `cause=recovery` for each, no `pitr-archive-check` Job and no recorded check (watched for 30 s after the rebuild), and no Deployment or pod change.
12. **Procedure**: profile C on P2 with a new passphrase; switch to it. P2's entry passes through `Pending/Establishing` (a check Job found the prefix empty) and ends `Bound` on the archive the sidecars report, after one roll; P1's entry is retained. The condition carries the baseline note until a full backup β2 of C, whose stamp's `pitrArchiveID` is P2's and whose dump key is its own snapshot of C's bytes; then the note is gone. A's user Secret is deleted.
13. **Release**: a release of P1 with a wrong `confirm`, and one with this group's name but a stale `uid` (WE-Q9), are rejected (Warning `PITRArchiveBindingReleaseRejected`, annotation removed, entry unchanged). A verification of β1 with `archiveProfileName: A` starts; once P1's snapshot carries its hold (`verification-hold.bloodraven.shipstream.io/<uid>`), a release with the right `confirm` and `uid` is refused (Warning `PITRArchiveBindingReleaseRefused`, entry unchanged; a site Deployment that renders P1's destination, references its snapshot in a pod template or carries an undecodable `pitr-render` annotation, or a non-terminal pod referencing the snapshot, would refuse it too). The verification is Verified exactly with A's user Secret gone (the retained snapshot). Then the kept annotation completes: Normal `PITRArchiveBindingReleased`, P1's entry, annotation and snapshots are gone. D's binding is released too.

**Timing**: 60-minute timeout (unmeasured; the steps add up to about 35-40 minutes). **Cleanup** (32-minute budget plus 4 minutes): removes a leftover release annotation, then as §37 (no restored group; the cleanup releases P2's binding and any binding left under P1), and deletes the extra passphrase Secrets the run created (UID precondition).

**Status**: written and compiled against the #203 API and contracts; first live runs in the e2e `pitr` job on the #203 PR (label `e2e`).

### 70. PITR Lossy Failover {#70-pitr-lossy-failover}
**Category**: PITR across failover (#206) | **Risk**: High | **Automated**: `70-pitr-lossy-failover` (release profile and the e2e `pitr` job, `ResetBeforeRunAll`)

**Hypothesis**: After a failover that loses archived transactions, PITR refuses until they are excluded or a newer backup is used (DESIGN §4.15 Examples 5, 8(c) and O2-5). Lost transactions the old primary had not archived when it died reach the archive when it comes back (#264): its sidecar, read-only, uploads the binary log that held them before anything reclones it, and restores refuse naming them too. Recloning the old primary and failing back to it leaves the archive usable (#265): the clone gives it a new `server_uuid`, so its new writes cannot reuse the lost transactions' GTIDs. Its archived lost rows stay as divergent history, which is evidence and not an archiver error; the failback raises no `PITRArchiveReissueRiskDetected`, and no verification refuses `PITRArchiveHistoryReset`. Until #265 this was the D-46 fail-closed path (a Warning, then a whole-archive refusal until a new prefix and a full backup), which now applies only to a server that lost history and kept its `server_uuid`; no playground scenario produces that.

**Injection and verification**: plaintext RustFS archive, fences as in §37.

1. Take β1. Wait out any anti-flap cooldown. Write era P on a (marker P with the background writer) and wait until b has applied it: P is history b keeps, so a's archive holds something beyond β1 that survives the loss (without it, latest with `excludeGtids` = L has nothing to replay and refuses `PITRNoArchivedReplay`). Delay b's applier with `SOURCE_DELAY=300` (the operator auto-recovers `STOP REPLICA SQL_THREAD`, see §14, and the failover's 30 s relay-log drain restarts a stopped applier, so a delay is what keeps L out of b). The delay covers the writer window, the flushing fence's upload wait (up to 3 minutes), detection and the bounded 30 s drain. Write the lost markers L on a with the §37 background writer (its commits are part of L), stop the writer, and archive L with a flushing fence. Then write a second lost batch L' (marker L3, with the writer) and do **not** flush it: it stays in a's active binlog, and the scenario records that file's name and a's manifest rows. Scale a to 0. b is promoted without L and L'; the scenario computes the lost archived set L as a's archived set minus b's `@@gtid_executed` and fails if it is empty. While a is down the archive knows nothing of L'. Every `ReadOnly` reader site replicates straight from a with no delay, so it applied L: the operator refuses to repoint it (`SourceConvergenceBlocked`) and the replica GTID audit reports it `RecoveryBlocked` with L. The scenario reclones each reader that holds any of L from b, confirmed by the divergent GTID's prefix. Left blocked, the reader stays without replication for the rest of the run, and the group never returns to a healthy baseline.
2. A latest verification refuses `PITRArchiveDivergentHistory`, and the message prints exactly the lost set (compared as GTID sets, not just a's UUID). With `excludeGtids` = the lost set it is Verified (`ArchiveHead`, a's archived set minus the lost set, which replays era P). `excludedDivergentTransactionCount` is not asserted here: it counts the timestamp-mode D window, which latest mode does not compute.
3. b writes S (fence TS, with the writer) and flushes. A timestamp verification of β1 at TS refuses `PITRArchiveDivergentHistory` printing exactly the lost set; with `excludeGtids` it is Verified exactly and reports `excludedDivergentTransactionCount` = |lost set|. Take β2; b writes S2 (fence, with the writer) and flushes; a verification of β2 **without exclusions** is Verified exactly.
4. Scale a back up and wait until it is observed read-only. Its restart sealed the binary log that holds L', a's latest archive record is still its own promotion record, and its server is the same incarnation, so its sidecar uploads that file while a is read-only: the scenario waits, up to 3 minutes, for a new row for that file in a's manifest and `tail.state=Drained` from a scan that saw the file sealed. It computes L ∪ L' as a's `@@gtid_executed` minus b's and fails if nothing beyond L is in it. The step-3 timestamp verification of β1 at TS, run again, now refuses `PITRArchiveDivergentHistory` printing exactly L ∪ L', and with `excludeGtids` = L ∪ L' it is Verified exactly with `excludedDivergentTransactionCount` = |L ∪ L'|. Before #264 the reclone wiped L' unseen and such a restore planned on the surviving history without a word.
5. Wait until a is `RecoveryBlocked` with a recorded `divergentGtid`, and reclone it with that GTID's prefix as the confirmation; the reclone asserts the operator's archive-tail step as in §68 (INFO `pre-reclone archive tail archived` for a, no `PITRArchiveTailNotArchived` Event; a's tail is already in the archive, which the operator confirms after its own rotation) and that a came back with a new `@@server_uuid` and no `CloneServerUUIDNotRotated` Event (#265). The reader reclones of step 1 assert the same. Wait, up to 5 minutes, until a's sidecar reports its archived lost rows as divergent evidence: `divergentRows` > 0 (a's own manifest still holds the rows with L and L', which its recloned server no longer has), `reissuableGtids` reported and empty (they were issued under the `server_uuid` a no longer has), no `quarantinedRows` and an empty `lastError`. A non-empty `reissuableGtids` fails the step at once. Then wait two operator archiver-poll intervals (the operator's reissue check reads its cached poll, and it publishes no suspect-row count), requiring that `status.pitr.message` reports no failed poll of a and that a still reports the rows. The failback waits until a is replicating and no longer `RecoveryBlocked`: only the replica GTID audit's next clean proof clears the block, and a planned failover refuses a blocked target (`DivergentTarget`). It then waits for a to be the primary on this run's prefix and archive ID with a promotion record after the request (read on a's server clock) and no archiver `lastError`. For 15 seconds after the failback succeeded the group must have no `PITRArchiveReissueRiskDetected` Event naming a.
6. a writes R (fence TR, with the writer) and flushes; the upload must be clean (no backlog, no archiver error). a's `@@gtid_executed` then holds none of L ∪ L' and holds `<a's new server_uuid>:1`: the new era is numbered under the new identity, and no lost GTID exists on a again. `PITRArchiveReady` is `True`. A timestamp verification of β1 at TR refuses `PITRArchiveDivergentHistory` printing exactly L ∪ L' (the lost set is still divergent history inside the old backup's window); with `excludeGtids` = L ∪ L' it is Verified exactly, across a's old era, b's era and a's new era, with `excludedDivergentTransactionCount` = |L ∪ L'|. A verification of β2 at TR **without exclusions** is Verified exactly: every lost transaction was committed before β2's newest one, so none is in its window. Nothing refuses `PITRArchiveHistoryReset`.

**Depends on the divergence audit change that ships with #265**: steps 5 and 6 require that an archived lost tail is not an archiver error after a reclone (an empty `lastError` on a, a clean upload of a's new era, `PITRArchiveReady=True`), that the sidecar reports `reissuableGtids`, and that the operator's reissue warning follows that verdict instead of the row count. With a sidecar or operator from before that change the scenario fails in step 5.

**No longer covered on a cluster**: `PITRArchiveReissueRiskDetected`, `PITRArchiveHistoryReset` and the new-prefix remedy, which this scenario pinned until #265. They remain for a server that lost history and kept its `server_uuid`; the sidecar's, the planner's and the controller's tests cover them. The switch to a new prefix is still exercised by §69.

**Timing**: 75-minute timeout. **Cleanup** (32-minute budget plus 12 minutes for the repair reclone): if the run got as far as applying the delay, clears b's `SOURCE_DELAY` only while b is still a read-only replica whose channel carries a delay (judged from b's live state, so a partly applied delay is cleared and a promoted b is not touched), reclones any `RecoveryBlocked` site (with PITR still enabled each such reclone first waits, at most 2 minutes, for the site's tail), then as §37. **Status**: written and compiled; first live runs in the e2e `pitr` job on the #206 PR (see §37). The #264 changes (L', step 4 and the reclones' tail assertions) and the #265 changes (steps 5 and 6) have not run on a cluster yet.

### 71. Old Primary Lost Its Own Transactions {#71-old-primary-lost-own-transactions}
**Category**: Old-primary recovery | **Risk**: High | **Automated**: `71-old-primary-lost-own-transactions` (full profile, `ResetBeforeRunAll`)

**Hypothesis**: When the old primary comes back after a failover lacking transactions it issued itself that the new primary holds, the operator does not rejoin it by replication. It reports `OwnTransactionsLost` with the exact set (`status.sites[].recoveryState: RecloneRequired`, `lostGtid`), reclones the site without a request, and the site ends as a healthy replica under a new `server_uuid` that holds the lost transactions again.

**Injection**: Write ten single-row transactions on the primary, wait for the replica to hold them, scale the primary to 0 and wait for the failover. The new primary writes three rows of its own. The set the old primary is to lose is then chosen among the transactions it issued that the new primary holds, and its GTID state is reduced to what the new primary holds minus that set: a counter-app write that reached the old primary only, between the catch-up wait and the scale-down, is left out like the lost tail (with it the site would come back divergent, which is another case). Then scale the operator to 0, scale the old primary back up and, on the read-only server, run `RESET BINARY LOGS AND GTIDS` and set `gtid_purged` to its former set minus its newest three own transactions. Scale the operator back to 1.

A real cluster loses such a tail with a volume that comes back older. The playground cannot swap a volume's contents under a stopped pod, so the scenario gives the site the GTID state of the older volume instead. The rows of the three transactions stay in its tables, which an older volume would not have; the operator decides on GTID sets alone, and the reclone replaces the site's data either way.

**Verify**:

- An `OwnTransactionsLost` Warning names the old primary and counts three transactions.
- The site reaches `recoveryState` empty with `lostGtid` empty, replicating from the new primary, while the active site does not move.
- Its `server_uuid` differs from the one it had (it was cloned), an `OwnTransactionsLostResolved` Event exists, and no `OldPrimaryRecovered` Event names it (that Event announces a rejoin by replication).
- `GTID_SUBTRACT(<lost set>, @@gtid_executed)` is empty on the site, and the marker table has 13 rows on both sites.

**Status**: written and compiled, not yet run on a live cluster. It is in the full profile only; move it to the release profile after its first green run.

### 72. PITR With an Encrypted Binary Log {#72-pitr-encrypted-binlog}
**Category**: PITR, encryption at rest | **Risk**: Medium | **Automated**: `72-pitr-encrypted-binlog` (quarantined: needs the TLS + `spec.encryptionAtRest` baseline, run by the encryption jobs)

**Hypothesis**: With `spec.encryptionAtRest` MySQL encrypts the binary log (`binlog_encryption=ON`). The sidecar archives the plaintext of those files, read with the key in the site's keyring, and reports no archiver error. A pinned `MysqlBackupVerification` then restores a full dump and replays the archived binlogs to a timestamp with `mysqlbinlog`, which reads plaintext only: the pre-target marker rows are there and the post-target rows are not.

**Injection**: The steps of scenario 31 (RustFS profile with PITR, a baseline row, a full backup, marker rows around a target timestamp, a forced rotation) on the encryption baseline (`./playground/enable-encryption.sh`).

**Verify**:

- `@@global.binlog_encryption` is 1 on the active site and the replica, before PITR is enabled and after the archive was forced (enabling PITR rolls every site).
- The archiver of the active site becomes ready and covers the target timestamp (scenario 31's checks).
- The verification succeeds with the marker rows scenario 31 expects.

Before the archiver could decrypt, this stopped at the archiver's readiness: nothing was archived and `PITRArchiveReady` was `False` with `ArchiverError`.

**Status**: passed once on a four-node kind cluster (2026-11-12, about 5 minutes).

### 73. Paced Rollout: a Bad Build Halts at the First Group {#73-paced-rollout-bad-build-halt}
**Category**: Upgrade, paced rollout (#244, `proposals/23-paced-rollouts.md`) | **Risk**: Medium | **Automated**: `73-paced-rollout-bad-build-halt` (release profile; the e2e `paced-rollout` job)

**Hypothesis**: When an operator release changes the pod template of two groups, the pacer admits one and the other keeps its pods (`status.rollout.phase=Pending`). A release whose pods cannot become Ready stalls the admitted group (`Stalled`) before its primary is rolled or handed over, and the waiting group stays untouched (`Pending`/`RolloutHalted`), across an operator restart too. After a release that works, the first group completes and only then the second is admitted and rolled. A group that pins its sidecar image is not rolled by a change of the default.

**Setup**: The playground has one group, so the scenario creates two auxiliary groups for its duration (`paced-a`, `paced-b`: two sites each, no Dragonfly, 256Mi requests, and a `taintNodeSelector` that matches no node, so they taint nothing) and deletes them in cleanup. Both sites of an auxiliary group run in the zone of the playground group's active site: the operator taints the nodes of the playground group's other sites (`NoExecute`), and a pod of another group does not tolerate that taint. For the same reason the playground group must not fail over during the run, so the scenario pins its `spec.sidecar.image` to the image it already runs. The effective image does not change, so the pin restarts nothing, and it keeps the release from rolling the group (an ordered update ends in a handoff).

**Injection**: A release is a change of the operator's default sidecar image (`BLOODRAVEN_SIDECAR_IMAGE` on the operator Deployment), which changes the renderer identity and the spec hash of every group that does not pin `spec.sidecar.image`.

1. Bad release: the default becomes `bloodraven-sidecar:playground-s73-missing`, a tag no node or registry has, with `BLOODRAVEN_ROLLOUT_STALL_AFTER=45s` and `BLOODRAVEN_ROLLOUT_SOAK=10s`.
2. The operator pod is killed while the rollout is halted.
3. Good release: the default becomes `bloodraven-sidecar:playground-next`, the same sidecar build under a second tag (`setup.sh`, `rebuild.sh sidecar` and `make e2e-images` tag and load it), with `stallAfter` back at 30m.

**Verify**:

- Exactly one auxiliary group gets `status.rollout.startedTime`; the other reports `Pending` with the pods it had (same UIDs, no container restarts).
- The first group reports `Stalled`. Its follower Deployment carries the bad image and that pod is not Ready; its active site, its primary pod and its primary Deployment (renderer, hash, image) are what they were. The second group reports `Pending`/`RolloutHalted` with its pods and Deployments unchanged.
- After the operator restart, the new process reports `bloodraven_rollout_groups{phase="stalled"} 1` and `{phase="pending"} >= 1`, and the same checks hold for 30s more; `admittedTime`, `startedTime` and `stalledTime` of the stalled record did not change.
- After the good release the first group completes on the admission that stalled (same `admittedTime` and `startedTime`), then the second is admitted and completes.
- Over samples taken once a second from the bad release to the end: never more than one group with an unfinished admission; a group without an admission always runs the pods and Deployments it started with; the second group's pods changed only in samples where the first shows `completedTime`; `admittedTime` of the second is not before `completedTime` of the first.
- At the end every site Deployment of both groups carries one new `rendered-by` value, a new spec hash and the good image, on new Ready pods, and both groups pass the shared baseline. The playground group's pods are the ones it started with.

**Cleanup**: Reads what to undo from the cluster, so it also undoes a run that was killed: it deletes the auxiliary groups, restores the operator's environment from the `playground.bloodraven.shipstream.io/paced-rollout-original-env` annotation on the operator Deployment, then removes the pin. On a failure the capture directory also holds the auxiliary groups (`paced-rollout-groups.yaml`), their pods, the sampled timeline and the log of the operator pod that runs.

**What it does not cover**: the stall is found by `stallAfter`, shortened to 45s; the ordered updater's own report (`OrderedUpdateStalled`, after its 5m wait for the rolled pod) is covered by `TestPacedRollout_BadBuildStopsAtTheFirstGroup`. `Recreate` groups and Dragonfly StatefulSets are covered by the controller tests only.

**Status**: not run on a cluster yet (written 2026-11-12 on a host where no local cluster could start). Its checks, its cleanup and the objects it creates are tested without one (`paced_rollout_*_test.go`, and `TestPacedRolloutScenarioObjectsAdmitted` in envtest). The e2e `paced-rollout` job is its first run; expected runtime about 10 to 12 minutes, unmeasured.

### 74. Upgrade From the Previous Release Is Paced {#74-upgrade-paced-rollout}
**Category**: Upgrade, paced rollout (#244; C-8 in `proposals/20-api-v1beta1.md`) | **Risk**: Medium | **Automated**: `74-upgrade-paced-rollout` (quarantined: needs a cluster installed at v1.7.0 with a spare node, run by the e2e `upgrade` job)

**Hypothesis**: Upgrading the operator from v1.7.0 changes the pod template of every failover group (`deployment-pod-render-v8-mysql-launcher`), and nothing v1.7.0 rendered carries the `rendered-by` annotation, so every MySQL pod of every group rolls, one group at a time.

**Setup**: `./playground/upgrade-e2e.sh install` installs the published v1.7.0 chart (from the chart repository) with `ghcr.io/shipstream/bloodraven:1.7.0` and `bloodraven-sidecar:1.7.0`, and the playground group and apps as `setup.sh` does. The scenario adds the two auxiliary groups of scenario 73, under the old operator. Here the playground group rolls too and hands off, so the auxiliary groups run on a spare node, one that carries none of the playground group's labels (a fourth worker: `.github/kind/e2e-upgrade.yaml`, or `k3d cluster create bloodraven --agents 4`).

**Injection**: `./playground/upgrade-e2e.sh upgrade`: this checkout's CRDs are applied first (Helm does not upgrade `crds/`, and without them the API server prunes `status.rollout`), then `helm upgrade --reuse-values` to this checkout's chart with the operator image and `sidecar.image.{repository,tag}` set to the build, and `rollout.soak=10s`. `--reuse-values` is the upgrade the operations guide shows; it renders the new templates over values that have no `rollout` key.

**Verify** (samples once a second from before the upgrade until every group completed):

- Never more than one group with an unfinished admission.
- A group without an admission runs the pods and site Deployments it had before the upgrade.
- A group's pods changed only in samples where every group admitted before it shows `completedTime`; each group's `admittedTime` is not before its predecessor's `completedTime`; the order the samples saw the admissions in is the order of `admittedTime`.
- At the end every site Deployment of the three groups carries one `rendered-by` value and a new spec hash, on a new Ready pod, and every group passes the shared baseline.
- The job then runs `01-clean-primary-kill`: one failover works on the upgraded build.

**Cleanup**: Deletes the auxiliary groups and the spare node's zone label. The upgrade is not undone.

**Scope**: v1.7.0 is the last release without paced rollouts, and the precheck requires workloads without `rendered-by`. This is the gate for the release that introduces pacing, not a rolling check of "the previous release": an upgrade between two releases that both pace may change no pod template at all.

**Status**: not run on a cluster yet (written 2026-11-12). Checked without a cluster: the v1.7.0 chart and both images pull anonymously, the v1.7.0 chart renders with the values `setup.sh` passes, the checkout's chart renders over v1.7.0's values (`TestRolloutValuesChart`), and the auxiliary groups are admitted by the published v1.7.0 CRD and survive this build's CRD being applied over it, after which `status.rollout` is stored (`TestPacedRolloutScenarioObjectsAcrossTheUpgradeFromV170`, envtest). The e2e `upgrade` job is its first run.

### 75. Full-Instance Restore: Sessions, Spec Removal, Reclones {#75-full-instance-restore-sessions-spec-removal}
**Category**: Backup/restore | **Risk**: High | **Automated**: `75-full-restore-three-site-sessions-spec-removal` (full profile, `ResetBeforeRunAll`)

**Hypothesis**: The three-site full-instance in-place restore PR #197 left without live coverage (#214). A restore from an encrypted backup on a PVC profile decrypts from the read-only claim and loads a read-only staging directory without a progress file. Its Fencing step ends application sessions held through the `-primary` Service and through the primary's per-site Service and refuses new ones while the restore runs. Removing `spec.restoreInPlace` while the Job runs keeps every guard until the Job finishes, and the run then ends `Succeeded`. Re-applying the same confirm starts nothing. Every peer is recloned and holds the backup's data.

**Setup**: An encrypted profile (`AES-256-GCM`, a passphrase Secret of the run) on an operator-provisioned claim of `playground-fast`. The claim is bound on the primary's node by a short-lived pod first: the local-path helper pod does not tolerate the operator's `db-readonly` taint, so a claim first consumed on a read-only site's node is never provisioned. Marker row 1 is in the backup, row 2 is written after it and reaches both peers. An application account `s75app` with table grants only (no `CONNECTION_ADMIN`/`SUPER`, checked) holds `SELECT SLEEP(86400)` sessions from a pod in the cluster through `mysql-playground-primary` and `mysql-playground-<primary>`, and probes a new connection through each every two seconds.

**Injection**: An operator-account session (the fence keeps the operator's own sessions, by contract) opens a transaction that reads the marker table, then the restore is requested. The Job blocks in `DROP DATABASE chaos_s75_marker` on that metadata lock, which makes the window for removing the spec deterministic. With the Job held there, `spec.restoreInPlace` is removed; after the guard window the transaction is committed.

**Verify**:

- While the Job is held: `status.restoreOfflineFence` names the primary and the run, records `priorOfflineMode=false` and `sessionsEvicted`; `RestoreOfflineFenceApplied`; `offline_mode=ON`; no `s75app` session on the primary and both held sessions ended with an error after the request; new connections fail through `-primary` (no endpoint) and with error 3032 through the per-site Service; `status.restoreReclones` is `Armed` with both peers (the reader last) and both peers' receivers are `OFF`. The Job's claim volume and every mount of it are read-only, `decrypt-download` reads it, the loader reads `/restore-decrypted` read-only and `BLOODRAVEN_LOAD_PROGRESS_FILE` is set and empty.
- After the spec removal: `RestoreInPlaceSpecRemoved`, and for 40 seconds the run stays `Restoring` on the same Job and `runID` with the removal in its message, the fence record, `offline_mode=ON`, the empty `-primary` Service, the armed queue and stopped receivers stay, `activeSite` does not move, `bloodraven_failovers_total` to the other candidate does not grow, new connections are still refused, and no `RestoreInPlaceCanceled`, `RestoreInPlaceJobLost` or `RestoreOfflineFenceReleased` is emitted.
- After the commit: `Succeeded` with `confirmTokenUsed` the run's confirm, the same Job succeeded, `RestoreOfflineFenceReleased` (`OFF`), `offline_mode=OFF`, the primary holds row 1 only, and new connections succeed through both Services.
- The same confirm re-applied: for 45 seconds the terminal status is unchanged (same `startTime` and `completionTime`, no `runID` or `runConfirm`), no fence, queue or Job appears, and no `RestoreInPlaceStarted` is emitted.
- The queue drains: `RestoreRecloneSucceeded` for both peers and `RestoreReclonesDrained`; the group is `Ready` with one writable site, both peers replicate and hold row 1 only.

**Cleanup**: Commits the lock holder if it is still open, deletes the pods, removes the spec, waits for the run to end and the queue to drain (removes a `Held` queue), drops the marker schema and the account, deletes the backup, restores `spec.backup`, deletes the passphrase Secret and, if the run created it, the claim.

**Status**: passed twice in a row on a kind cluster with Calico on 2026-11-13 (`run-all`, each after a reset): 3m09s with `pdx` active and 2m23s with `iad` active. The DROP was held for about 55 seconds each time, and the queue recloned both peers in under 20 seconds. An earlier run failed on the scenario's own assertion: it expected the terminal status to keep `runID`, which the API clears once the run is terminal.

### 76. Blackholed Dragonfly Master During a MySQL Failover {#76-blackholed-dragonfly-master-during-a-mysql-failover}
**Category**: Dragonfly HA, emergency failover (#199, #214) | **Risk**: Medium | **Automated**: `76-dragonfly-blackhole-mysql-failover` (full profile)

**Hypothesis**: With one tagged-GTID transaction on both sites, blackholing the Dragonfly master's pod and scaling the MySQL primary on the same site to 0 makes the operator fail MySQL over to the peer, and the Dragonfly target is stamped master within 14s of the MySQL promotion, with REPLTAKEOVER skipped and REPLICAOF NO ONE sent. The new primary holds the tagged transaction. Both old masters rejoin as replicas once the faults are removed.

It covers two #199 fixes. Before #199, a takeover against a dead Dragonfly master used up the promotion's single context, and the REPLICAOF NO ONE fallback then ran on an expired context. And MySQL 8.4+ prints a GTID set with tagged transactions in a compact form (`uuid:1-15:chaos76:1`) that the failover picker could not parse, so no candidate could be chosen.

**Injection**:

1. Seed a key on the Dragonfly master and wait for it on the peer.
2. On the MySQL primary, commit one row under `SET gtid_next='AUTOMATIC:chaos76'` (root has `TRANSACTION_GTID_TAG`). Find its GTID by subtracting the `gtid_executed` read before it from the one read after it, using the operator's parser, and wait until the peer has executed it.
3. Apply a deny-all NetworkPolicy (ingress and egress) to only the master site's Dragonfly pod, then scale that site's MySQL Deployment to 0.

**Verify**:

- MySQL fails over to the peer. The operator logs `failover complete` for the peer about 5s after the fault.
- `status.dragonfly.activeSite` and `lastPromotionTarget` name the peer. `lastPromotionTime`, plus one second for its resolution, is within the bound of the `failover complete` line.
- The operator logged `dragonfly emergency: old master unreachable; skipping REPLTAKEOVER` and `... promoted via REPLICAOF NO ONE (sessions lost)`. A `REPLTAKEOVER succeeded` fails the run, because it means the blackhole was not in effect. `dragonfly_promotions_total{result="sessions_lost"}` and `failovers_total` for the peer increment, and `{result="success"}` does not.
- After `failover complete`, the operator promotes Dragonfly on no site other than the peer (see the second finding).
- The seeded key reads back on the new master, and the active Dragonfly Service selects only its pod.
- On the new primary, `GTID_SUBSET(<tagged GTID>, @@gtid_executed)` is 1, the operator's parser reads its `gtid_executed` and finds the tagged GTID in it, and the marker row is present.
- After the NetworkPolicy is removed and MySQL is scaled back, the old Dragonfly master rejoins as a replica with its link up. If it does not, its pod is force-deleted once, as in §39. The old MySQL primary then returns `ReadOnly` with replication running and no recovery state, while the active site stays on the peer.

**The bound**: The MySQL failover only queues the Dragonfly follow-up. Two promotions then race for the group's Dragonfly gate:

- **The queued follow-up.** It runs after the Tick in flight, whose observation of the blackholed master is bounded by the dial and I/O timeouts (3s + 5s).
- **The manager's own promotion.** It runs when the master has been missed `failureThreshold` times. Each miss costs a 3s dial timeout. The first may cost the 5s I/O timeout more, when a connection made just before the policy took effect waits for a reply. These misses start at the blackhole, before the MySQL promotion.

Either promotion then:

1. Probes the old master (1s).
2. Sends REPLICAOF NO ONE.
3. Dials the old master for CLIENT KILL (2s).
4. Stamps the status.

Allowing 3s for the calls that answer in milliseconds, the bound is 3s + 5s + 1s + 2s + 3s = 14s: the queued follow-up runs right after the Tick in flight, and when that Tick makes the manager's own promotion instead, it finishes no later. Before the queued request had priority (Finding 1) the bound was the own promotion's, 3×3s + 5s + 1s + 2s + 3s = 20s at the playground's `failureThreshold` of 3. `TestS76OperatorMirrors` pins the budgets and log messages it mirrors to the controller's source.

**Finding 1**: The follow-up is not always the promotion that runs. The Dragonfly manager's `Run` loop selects between its wake channel and its ticker. While the master is blackholed, every Tick lasts longer than the 2s poll interval, so both channels are ready when a Tick ends, and Go picks one at random. The queued request can lose to the Ticks until the failure threshold promotes the target, after which the request finds it promoted (`dragonfly promotion: target already promoted since the request; skipping`). The 20s bound held either way. Fixed with Finding 2: a request queued while a Tick ran now runs before the next Tick, so the follow-up takes at most one Tick, and the scenario holds it to 14s.

**Finding 2 (operator defect, fixed)**: In 1 of 11 runs, the manager's own promotion moved the Dragonfly master to the reader site, after MySQL had already failed over to pdx. In that run:

1. One observation of pdx's Dragonfly failed with a DNS lookup timeout (`lookup playground-dragonfly-pdx…: i/o timeout`). The master's third miss came in that same Tick.
2. `dragonflyOnlyPromotionCandidate` chose among the replicas it had observed, which was only the reader. It did not consider the MySQL active site or the follow-up already queued for pdx. It promoted the reader with REPLICAOF NO ONE and gave it the master and traffic labels.
3. The queued follow-up ran 3s later and promoted pdx in the same way. It demoted only its `oldSource` (iad), so the reader and pdx both carried `role=master` and traffic for about 1s, until the next Tick found the reader to be a stale master and re-attached it to pdx. Anything written to the reader in those 4s was discarded.

The scenario fails on any promotion of another site after `failover complete`. Fixed in the same change as the scenario: while a MySQL-follow promotion is queued, a Dragonfly-only promotion (or the restarted-master path) promotes only the request's target, and none while it was not observed (`TestDragonflyOnlyPromotionCandidate_FollowsMySQLOnceItMoved`).

**Timing**: 6-minute timeout. A run takes 25–45s, plus about 15s of cleanup. **Cleanup**: The settle step removes the NetworkPolicy and scales MySQL back. Cleanup then reclones any site left `RecoveryBlocked` and drops `chaos_s76`. The tagged GTIDs stay in `gtid_executed`; a rerun commits the next one (`chaos76:2`).

**Status**: Run 11 times on a four-node kind cluster with Calico (2026-11-13). All 11 runs met the hypothesis's checks. One run (the reader promotion of Finding 2) would fail the check that was added after it; the last six runs, on the final code, all passed.

| Promotion that ran | Runs | Stamp delays after `failover complete` |
| --- | --- | --- |
| Queued follow-up | 6 | 4.6–6.3s; 13.6s in the reader run |
| Manager's own promotion | 5 | 7.7–9.5s |

Every run skipped REPLTAKEOVER. The picker's log line showed the tagged set parsed (`…:1-15,…:chaos76:1`).

### 77. PITR Archive Storage Down at Sidecar Start {#77-pitr-archive-store-init-failure}
**Category**: PITR, degraded start (#214 item 5, PR #200) | **Risk**: Medium | **Automated**: `77-pitr-archive-store-init-failure` (full profile)

**Hypothesis**: Since v1.6.0 a sidecar whose PITR archive store cannot be reached keeps serving readiness, the startup safety net and self-fencing, and retries; it no longer exits. Enabling PITR while the storage is down therefore costs archiving only: the ordered update finishes, every pod stays Ready, nothing fails over, and the operator reports `PITRArchiveReady=False/ArchiveStoreInitFailed`. When the storage returns, the running sidecars create the archive without a pod restart.

**Injection**: Create the RustFS bucket and a passphrase Secret for this run (its bytes are stashed). Scale RustFS to 0 and wait until no RustFS pod exists. Enable PITR with an encrypted profile (`AES-256-GCM`) on a new prefix `e2e/77-…/<run-stamp>`. With PITR off before, no writer runs, so the operator renders the profile without a check Job and rolls every site onto sidecars that start against dead storage.

**Verify**:

1. The ordered update finishes with RustFS still at 0: `Ready=True` and `Updating=False/NotUpdating` at the patched generation, every site Deployment renders PITR on this destination and has rolled out, and every MySQL pod (reader included) is Running and Ready with its `mysql` and `sidecar` containers ready.
2. Every site's `/archiver/status` (reader sites archive too) reports `enabled: true`, `storeReady: false`, `lastError` starting with `archive store init: `, the S3 store, this prefix's `binlogs`, `primary` true on the active site only, `uploadFailures` > 0, and an `archive` block with `encryptionMode: Passphrase`, `state: AwaitingDescriptor` and no archive ID. `PITRArchiveReady` is `False/ArchiveStoreInitFailed`, and both its message and `status.pitr.message` hold `<site>: archive store init: ` for every site. A Warning `PITRArchiveNotReady` with `(ArchiveStoreInitFailed)` exists since the patch. Each sidecar's `uploadFailures` rises again: it retries.
3. For 75 seconds (two operator polls, a dozen archiver scans), checked every 5 seconds: the active site and `status.lastFailover` do not change, the group stays `Ready`, the condition stays `False/ArchiveStoreInitFailed`, no pod is replaced and no container restarts, and every sidecar answers `/health` 200 and `/status` with `self_fenced: false`. Then the primary is writable (`read_only` and `super_read_only` off) and takes a marker row. The operator never logged `initiating failover` (the emergency path; the ordered update's handoff does not log it).
4. Scale RustFS back to 1. Without a pod restart (the pods and restart counts from step 3 are unchanged): the binding of the prefix becomes `Bound` with a Passphrase key snapshot, `status.pitr.archive` verifies the same archive, every site's archiver reports a ready store, no error, and the archive `Verified` with that archive ID and the snapshot as `keyRevision`. `PITRArchiveReady` returns to `True/Archiving` and a Normal `PITRArchiveRecovered` Event is recorded.
5. Insert a second marker, `FLUSH BINARY LOGS` on the primary, and wait for a new row of the sealed file in its manifest (no backlog, no error). The archived object starts with the `BRV1` magic, decrypts with the stashed passphrase bytes, and the plaintext starts with the binary log magic `\xfebin`. Still no emergency failover and no pod restart.

**Not covered here**: the store construction failures of the same contract (an unreadable passphrase or credentials file, an invalid endpoint, a PVC mount path that is not a mount point or not writable). They report the same reason through the sidecar's construction retry (log `failed to init archive store`, then `archive store initialized after retries`); unreachable S3 storage instead fails the archive session that the archiver opens on every scan, which logs only at debug level, so neither line appears here.

**Timing**: 30-minute timeout; the bounded waits add up to more than a run takes. **Cleanup** (32-minute budget): the revert scales RustFS back to 1 if a step failed first; then as §37 (restores the original `spec.backup`, which rolls every site again, deletes the passphrase Secret, releases the run's binding and deletes its prefix).

**Status**: passed twice in a row on a four-node kind cluster (2026-11-13): the steps took 5m05s and 4m49s, about 8 minutes each with the cleanup; the rollout with RustFS down took about 2.5 minutes. Observed during the rollout, not asserted: once the first site had rolled, sites whose ordered update was still pending (the old primary; in one run the reader too) were listed in the condition message and the `PITRArchiveNotReady` Event as "sidecar reports PITR archiving disabled ... check that spec.backup.pitr.profileName names an existing profile" (`ArchiverDisabled` detail) for about a minute, until they rolled. The documented state for a site whose update is pending is `AwaitingRollout`. The reason stayed `ArchiveStoreInitFailed`, which outranks it.

### Recorded, not automated

- **Write stall is invisible (F9)**: `FLUSH TABLES WITH READ LOCK` held on the primary for 75s stalled every write for 75s, with no state change, condition, event or alert. Health is `SELECT @@read_only`, and nothing measures whether a write can commit. A write-heartbeat probe, metric and condition is the proposal.
- **mysqld starts writable**: a restarted primary pod accepts writes from the moment mysqld listens until the sidecar's safety net or the operator fences it. Observed while rewriting scenario 09: iad came back writable next to the active pdx, and the operator fenced it at the first observation (`SPLIT BRAIN: 2 sites are writable`) about 4s after the pod started. **Addressed**: every mysqld now starts with `--read-only=ON`, so a restarted pod refuses application writes and the operator never observes it writable; the operator re-asserts a primary that restarted read-only (`authority=lastActiveSite` when no failover is recorded), and the sidecar's safety net still super-fences a non-active site. `super_read_only=ON` was the proposal but is not used: the MySQL image's first-boot init runs its SQL as root through a server started with the same flags and fails with `ERROR 1290`. Scenario 40 (reader data loss) tripped on this: the replaced reader read writable for about 2s and blank `activeSite`.

---

## Execution Plan

1. **Setup**: `k3d cluster create` + `./playground/setup.sh`
2. **Run scenarios 1-10 first** (core failure modes, each builds confidence for later ones)
3. **Run 11-18** (advanced, may need reset between scenarios)
4. **Run 19 immediately after scenario 8** (scenario 8 leaves behind the divergent state it needs as a prerequisite — don't reset in between)
5. **Run 20-21 after a clean reset** (placement selector and `NoExecute` eviction semantics)
6. **Run 22-23 anywhere they fit** (CR-status regressions; both expect a healthy baseline and clean up after themselves)
7. **Between scenarios**: `./playground/chaos.sh status` to confirm clean state; `./playground/reset-mysql.sh` if topology is broken
8. **For each scenario**: Document actual vs. expected, note timing and any bugs
9. **After code fixes**: `./playground/rebuild.sh operator` (or `sidecar`), then re-run affected scenario
