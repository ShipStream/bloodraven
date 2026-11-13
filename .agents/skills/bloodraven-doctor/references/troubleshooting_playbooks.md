# Troubleshooting Playbooks for Bloodraven Doctor

Step-by-step diagnostic and remediation playbooks for common Bloodraven failure modes.

---

## 1. Split-Brain / Dual-Writable Recovery

### Symptoms
- `BloodravenSplitBrainDetected` alert firing.
- Multiple sites report `.readOnly=false`.
- Applications write to both sites simultaneously.

### Diagnostic Steps
1. Identify all writable sites:
   ```bash
   kubectl get mfg <group> -n <ns> -o jsonpath='{range .status.sites[*]}{.name}{": readOnly="}{.readOnly}{", reachable="}{.reachable}{"\n"}{end}'
   ```
2. Determine which site has the latest authoritative writes and highest GTID.
3. Check DNS routing and that the published name matches the spec:
   ```bash
   kubectl get mfg <group> -n <ns> -o jsonpath='{.spec.dns.hostname}{"\n"}'
   kubectl get dnsendpoint -n <ns> -o jsonpath='{range .items[*]}{.spec.endpoints[0].dnsName}{" -> "}{.spec.endpoints[0].targets[0]}{"\n"}{end}'
   ```

### Safe Remediation Steps
1. **Immediate Fencing**: Immediately set `read_only=ON` and `super_read_only=ON` on the non-authoritative site:
   ```bash
   kubectl exec -n <ns> <loser-pod> -c mysql -- mysql -u root -e "SET GLOBAL super_read_only=ON; SET GLOBAL read_only=ON;"
   ```
2. **Confirm Single Primary**:
   Ensure only the winning site is writable.
3. **Reconcile Divergent Data**:
   - Compare GTIDs using `./.agents/skills/bloodraven-doctor/scripts/gtid-audit.sh -n <ns> <group>`.
   - If divergent transactions exist on the loser, evaluate if they need manual data extraction (via `mysqldump` or binlog inspection).
4. **Reclone Loser**:
   ```bash
   kubectl bloodraven reclone <group> <loser-site> -n <ns>
   ```

---

## 2. Divergent Transactions & Errant GTID on Replica

### Symptoms
- Old primary fails to rejoin as replica after emergency failover (`status.sites[].divergenceDetectedBy=OldPrimaryRecovery`, Event `DataLossDetected`).
- `BloodravenDivergentTransactions` alert firing.
- `SHOW REPLICA STATUS` shows replication stopped with GTID error (e.g. error 1236).
- A replica that is still replicating normally can be divergent too (`divergenceDetectedBy=ReplicaAudit`): use playbook 33.

Before treating a site stuck in `RecoveryInProgress` (or an old primary that has not rejoined) as a stall, check whether a handoff owns the sites. Old-primary recovery, its connection drain and replication-source convergence stand down while a planned failover holds its claim (`.status.plannedFailover.phase` from `Fencing` through `Promoting`, or `RollingBack`), while an automation hold stands (`AutomaticFailoverHeld=True`, [playbook 38](#38-automation-held-after-a-planned-failover-automaticfailoverheld)), or while an ordered update is in flight (`.status.updatePhase`), and resume, with a fresh drain budget, once it ends. A planned failover that is only `Admitting` or `Deferred` owns nothing, and a Dragonfly snapshot upgrade does not defer recovery at all, so neither explains a stall. A planned failover waits in `Admitting` (`admission.waitReason: RecoveryInFlight`) for a recovery or convergence step that is already running; nothing is fenced while it waits, and if the step outlasts the 2-minute admission deadline the run ends `NotStarted` with `AdmissionTimeout`. After an operator config or credential change restarts a group's topology manager mid-update, the new manager takes no failover, re-assert, recovery or update until the old manager's ordered update exits, and a new planned failover waits in `Admitting` with `PredecessorBusy`. Meanwhile `status.updatePhase` is empty. The operator log brackets this window with `topology manager deferring to its predecessor's ordered update; no failover, recovery or update until it exits` and `predecessor's ordered update exited; topology manager resuming` (`heldFor`).

### Diagnostic Steps
1. Inspect GTID executed sets:
   ```bash
   ./.agents/skills/bloodraven-doctor/scripts/gtid-audit.sh -n <ns> <group>
   ```
2. Confirm active primary is healthy and taking production traffic.

### Safe Remediation Steps
1. **Explain the trade-off to the operator**:
   - Recloning will wipe the divergent replica and rebuild it from the active primary using MySQL Clone plugin.
   - Any transactions committed *only* to the old primary will be lost.
2. **Execute Reclone**:
   ```bash
   kubectl bloodraven reclone <group> <divergent-site> -n <ns>
   ```
3. **Monitor Reclone Progress**:
   ```bash
   kubectl get mfg <group> -n <ns> -w
   ```

---

## 3. Keyring & Encryption-at-Rest Troubleshooting

### Critical Alert: `KeyringEscrowMissing` or `KeyringEscrowCorrupt`
> [!CAUTION]
> **DO NOT RESTART THE MYSQL POD!**
> If the keyring escrow Secret in Kubernetes is deleted or corrupt while the pod is running with an in-memory or unsealed keyring, restarting the pod will cause total loss of the tablespace encryption master key, rendering the database files unrecoverable.

### Diagnostic Steps
1. Check sidecar keyring status (metadata and digests only; `/keyring/status` never returns key material):
   ```bash
   ./.agents/skills/bloodraven-doctor/scripts/sidecar-probe.sh -n <ns> <group> keyring
   ```
2. Find the site's escrow Secret through the CR, not by guessing its name. Escrow Secrets are named `mysql-<group>-<site>-keyring-v<N>` (immutable, one per keyring version):
   ```bash
   SECRET=$(kubectl get mysqlfailovergroup <group> -n <ns> \
     -o jsonpath='{.status.encryptionAtRest.sites[?(@.name=="<site>")].keyringSecret}')
   VERSION=$(kubectl get mysqlfailovergroup <group> -n <ns> \
     -o jsonpath='{.status.encryptionAtRest.sites[?(@.name=="<site>")].keyringVersion}')
   DIGEST=$(kubectl get mysqlfailovergroup <group> -n <ns> \
     -o jsonpath='{.status.encryptionAtRest.sites[?(@.name=="<site>")].keyringDigest}')
   kubectl get secret -n <ns> "$SECRET"
   kubectl get secret -n <ns> -l bloodraven.shipstream.io/failover-group=<group>,app.kubernetes.io/name=mysql-keyring
   ```

### Safe Remediation Steps
The only surviving copy of a sealed site's keys is the keyring file in its running pod's `mysql` container (`$KEYRING_DIR/keyring`; `KEYRING_DIR` is `spec.encryptionAtRest.keyring.dataFileDir`, default `/run/mysql-keyring`). Do not delete, restart, evict or drain that pod, and do not force an unseal or rollout.

1. `KeyringEscrowMissing` (the Secret is gone): recreate it from the running pod with the helper. It runs only on a site that is `Sealed` or `Failed` with no `unsealReason` (a rotation, bootstrap or clone in progress owns its Secret). It reads the pod's keyring and checks it against `status…keyringDigest` before writing anything, refuses if the Secret exists, never deletes a Secret, and re-reads the group before writing, stopping if its UID or the site's phase, unseal reason, Secret, version or digest changed. It then adds the labels (including `bloodraven.shipstream.io/keyring-version`), the digest annotation, the ownerReference and `immutable: true`. It never writes the group's status: the operator returns the site to `Sealed` itself once MySQL confirms it runs that keyring (Event `KeyringRecovered`), and the script waits up to `KEYRING_RECOVER_WAIT` seconds (default 120) to report it:
   ```bash
   ./.agents/skills/bloodraven-doctor/scripts/keyring-escrow-recover.sh -n <ns> <group> <site>
   ```
   It reads from the site's one Running, Ready MySQL pod, selected as its Deployment selects it, and refuses if there is not exactly one; `--pod <name>` names it explicitly, and also accepts a terminating pod (with a warning): with Recreate, a pod in its grace period may hold the last copy, so read it before the grace period ends. It runs with GNU or macOS/BSD tools (`sha256sum` or `shasum`).
2. `KeyringEscrowCorrupt` (the Secret exists but no longer hashes to the recorded digest): the helper refuses by default and changes nothing. Replacing the Secret is a separate, explicit step: re-run it with `--replace-corrupt <secret-name>`, repeating the exact Secret name. It then confirms the existing Secret really fails the digest check and the pod's keyring passes it, writes a backup to `./keyring-escrow-backup-<secret>-<timestamp>.yaml`, and only then deletes and recreates that one Secret. Get the human operator's go-ahead before passing `--replace-corrupt`.
   The canonical procedure is "Keyring escrow lost" in `site/content/docs/6.operations/4.runbooks.md`; "Disaster recovery" in `site/content/docs/4.configuration/8.encryption-at-rest.md` points to it. Neither, nor this helper, writes the site's phase: the operator re-seals it.
3. Verify the operator returns the site to `Sealed` (Event `KeyringRecovered`) and the `KeyringEscrowMissing` / `KeyringEscrowCorrupt` events stop. Do not patch the phase by hand: a rotation started meanwhile would be overwritten.

---

## 4. Dragonfly Cache & Session Degradation

### Symptoms
- `status.dragonfly.phase=Degraded` or `DragonflyPromotionFailed`.
- `<group>-dragonfly` service has 0 endpoints.

### Diagnostic Steps
1. Check Dragonfly pods and endpoints:
   ```bash
   kubectl get pods -n <ns> -l app.kubernetes.io/name=dragonfly
   kubectl get endpoints <group>-dragonfly -n <ns>
   ```
2. Check Dragonfly replication status:
   ```bash
   kubectl exec -n <ns> <dragonfly-pod> -- redis-cli -p 6379 info replication
   ```
3. After an emergency failover, find which promotion path ran: `kubectl logs -n bloodraven deploy/bloodraven | grep 'dragonfly emergency'`.
   - `REPLTAKEOVER succeeded`: sessions were preserved.
   - `old master unreachable; skipping REPLTAKEOVER`, or `REPLTAKEOVER failed; falling back`, followed by `target promoted via REPLICAOF NO ONE (sessions lost)`: the target is a writable master and keeps every key it held as a replica. What is lost is what the old master had written and the target had not yet received (nothing, when the replica was in sync when the master died). The same line follows `dragonfly: active master restarted without the cache; promoting the replica that holds it` (step 10).
   - `REPLTAKEOVER failed; not falling back to REPLICAOF NO ONE`: the takeover failed and the old master still answers as a master (or the target's sync was never proven by position: `reason` says which). The operator does not make the target a master with whatever it holds over a master that holds all of it. `promotion failed; target not observed as master` follows, with `REPLICAOF NO ONE not sent` in its `cause`; the old master keeps the role and its traffic. The cause is on the target: `ERR Full sync not done` means it is not in sync (step 11).
   - `REPLICAOF NO ONE failed` or `re-dial for REPLICAOF NO ONE failed`: an unconfirmed promotion, not the outcome. A takeover can complete on the server after its client gave up, and a timed-out fallback may still have applied. The operator waits for the commands to settle, reads the target's role and logs the result:
     - `promotion outcome unconfirmed; target observed as master`: recorded as promoted (labels, status), with `DragonflySessionsLost`.
     - `promotion failed; target not observed as master`: not promoted; the old source's traffic label was restored. The manager promotes again, 30s later at the earliest, if MySQL and Dragonfly still disagree and the target is proven in sync (step 11).
     If neither line follows within about 30s, confirm the target's role with `kubectl exec -n <ns> <target-dragonfly-pod> -- redis-cli -p 6379 info replication` and check the active Service's endpoints.
   - `dragonfly promotion: ... skipping` or `request dropped` (coalesced, manager stopping, yielding to another owner, planned failover in flight, target already promoted): the request was superseded or made unnecessary. Not a failure on its own.

   A fallback after a failed takeover can take up to about 14s, and settling an unconfirmed outcome up to about 13s more. Dragonfly holds the replica's `REPLICAOF` lock until the takeover's own wait ends, so a slow promotion is not by itself a failure.
4. A planned failover or rollout logging `dragonfly gate busy; retrying` is waiting for the group's Dragonfly gate, held by the Dragonfly manager's current poll or promotion, or by a stopping manager's last step (WARN `timed out waiting for the Dragonfly manager to stop; its last step finishes under the group's Dragonfly gate`). It requeues every second. If it repeats for minutes, capture a goroutine dump of the operator (the holder is stuck in a Dragonfly or API call) before restarting it. A planned failover waiting this way emits Warning `DragonflyPromotionDeferred` and sets `status.plannedFailover.dragonfly.message`; MySQL writes stay paused on the fenced source meanwhile (about 12-13s typically, about 40s worst case).
5. Snapshot upgrade `status.dragonfly.upgrade.phase=Failed` with `reason=ActiveSiteChanged`: MySQL failed over or a promotion moved the Dragonfly master off the site the upgrade retained, so the upgrade stopped without touching routing. Confirm `status.dragonfly.activeSite` and the active Service endpoints. The run is not restarted while `spec.dragonfly` still matches `status.dragonfly.upgrade.specDigest`; to retry against the new active site, change `spec.dragonfly`. Only when every site's StatefulSet runs the same image (`kubectl get sts -n <ns> -o custom-columns='NAME:.metadata.name,IMAGE:.spec.template.spec.containers[0].image' | grep -- -dragonfly-`) may you instead set `spec.dragonfly.image` back to that image (the `Failed` record is removed) and then forward again. With mixed images, setting it back starts a run toward the old image that downgrades the sites that moved: keep the target and fix `spec.dragonfly`, or switch `upgradeStrategy` to `Rolling`. The same applies to any other `Failed` reason once its cause is fixed.
6. A snapshot upgrade that never starts although `spec.dragonfly.image` differs from the sites' StatefulSet images: check `spec.dragonfly.upgradeStrategy` is `SnapshotRestore` (under `Rolling` the StatefulSets roll instead), then the `DragonflyUpgradeDeferred` Event and log `dragonfly snapshot-upgrade deferred` for the waiting cause (including a Dragonfly StatefulSet refused for `WorkloadSelectorMismatch`: playbook 44), and a `Failed` record whose `specDigest` still matches (step 5).
7. `status.dragonfly.phase=ConfiguringReplication` with `no master observed on active site <site>`: the operator could not reach the active site's Dragonfly (`dragonfly: site observation failed` carries the error). If the only site that reports master holds no keys, the recorded site keeps the role for 15s (`dragonfly: active site not observed; keeping its role`), the Dragonfly-only promotion of a replica is not made, and the empty instance is left as it is until the recorded site answers. After 15s, or at once if the other master holds keys or its key count cannot be read (`dragonfly: key count unavailable`), that master becomes `status.dragonfly.activeSite`.
   A healthy replica is not promoted on the first missed poll either: the operator logs `dragonfly: active master not observed; waiting before promoting a replica` once and promotes after `spec.failureThreshold` consecutive missed polls (default 3: about 4 to 6s when the dial is refused, 9 to 11s when it times out, 15 to 17s when the master accepts connections and does not answer). The misses must follow each other: a poll that reaches the master, or a gap of more than two poll intervals, starts the count again. A replica that reports `master_link_status:down` does not shorten the wait (a refused path between the pods gives the same answer for hours while the master is healthy). It does not wait when the active site answers as a replica. A master that comes back within the wait is step 10. A promotion that does not come after that: check that a replica is reachable, not in full sync and not loading (`info replication`, `info persistence`); with two such replicas (three sites) the one with the highest `slave_repl_offset` is promoted, and none when they report different `master_replid` values or no offset (`dragonfly: active master not observed and its replicas cannot be compared; not promoting any`); that no other site reports `role:master`, and that no planned failover, hold or snapshot upgrade owns Dragonfly.
8. A stale master that stays (`DragonflyStaleMasterDetected` without `DragonflyOldSiteReconfigured`): either a replica is attached to it, or it holds keys while the active site holds none, which the operator will not discard. (When that stale master is on the MySQL active site the operator makes it the active site instead: `dragonfly: recorded active site holds no keys; following the master on the MySQL active site`.) Compare `kubectl exec -n <ns> <dragonfly-pod> -- redis-cli -p 6379 info keyspace` on both sites before deciding. If the stale site holds the cache you want, the record is wrong: do not run `REPLICAOF` on it; the operator reattaches it by itself as soon as the active site holds a key.

9. A planned failover that ends with `dragonfly.reason: DragonflySyncTimeout` (or rolls back with it under `onSyncTimeout: Fail`), or a Dragonfly rollout that does not promote the replica: the target did not reach the source's replication position, or was not linked to it.
   - Planned failover: `status.plannedFailover.dragonfly.message` and the `DragonflySyncTimedOut` Event carry the reason (`target flow N at X behind source Y`, `target not in stable sync (...)`, `target syncs from replication id ...`, `source position unavailable`). A site that refused the position command (`NOPERM ... has no ACL permissions`, `unknown command`, `Unknown subcommand or wrong number of arguments`), or two sites that report positions in different forms, do not end in a timeout by themselves: with the target's link healthy the run went on to the takeover at once (Warning Event `DragonflySyncUnproven`, `dragonfly.message` and log `sync could not be proven by position; relying on REPLTAKEOVER`), and `onSyncTimeout` applied only if that takeover failed (`dragonfly.reason: DragonflyPromotionFailed`). A message that starts `the sync cannot be proven by position (...) and the target is not in stable sync` means the link checks did not hold either until `maxSyncWait`. Fix: let the operator's Dragonfly user run `DEBUG` and `DFLY`, or use a supported image.
   - Rollout: the operator logs the state when it changes, not on every pass: `dragonfly rollout: target not ready for promotion` with `state=NotLinked` (link down, full sync, loading) or `state=Behind`, or `dragonfly rollout: sync-readiness check failed` with `state=Unreadable` (a site did not answer). A refused or incomparable position does not hold the rollout: with the replica's link healthy it promotes with `REPLTAKEOVER` alone and says so once (`dragonfly: sync could not be proven by position; relying on REPLTAKEOVER`, Warning Event `DragonflySyncUnproven`); fix the operator's Dragonfly user all the same. `detail`/`error` carry the numbers. After two minutes without a ready target: Warning Event `DragonflyRolloutStalled` and log `dragonfly rollout: stalled waiting for the target`. `dragonfly rollout: target in sync` ends it. Under the gate, a target that fell out of sync again logs `dragonfly rollout: target no longer in sync; skipping`.
   - Rollout, other waits: the rollout is strict (one site at a time, the master's site last, and only after a successful `REPLTAKEOVER`). It never restarts the master where it is, however long it waits. `dragonfly rollout: waiting` gives the state when it changes, and `DragonflyRolloutStalled` repeats it after two minutes:
     - `MasterUnknown`: status names no Dragonfly master, the site it names cannot be read or does not answer as a master, or a second site answers as a master. No StatefulSet is changed. Compare `status.dragonfly.activeSite` with `info replication` on every site. A recreated pod that the manager has not attached yet clears by itself within seconds; a second master that stays is step 8.
     - `SiteInFlight`: a site the rollout restarted (StatefulSet annotation `bloodraven.shipstream.io/dragonfly-rollout-in-flight`) is not Ready on the new template or not back in sync. Look at that site's pod first: a new template that cannot start (image, args, resources) stops the rollout there, with the master untouched. Fix `spec.dragonfly`; the site is rolled again.
     - `TakeoverFailed`: `REPLTAKEOVER` from the active site keeps failing and is retried (1 s to 32 s backoff). The `DragonflyPromotionFailed` Events carry the server's error.
     - Do not remove the in-flight annotation by hand to get past a wait.
     - Only when the rollout can never proceed and losing the cache is acceptable: `kubectl annotate mysqlfailovergroup <name> -n <ns> bloodraven.shipstream.io/dragonfly-restart-in-place=<site>`. The operator applies that one site's template without a takeover, emits `DragonflyRestartedInPlace` and removes the annotation. If the site held the master it comes back empty and step 10 applies. Ask the user before setting it: it is destructive for the cache.
     ```bash
     kubectl logs -n bloodraven deploy/bloodraven | grep 'dragonfly rollout:'
     kubectl get events -n <ns> --field-selector reason=DragonflyRolloutStalled
     kubectl get statefulset -n <ns> -l app.kubernetes.io/name=dragonfly -o custom-columns='NAME:.metadata.name,IN_FLIGHT:.metadata.annotations.bloodraven\.shipstream\.io/dragonfly-rollout-in-flight,READY:.status.readyReplicas'
     ```

   The replica's own `info replication` can look healthy (`master_link_status:up`, `master_last_io_seconds_ago:0`) while it is behind; compare the positions the operator compares:
   ```bash
   # On the master: the journal position of each shard.
   kubectl exec -n <ns> <master-dragonfly-pod> -- redis-cli -p 6379 dfly replicaoffset
   # On the replica: the sync id, then the records each flow has executed.
   kubectl exec -n <ns> <replica-dragonfly-pod> -- redis-cli -p 6379 debug replica offset
   # The master's view of each replica: state and lag in journal records (0 to about 1000 is normal under load).
   kubectl exec -n <ns> <master-dragonfly-pod> -- redis-cli -p 6379 info replication
   ```
   Every replica flow must be at or past the master's shard, and both must print the same `master_replid`. A replica that stays behind on an idle master is stuck: `REPLICAOF <master-service> 6379` on the replica starts a new full sync (it discards what the replica holds, which the master has). `sourceOffsetState: Unavailable` means the source did not answer as a master or refused `DFLY REPLICAOFFSET`; check the Dragonfly image against the supported versions. All three commands are read-only.
10. The Dragonfly master's pod was recreated or its process restarted, and `status.dragonfly.activeSite` moved to the other site although MySQL did not fail over: expected. The restarted master is a new instance (a new `master_replid`; no keys, or a snapshot's). Its replica kept the cache, with `master_link_status:down` and the old `master_replid`, and re-attaching it would have emptied it. The operator promoted it instead:
    ```bash
    kubectl logs -n bloodraven deploy/bloodraven | grep -E 'active master restarted without the cache|promoted via REPLICAOF NO ONE|active-site drift'
    ```
    - `dragonfly: active master restarted without the cache; promoting the replica that holds it`, then `target promoted via REPLICAOF NO ONE (sessions lost)` (Event `DragonflySessionsLost`): the cache survived on the replica. Lost: what clients wrote to the restarted instance before the promotion (up to one poll interval).
    - About 30s later, once the restarted instance is a replica proven in sync: `dragonfly/mysql active-site drift: promoting Dragonfly replica to match MySQL` and `REPLTAKEOVER succeeded`. Dragonfly is back on the MySQL active site with nothing lost, and the other site's pod is recreated as a replica. Two promotions and two pod restarts in a minute are this sequence, not a flap. If the same site's master has to be replaced again, the way back waits longer each time (1, 2, 4, 8, at most 15 minutes: `dragonfly: active master was replaced repeatedly; delaying the take-back to the MySQL active site`); a master that keeps restarting is the thing to fix (`kubectl describe pod`, `kubectl logs --previous`), and the cache is safe on the other site meanwhile.
    - `dragonfly: restarted master is loading a snapshot; leaving its replica as it is`: the returned master reports `loading:1` (`info persistence`). Nothing is decided until the load ends; then one of the other outcomes follows.
    - Warning Event `DragonflyStaleReplicaNotPromoted` (log `dragonfly: replica of the restarted master holds a stale copy; not promoting it`): the operator had seen that replica with its link down while the master was still running, so its copy was as old as that state (the Event says how long) and could hold keys the application had deleted. It was re-attached and took the restarted master's content; with `0 keys` in the Event the cache started empty. Nothing to repair on the data; find why the replica could not reach the master (a NetworkPolicy between the sites, a firewall that rejects, `masterauth`) before the next restart, and alert on `status.dragonfly.sites[].linkStatus: down` beside a healthy master.
    - No promotion although the replica holds the cache and another site also reports `role:master` (`DragonflyStaleMasterDetected`): with a second master that may hold the cache the operator does not create a third. Step 8 applies; compare `info keyspace` on the sites.
    - If instead the replica was re-attached and came up empty (`dragonfly: configured replica` right after the master returned, no promotion): the operator had no evidence that the replica held more than the restarted master. That is the case when the operator itself restarted during the outage and the returned master already held keys, or when a key count or `master_replid` could not be read (`dragonfly: key count unavailable`). The cache is gone; nothing to repair.
    - Before the operator acts you can see the state yourself: `info replication` on the replica shows `master_link_status:down` and a `master_replid` that differs from the master's. Do not run `REPLICAOF <master>` on that replica: it discards the cache.
11. `status.dragonfly.activeSite` stays on another site than `status.activeSite` (after an emergency MySQL failover whose old site still runs a healthy Dragonfly, or after step 10): the manager takes the master over to the MySQL active site only with `REPLTAKEOVER`, and only to a replica proven in sync with the master (link up, same `master_replid`, the master's position reached on every flow). It never sends `REPLICAOF NO ONE` over a master that still answers as a master.
    ```bash
    kubectl logs -n bloodraven deploy/bloodraven | grep -E 'active-site drift|delaying the take-back|not falling back'
    kubectl get events -n <ns> --field-selector reason=DragonflyFollowStalled
    ```
    - `dragonfly/mysql active-site drift: target not in sync with the Dragonfly master; not taking over` with `state=NotLinked` (link down, full sync, loading, or another `master_replid`), `state=Behind` or `state=Unreadable`: logged when the state changes. Warning Event `DragonflyFollowStalled` after two minutes. The cache is complete where it is and clients reach it through the active Service; repair the replica's link (step 9 shows how to compare positions). The operator re-attaches a replica whose link is down on every poll.
    - `delaying the take-back`: step 10.
    - Do not promote the MySQL active site's Dragonfly by hand with `REPLICAOF NO ONE`: it keeps only what that replica holds, and the operator then re-attaches the old master to it.

### Safe Remediation Steps
1. Inspect if network policies are blocking port 6379 or Dragonfly admin port 9999.
2. Check if auth secret (`requirepass`) matches across sites.
3. If a stale master was isolated, allow Dragonfly to sync from the promoted site. Note: MySQL remains authoritative for all durable persistent application state.

---

## 5. Planned Failover Preflight Checklist

Before executing a planned failover with `kubectl bloodraven promote <group> <target-site> -n <ns>`:

- [ ] Target site is reachable and `status.sites[target].reachable == true`.
- [ ] Replication lag on target site is 0 seconds (`SecondsBehindSource == 0`).
- [ ] Target site has received all GTIDs from active primary.
- [ ] Target node has sufficient CPU/memory capacity and no scheduling taints.
- [ ] Every other `PrimaryCandidate` and `DisasterRecovery` site is reachable and read-only. A planned failover is strict: an unreadable one makes it wait (`admission.waitReason: PeerUnreadable`) and end `NotStarted/AdmissionTimeout` after 2 minutes. Readers do not block it.
- [ ] No `AutomaticFailoverHeld=True` condition: a standing hold refuses new requests (`ManualRecoveryPending`), and the deployment API reports it as `unstableReason: AutomaticFailoverHeld`.
- [ ] The target is not awaiting a post-restore reclone (`status.restoreReclones.pending`): it is refused with `TargetUnhealthy`, and a planned failover is not an override.
- [ ] No planned failover is in flight before an operator upgrade across the claim release: an older operator's in-flight block is refused, not resumed.
- [ ] Dragonfly replica is caught up and `status.dragonfly.replTakeoverSupported == true`.
- [ ] DNS provider / `external-dns` is healthy.
- [ ] No unexpired deployment `failover-hold` is active; otherwise expect `Deferred/DeploymentHold` and inspect the named operation, instance, and `admission.retryTime`.

## 6. Deployment lease lost or planned operation deferred

### Diagnostic steps

1. Run `bash ./.agents/skills/bloodraven-doctor/scripts/deployment-probe.sh <namespace> <group>`. An `unstableReason` of `RestoreReclonePending` means a full-instance in-place restore still has peers to reclone (or holds them after a failure); follow [playbook 20](#20-peers-not-yet-recloned-after-a-full-instance-in-place-restore). `AutomaticFailoverHeld` means a planned-failover automation hold stands (follow [playbook 38](#38-automation-held-after-a-planned-failover-automaticfailoverheld)); `PlannedFailover` means a run is in flight or holds its claim. `BootstrapFailed` means a failed clone's target does not replicate yet (`Degraded=True/BootstrapFailed`). Collect the deployment operation ID, grant generation, current `status.topologyGeneration`, lease expiry/state/reason, and planned-operation status. The helper projects public fields; never collect raw ownership hashes.
2. Inspect Events for `DeploymentLeaseRevoked` and `PlannedFailoverDeferred`, metrics `bloodraven_deploy_leases_active`, `bloodraven_deploy_lease_age_seconds`, `bloodraven_deploy_lease_revocations_total`, `bloodraven_deploy_lease_expirations_total`, and `bloodraven_planned_failovers_deferred_total`. A restart does not clear durable Leases.
3. Correlate `msg="deploy api"` logs by `group`, `instance`, `namespace`, and `operationId`. HTTP `status` and `duration_ms` identify authorization failures and slow requests without credential dumps. `401` means authentication failure; `403 forbidden` means the caller is not declared for the requested group; `403 token_mismatch` can indicate a stale token after same-operation POST rotation.
4. For network failures, verify the shared TLS listener and Secret, projected token audience, and client CA/SAN validation. Check both deployment pod and namespace selectors. Preserve sidecar `/active-site` and `/pitr-cutoff` access on 8082 and escrow on 8443 when changing policies.
5. Compare the deployment's DDL boundary and heartbeat with topology changes. Use `gtid-audit.sh`; it reports generation before and after its non-atomic site observations. A changed generation requires another observation pass, not permission to resume DDL.

### Invalid Lease duration blocks expiry

**Symptom:** `Deferred/DeploymentHold` persists beyond expiry, with `deployment lease housekeeping failed` or reconciler errors containing `spec.leaseDurationSeconds: Invalid value: 0: must be greater than 0`.

**Cause:** The affected operator writes zero after rounding the remaining lifetime down. Kubernetes rejects the Lease write when `spec.leaseDurationSeconds` becomes `0`; this affects expiry writes, release/revocation in the final fractional second, and revocation tombstone creation after expiry. Stored state remains unchanged, so retries fail again. Correct TLS trust does not fix this API-server validation error.

1. Collect sanitized state/expiry/reason with `deployment-probe.sh` and correlate operator errors. Do not dump raw Lease annotations or ownership hashes. Stop deployments whose confirmed lease expired; post-DDL uncertainty requires manual schema verification.
2. Propose an approved operator upgrade or rebuild containing the positive-duration persistence fix. Coordinate deployment owners and review queued maintenance before rollout, since it can resume when expiry processing succeeds. No MySQL data reset or Lease edit is required. Restarting the same affected binary and requesting revocation do not bypass the invalid write.
3. Verify errors stop, expired holds disappear from active observations, and the planned operation re-enters normal preflight once all holds end. Inspect any remaining blocker rather than forcing promotion. Revoked operation IDs must remain fenced after replacement and restart.

The fixed Kubernetes spec has a minimum one-second duration even for terminal records. Authority still uses the annotation's state, exact `expiresAt` (`now > expiresAt`), and topology generation; the floor adds no grace period. Do not treat the spec duration or holder identity alone as evidence of an active deployment.

### Remediation boundaries

A renewal `404`, `409`, or changed generation means stop the deployment. Post-DDL uncertainty requires manual schema/ledger verification; do not reconnect to the new primary and continue. Same-operation POST rotates the ownership token and must never be used to evade revocation. Release a hold through the owning deployment client's cleanup, not by editing API-server records.

For approved administrative interruption, first coordinate with all deployment owners, stop/fence their clients, and preserve failure evidence. Then annotate the group:

```bash
kubectl annotate mysqlfailovergroup <group> -n <namespace> \
  bloodraven.shipstream.io/revoke-deployment-leases=true --overwrite
```

Verify `DeploymentLeaseRevoked` and renewals reporting `OperatorRevoked`. Revocation does not roll back DDL, kill the client, or itself change topology generation. Planned failover, ordered update, and restore-in-place can proceed after holds end and normal preflight passes. Emergency failover, returning-primary fencing, and primary reassertion never wait on a hold. Never delete the leader-election Lease.

For an isolated writable site, probe `/status` (`self_fenced`) and `/peer/active-site`, not `/fencing`. Keep the effective timeout `max(configured leaseTimeout, 3s, 3 * max(peerCheckInterval, 1s)) < 60s` for a 60s DNS TTL and allow additional margin for monitor ticks, network timeouts, and SQL fence completion. A reachable peer can prevent lease-expiry fencing, but a fresh conflicting authoritative topology triggers immediate fencing without waiting for expiry.

---

## 7. Tenant database adoption refused (`MysqlDatabase`)

### Symptoms
- `status.phase=Failed` with `reason` `DatabasePreExists`, `PreExistingOwnerUser`, `PreExistingUser`, or `UserClaimedBySibling`.
- A `deletionPolicy: Delete` teardown left an account behind, with `OwnerUserDropSkipped`, `UserDropSkipped`, `OwnerUserReservedSkipped`, `UserReservedSkipped`, or `UserTransferred` in Events.
- `status.pendingOwnerUser` or an `appliedUsers[].pendingUsername` has been set for longer than one reconcile interval.

### Diagnostic steps
1. Read the phase, the `Ready` condition's reason, and the message — the message names the first refused `user@host`, which is the whole diagnosis:
   ```bash
   kubectl get mysqldatabase <name> -n <ns> -o jsonpath='{.status.phase}{" "}{.status.message}{"\n"}'
   ```
2. Dump the write-ahead records and compare them **per `user@host`** with what exists in MySQL:
   ```bash
   kubectl get mysqldatabase <name> -n <ns> -o jsonpath='{.status.databaseCreated}{" owner="}{.status.ownerUser}{.status.ownerHosts}{" pendingOwner="}{.status.pendingOwnerUser}{.status.pendingOwnerHosts}{"\n"}{range .status.appliedUsers[*]}{"  entry "}{.secretName}{" user="}{.username}{.hosts}{" pending="}{.pendingUsername}{.pendingHosts}{"\n"}{end}'
   ```
   `pendingOwnerHosts` / `appliedUsers[].pendingHosts` belong to the rotation *target*; `ownerHosts` / `hosts` belong to the settled name. An empty pending host list with a pending username set is an older operator's shape and falls back to the settled list. A record naming `user@H1` is not a claim on `user@H2`.
3. Identify the other claimant. `UserClaimedBySibling`, `DatabaseNameConflict` and `OwnerConflict` name the peer CR in the message; `PreExisting*` means the account exists in MySQL and **no** record of this CR names it on that host — usually a schema or account created out of band, or a `databaseName` / Secret username collision.
4. For a skipped drop, read the event text: it says which claim vetoed it (a sibling's owner, `grants[]` or ledger; a group-level principal; or this CR's own `grants[]`).

### Safe remediation
1. **Never hand-edit `status`.** These fields are the operator's record of which accounts it created. Clearing `ownerUser`, `pendingOwnerUser`, `ownerHosts`, `appliedUsers[]` or their `pending*` counterparts strands a live privileged account that no later reconcile or delete can find; inventing an entry authorizes a `DROP USER` on an account Bloodraven did not create. There is no supported `kubectl patch` remedy here.
2. Resolve the collision in the *spec* instead: pick an unused `databaseName`, point the owner or `users[]` Secret at a tenant-unique username, or remove the duplicate CR. One refused `user@host` parks the whole CR, so the fix is one edit, not a per-entry cleanup.
3. To adopt an existing schema or account deliberately, drop and recreate it under a name this CR creates — adoption is refused by design and cannot be granted after the fact.
4. A skipped drop is correct behaviour, not a leak: the account is still claimed by something. It has already lost its rights on the deleted database. If it genuinely should not outlive the CR, remove the remaining claim (the sibling's `grants[]`/`users[]` entry, or this CR's own `grants[]`) and drop the account by hand with the group's operator credential.
5. A stuck `pending*` record almost always means the apply keeps failing earlier. Look at the `Ready` condition reason first (`PrimaryUnavailable` is transient and self-heals; `MySQLError`, `GrantUserMissing` and `InvalidSpec` are verdicts about the CR).

---

## 8. Sidecar restarting, unready, or not fencing during a MySQL stall

### Symptoms
- The `sidecar` container restarts or sits in `CrashLoopBackOff` while `mysqld` is hung, in crash recovery, or restarting after CLONE.
- Pods go unready during a MySQL stall.
- After a stall or a sidecar restart, a site that is not the active one is still `super_read_only=OFF`.

### Diagnostic steps
1. Check which paths the probes hit. Liveness must be `/peer/ping` and readiness `/health`:
   ```bash
   kubectl get deploy -n <ns> -l app.kubernetes.io/instance=<group> \
     -o jsonpath='{range .items[*]}{.metadata.name}{" live="}{.spec.template.spec.containers[?(@.name=="sidecar")].livenessProbe.httpGet.path}{" ready="}{.spec.template.spec.containers[?(@.name=="sidecar")].readinessProbe.httpGet.path}{"\n"}{end}'
   ```
   A liveness path of `/health` means the operator predates the fix, so any MySQL stall longer than about 30s restarts the fencing agent. Upgrading the operator rolls the new probe out through the ordered update. That fixes liveness even for a pinned older `spec.sidecar.image`, since every sidecar serves `/peer/ping`.
2. Unready with liveness passing is the intended state while MySQL is down. Confirm the process is alive and the fencing loop is making progress:
   ```bash
   ./.agents/skills/bloodraven-doctor/scripts/sidecar-probe.sh -n <ns> <group> livez
   ```
   `secondsSinceProgress` should stay well under `stallThresholdSeconds`. A 503 here means the loop is wedged on a call that ignored its timeout. Capture `support-bundle.sh` output before anyone restarts the container. A sidecar older than this endpoint returns 404 for it.
3. Read the sidecar's `safety net:` log lines (see `site/content/docs/8.observability/7.log-schema.md`):
   - `could not set initial super_read_only; the fencing monitor will retry` and `MySQL was unreachable long enough to have restarted; re-arming` mean a fence is still owed. On the first tick where MySQL answers, the monitor fences it unless the operator confirms this site on that tick.
   - `set super_read_only=ON after MySQL became reachable`, `super_read_only=ON confirmed; staying fenced`, and `operator confirms this is the active site; ...` mean the check completed.
   - `could not set super_read_only; will retry` repeating means MySQL answers reads but rejects the fence write. Check the operator user's grants and MySQL's error log.
4. Compare `@@super_read_only` with the operator's `activeSite` (`/peer/active-site`, or `status.activeSite` on the CR). The topology-mismatch rule fences a writable non-active site within one `peerCheckInterval` whenever an authoritative view exists.

### Safe remediation
1. Do not restart the sidecar to "re-run the safety net". The monitor applies the same rule on its own once MySQL answers, and a restart only adds a window with no fencing agent.
2. If a non-active site is writable and the sidecar logs show no pending `safety net:` work and no topology view, fence it by hand (playbook 1, step 1) and investigate why neither the operator nor a peer reported an active site.
3. A short stall (under 30s) does not re-arm the safety net, by design: a slow query on the active primary must not fence it. A `mysqld` that restarts that quickly comes back `read_only=ON` (every `mysqld` is started with `--read-only=ON`), so application users cannot write; it is super-fenced by the topology-mismatch rule only if someone makes it writable, and the operator re-asserts it if it was the primary. A pod still on a pre-`--read-only` spec (mid-upgrade) comes back writable and is fenced only by the topology-mismatch rule or the operator.

## 9. `app` user's mysql schema restriction pending, or a replica stopped on a partial revoke

The operator partially revokes the `app` user's write and DDL privileges on the `mysql` system schema. That needs `partial_revokes=ON`, and the revoke replicates, so the operator first turns it ON on every site and applies the revoke only once all of them confirm.

### Symptoms
- `CredentialReconcileFailed` events containing `partial_revokes not confirmed on every site`, naming a site.
- Or, on a replica: `Replica_SQL_Running: No` with the failed transaction being `REVOKE ... ON mysql.* FROM 'app'@'%'`. This happens only when a site was switched back to `partial_revokes=OFF` by hand, or a site outside the group's spec replicates from it.

### Diagnostic steps
1. Check each site:
   ```bash
   kubectl exec -n <ns> <pod> -c mysql -- mysql -N -e "SELECT @@GLOBAL.partial_revokes"
   ```
2. Check whether the restriction landed on the primary:
   ```bash
   kubectl exec -n <ns> <primary-pod> -c mysql -- mysql -N -e \
     "SELECT User_attributes FROM mysql.user WHERE User='<app-user>'"
   ```
   It should list a `Restrictions` entry for `"Database": "mysql"`. The group's `bloodraven.shipstream.io/system-schema-restriction` annotation equals `bloodraven.shipstream.io/credential-hash` once it has.

### Safe remediation
1. Pending restriction: bring the named site back. The operator retries every minute and turns `partial_revokes` ON itself. Nothing to run by hand.
2. Stopped replica: `SET GLOBAL partial_revokes = ON; START REPLICA;` on that replica. Do not skip the transaction; skipping it leaves the replica without the restriction, so after a failover the `app` user could write the grant tables there.

### Tenant Pending with `PartialRevokesOff`, or Ready with escaped grants

**Symptom:** a `MysqlDatabase` is `Pending` with Ready reason `PartialRevokesOff` and a Warning event. Its message identifies a failed setting read or a refused `SET GLOBAL partial_revokes = ON` on the primary. Transient connection errors instead use `PrimaryUnavailable` and retry. Tenant SQL and the write-ahead stamp have not run; existing grants and accounts remain intact. Cleanup and Delete-policy deletion do not wait on this setting.

1. Read the tenant status and events without reading its credential Secrets:
   ```bash
   kubectl describe mysqldatabase <name> -n <namespace>
   ```
2. Discover the primary's MySQL container and query `SELECT @@GLOBAL.partial_revokes` through the approved admin connection. Check the operator account's ability to read that variable and its `SYSTEM_VARIABLES_ADMIN` privilege. Use the event's error to distinguish missing privileges from connectivity problems.
3. With approval, restore the operator's setting access. The controller retries automatically, enables the setting and applies literal schema grants. Do not work around this by disabling partial revokes; the group's `app` restriction on `mysql.*` depends on it.

**Older false-Ready state:** intermediate development builds and the former SET-refused escaped fallback could leave an escaped row such as `acme\_wms` after the mode became ON. That spelling then matches no schema. Through an approved admin connection, inspect `SELECT Host, Db, User FROM mysql.db WHERE User = '<tenant-user>'` and compare the exact stored spellings. Upgrading to a release containing the escaped-grant reapplication fix reapplies affected Ready tenants once and restores the literal row before removing the escaped row. Ordinary v1.4.0 → v1.5.0 release upgrades already invalidated tenant hashes and are not this unchanged-hash case. If the setting was changed manually after migration, make an approved spec edit to reapply; restarting the operator or editing metadata alone does not invalidate an unchanged Ready hash. Never patch status to manufacture ownership or clear the applied hash.


## 10. Shared principal loses global access to a tenant schema

**Symptom:** a principal listed in `grants[]` loses global DDL, INSERT, SHOW VIEW or backup access on one tenant schema. Older tenant surplus/removal/deletion revokes could create partial restrictions instead of only removing explicit grant rows. If the principal is the operator account, subsequent tenant apply or deletion can fail too. Restrictions can persist after the CR is deleted or return through restore/PITR.

1. Through an approved admin connection, inspect restrictions without collecting passwords or password hashes:
   ```sql
   SELECT User, Host, JSON_EXTRACT(User_attributes, '$.Restrictions') AS Restrictions
   FROM mysql.user
   WHERE JSON_LENGTH(JSON_EXTRACT(User_attributes, '$.Restrictions')) > 0;
   ```
   Compare them with the affected account's explicit schema grants in `information_schema.SCHEMA_PRIVILEGES`. Independent global privileges and role grants are a separate source of access. The `app` account's restriction on `mysql` is intentional; other entries require investigation, not automatic removal.
2. Upgrade to the row-derived tenant revoke fix. Narrowing, removal, retirement and Delete-policy cleanup then revoke only privileges present in each schema grant row, including on retry. Ready objects reapply once. `PartialRevokeLifted` records repair of restrictions on impossible escaped schema names for accounts already recorded by the CR. `TenantPrivilegeRestriction` reports real-schema restrictions, a restricted connecting grantor, or a repair error. Escaped-name repair is best-effort and does not gate normal apply/deletion. If the connecting grantor is restricted on an escaped spelling, repair for every principal on that spelling is skipped; the warning names the grantor and this playbook. These escaped restrictions are inert and can remain until an unrestricted administrator lifts them. Grant-only schema rows (`USAGE` with `IS_GRANTABLE=YES`) retain independently managed GRANT OPTION and do not block cleanup retries.
3. Ask the privilege owner to confirm which real-schema restrictions were accidental. Their JSON contains no provenance. Desired tenant GRANTs can lift restrictions on their own requested privileges, but the operator does not grant surplus privileges on a real schema merely to clear a restriction.
4. After approval, use an unrestricted administrator to lift only the confirmed accidental privileges. For example, to restore an independently granted global INSERT on `acme_wms` for `cdc`:
   ```sql
   GRANT INSERT ON `acme_wms`.* TO 'cdc'@'%';
   ```
   If the operator account restricted itself, use a separate administrator such as root; do not retry the repair through the restricted operator account. Group credential reapplication can restore an authoritative group role's global set, but it must also retain/reapply the intended `app` restriction on `mysql.*`. Avoid broad global re-grants for external/shared principals whose intended policy is unknown.
5. Check the remaining restriction JSON and explicit schema rows, then verify the intended operation through a fresh connection as the affected principal. Retry the tenant reconcile/delete after admin recovery. Do not patch status, skip replicated transactions, disable partial revokes, or remove finalizers to hide a remaining access failure.

---

## 11. Restore or verification reports missing BRV1 magic

### Symptoms

- Restore or verification fails with `ciphertext tamper/downgrade detected` and `missing BRV1 magic` after profile encryption changed.
- A completed backup has `EncryptionAttributionCorrected` in Events.

### Diagnostic steps

1. Read the backup's recorded encryption choice and producing Job name:
   ```bash
   kubectl get mysqlbackup <backup> -n <ns> -o jsonpath='{.status.jobName}{" mode="}{.status.encryption.mode}{" algorithm="}{.status.encryption.algorithm}{"\n"}'
   ```
2. Inspect that Job's owner reference, `bloodraven.shipstream.io/backup-encrypted` label, container names, and the uploader's `BLOODRAVEN_ENCRYPTION_ALGORITHM`. The `backup-encrypt-upload` main container identifies an encrypted launch. A `mysqlsh` main container without the encryption label identifies a plaintext launch. Use the original Job configuration rather than today's profile. Do not collect Secret contents or passphrases.
3. If the owned Job is gone, preserve the audit record and verify the artifact's format through an approved storage inspection. Missing BRV1 magic on an encrypted launch can indicate tampering or a wrong source location.

### Safe remediation

1. Run an operator containing the attribution fix. On reconciliation it repairs completed backups from their owned Jobs and emits `EncryptionAttributionCorrected` once per correction. Profile edits affect newly created Jobs.
2. Confirm the corrected status, then retry restore or verification. Keep the original passphrase Secret for backups whose Jobs actually encrypted them.
3. If status and Job already agree, investigate storage identity and artifact integrity. Do not enable plaintext fallback to hide an unexplained mismatch.

---

## 12. Backup or restore reports Access denied with valid credentials

### Symptoms

- A backup or restore Job reports MySQL `Access denied` while the operator connects successfully.
- The password includes surrounding spaces, tabs, or line endings, often from a Secret created with `--from-file`.

### Diagnostic steps

1. Confirm the Job mounts the intended derived MySQL credential Secret and uses the expected backup scripts ConfigMap. Check the group's credential references without decoding or collecting password contents.
2. Inspect the `dump.py` or `restore.py` code in that ConfigMap. Fixed scripts use `_read_mysql_cred_file` to read bytes and decode UTF-8 without stripping or newline translation. Older scripts call the whitespace-normalizing `_read_cred_file` for MySQL credentials. The Go credential producer already preserves those contents.
3. Check file presence and readability without printing values. A readable empty password file intentionally overrides the legacy environment password. A readable empty username fails validation. Missing or unreadable files can use the legacy environment fallback.

### Safe remediation

1. Upgrade to an operator containing the reader fix and confirm it has refreshed the shared scripts ConfigMap before retrying the Job. Do not trim or rotate valid credentials to compensate for the old reader.
2. Retry a failed backup with `kubectl bloodraven backup <group> -n <ns>`. For a failed restore, follow the relevant restore runbook and its data handling precautions before retrying.
3. If authentication still fails with the corrected scripts, investigate the selected MySQL user, host grants, and Secret references. Preserve intended whitespace; AWS configuration normalization is separate from MySQL credentials.

---

## 13. Succeeded backup cannot be found at its recorded location

### Symptoms

- Restore or verification cannot load a dump from a Succeeded MysqlBackup.
- An older backup's `status.artifactStorage.location` contains spaces where the storage prefix used underscores, or recorded binlog names and GTID tags lost underscores.
- Cleanup reports an artifact absent while the original objects remain in storage.

### Diagnostic steps

1. Compare the backup's recorded location with the producing Job's main-container `BLOODRAVEN_OUTPUT_URL` and `BLOODRAVEN_STORAGE_TYPE`. Use `mysqlsh` for plaintext Jobs and `backup-encrypt-upload` for encrypted Jobs; the encrypted dump init container points at temporary staging, not the stored artifact. Do not consult today's mutable profile to reconstruct an older destination.
2. Account for the S3 display convention: plaintext Jobs record a bucket-relative key; encrypted Jobs record `s3://<BLOODRAVEN_S3_BUCKET>/<output-prefix>/`. PVC Jobs record the original absolute output path.
3. Inspect the completion log format. `v=2` percent-encodes token values and preserves underscores, percent signs, and whitespace. Unversioned logs used the same underscore for both original spaces and literal underscores, so the original value cannot be recovered from that log alone. Legacy tagged GTID and binlog metadata may also be damaged in already-stored status.

### Safe remediation

1. Upgrade to an operator and dump scripts containing the versioned codec for future completions. Older encrypted operator images can still finish with legacy lines; the fixed parser preserves their literal identity fields but cannot infer originally encoded spaces.
2. Establish the actual artifact using the original Job inputs and storage inspection. If those are unavailable, take a replacement backup. Keep old artifacts until their identity is verified; do not release cleanup finalizers based on a guessed path.
3. Do not globally substitute spaces and underscores in persisted status or infer original GTID tags. Future correct parsing does not reconstruct metadata already corrupted by an older controller.

---

## 14. Backup completed with incomplete metadata

### Symptoms

- `BackupMetadataIncomplete` Event or `MetadataComplete=False` on a MysqlBackup.
- A Succeeded backup has a location but missing size, GTID, or binlog observations.
- An older Succeeded record has an empty or underscore-damaged location.

### Diagnostic steps

1. Run `triage.sh` or collect `support-bundle.sh`. Their backup projections include phase, producing Job, location and metadata condition; Job projections include the owner UID and main-container storage/output fields. They do not collect credential or passphrase contents.
2. Inspect `MetadataComplete`: `SentinelUnavailable` means a pod-list, missing completed Pod, log-stream, scanner, or bounded-read failure; `SentinelMissing` means readable logs lacked a usable completion sentinel. Check access to the completed Pod's main-container logs and API/kubelet availability. `LocationMismatch` means the Job destination was used instead of a different sentinel value.
3. Compare `status.artifactStorage.location` with the owned Job's effective main-container output using playbook 13's S3/PVC conventions. Kubernetes expands `$(NAME)` from earlier known environment values and reduces `$$` once; a raw env template may differ from the producer's path. Runtime Secret/ConfigMap, downward-API and service values unavailable from the Job cannot be inferred; a valid producer-reported location is retained instead. A legacy record without `MetadataComplete` keeps its producer-reported path, even if it contains literal `$(NAME)`. Only a recorded Job fallback permits raw-template repair. Consumer Jobs double literal dollars in their env values, so apply one kubelet expansion before comparing their paths with status. Do not collect those Secrets or use encrypted staging paths or a changed profile to guess a destination.
4. `DestinationUnavailable` leaves `Ready=False`. On first completion it returns a reconcile error; on an already-Succeeded record it records a terminal repair outcome without error so retention can continue. Successful metadata repair preserves a True Ready condition; cleanup-only location recovery does not make a Running or Failed backup ready. Deletion of an unrecoverable record emits `ArtifactCleanupSkipped` naming the Job and releases the finalizer; inspect storage separately for any unlocated artifact.
5. Deferred positive size observations can refresh the last-size gauge when the repaired backup is not older than the latest observed success. They do not repeat terminal counters, completion timestamps or Events, and older repairs do not replace newer sizes.

### Safe remediation

1. Restore pod-log reachability while the operator retries every 15 seconds for up to ten minutes from completion. The recovery state is persisted, so restarting the operator does not lose the retry. An empty GTID or zero size in a collected sentinel is valid and does not trigger repeated collection.
2. If logs are gone permanently, use the recorded Job-derived location for restore or verification. Optional observations can remain absent; the backup's successful phase still reflects its completed Job and known artifact destination. Repeated repairs do not repeat terminal counters or completion Events.
3. For historical missing or underscore-damaged locations, reconcile with the fixed operator while the original owned Job is retained. The operator verifies the original destination and repairs the record, including before deletion. If that Job is gone, establish the artifact through approved storage inspection or take a replacement backup. Never invent location substitutions or remove a cleanup finalizer based on a guessed path.

---

## 15. Scheduled backup fails without the expected retry

### Symptoms

- A scheduled MysqlBackup failed with configured retry attempts remaining, but no successor appears.
- A retry's `status.attempt` is temporarily zero or lower than its launch attempt after a failed status patch.
- `BackupRetryConflict` warns that an existing retry backup has different lineage.

### Diagnostic steps

1. Check `spec.backup.retry.maxAttempts`, the failed run's completion time, and `status.backupSchedules[].nextRetryTime`. Backoff doubles per attempt up to the configured cap; ad-hoc backups do not participate.
2. Use the backup projections in `triage.sh` or `support-bundle.sh`. Match the failed run's UID to a successor's `bloodraven.shipstream.io/backup-retry-of` label. The root name/UID annotations identify the original scheduled run, and `bloodraven.shipstream.io/backup-attempt` is durable launch metadata. A retained higher attempt from another run does not prove that this run has a successor.
3. Compare status and launch attempts. Retry limits use the greater value; a lost status patch does not reset a chain to attempt 1. The backup reconciler restores status from the annotation before launch or terminal observation.
4. New names use `backup-retry-<predecessor UID hash>-r<N>`. Repeated reconciles and stale lists reuse that name and verify predecessor, root, attempt, schedule, group and profile. `BackupRetryConflict` means a name collision or altered metadata; inspect those public fields without changing credential Secrets. `status.backupSchedules[].retryConflict` names the blocking object. The operator emits the warning and log when that persisted marker changes, clears it when the conflict is no longer observed, and warns again if a resolved conflict recurs. Reconciles and operator restarts do not repeat an unchanged conflict warning. Only that retry is skipped; other schedules and group status rollups continue. An AlreadyExists/cache-NotFound race requests a five-second wake-up instead of group-wide error backoff; genuine API read failures still propagate.

### Safe remediation

1. If running an affected older operator, upgrade to the per-run lineage fix. Do not delete successful or failed history to make retries work; each new run receives its own chain.
2. If backoff is pending, allow the scheduled wake-up. If a legitimate successor exists, investigate that backup's own phase and Job instead of forcing another retry of its predecessor.
3. For a conflicting identity, establish the source of the conflicting object or metadata edit before acting. Do not rewrite lineage annotations or attempt counters to bypass `maxAttempts`. If an immediate backup is needed, use `kubectl bloodraven backup <group> -n <ns>` for an explicit new run.

---

## 16. Verification source does not match the expected backup

### Symptoms

- A newer successful backup appears while a verification is still running.
- `BackupNotAvailable` after a selected source was deleted or recreated before Job launch.
- `BackupIdentityMismatch` on a running legacy verification, or disagreement between status and the Job's source.

### Diagnostic steps

1. Collect the verification name, phase, Job name and `status.backupRef` name/UID/location/storage type. The triage and support-bundle projections include these public fields. Compare the selected UID rather than relying on a reused backup name or today's latest successful backup.
2. Inspect the producing Job's pod-template `bloodraven.shipstream.io/verification-backup` JSON annotation. New Jobs pin the same tuple in immutable Job spec; the operator checks it against the source env and restores missing or drifted status from it. Existing Jobs are observed without re-fetching the source CR or current profile/derived credentials.
3. For plaintext, compare the main container's `BLOODRAVEN_INPUT_URL` with the pinned location using the normal S3/PVC trailing-slash convention. For encrypted sources, inspect `decrypt-download` storage, bucket and `BLOODRAVEN_SOURCE_PREFIX`; its target and the main input are temporary decrypted paths. Do not collect passphrases, Secrets or arbitrary env.
4. A legacy Job without the snapshot can continue only with a complete status reference matching its source inputs. If the status claims a different location, or its identity is incomplete, the fixed operator fails it with `BackupIdentityMismatch` instead of certifying another backup. Historical terminal records produced by an older operator still require comparison with retained Job evidence.

### Safe remediation

1. For a running Job whose original source is still recorded, allow it to finish. Newer backups or edits to `spec.backupRef` do not change its execution identity. Create another verification to test the newer backup.
2. If the pinned source disappeared or its UID changed before launch, create a new verification for the intended source. The operator does not silently re-pin the old run, even when its spec requested latest selection.
3. For identity mismatch, preserve the recorded reference and Job inputs for investigation, then start a new verification with an explicit intended source. Do not patch the audit record to make a different backup appear verified. Pinning identifies the source but does not hold stored artifacts against retention or external deletion.

## 17. Restore Job failed on `Read-only file system` / `load-progress`

Restore Jobs mount the dump read-only: source PVCs at `/restore` or `/backups`, and decrypted encrypted backups at `/restore-decrypted`. Operators built before load-progress tracking was disabled for restores let mysqlsh try to write `load-progress.<uuid>.json` next to the dump. Every PVC-sourced or encrypted restore then failed.

### Symptoms
- Restore Job logs show `BLOODRAVEN_LOAD_FAILED` mentioning `load-progress` and `Read-only file system` (EROFS).
- For an in-place restore, the Job logs `BLOODRAVEN_DROP_SCHEMA` / `BLOODRAVEN_DROP_ALL_USER_SCHEMAS_DONE` before the load failure, so the target schemas are already gone. `status.restoreInPlace.phase` is `Failed`.

### Diagnostic steps
1. Confirm the Job's main container disables progress tracking. On a fixed operator the variable is present with an empty value:
   ```bash
   kubectl get job -n <ns> <restore-job> -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="BLOODRAVEN_LOAD_PROGRESS_FILE")]}'
   ```
   No output means the Job was built by an older operator.
2. For in-place, list what survived on the primary: `SHOW DATABASES`.

### Safe remediation
1. Upgrade the operator. Load-progress tracking is not a `loadOptions` setting, so no spec change helps.
2. Bootstrap: delete the failed restore Job; the reconciler rebuilds it. Restore Jobs no longer resume a partial load. If the failed Job's log shows `BLOODRAVEN_LOAD_START`, empty the target first (delete the new group's MySQL pods and PVCs together, `kubectl delete pvc,pod -l app.kubernetes.io/instance=<group>,app.kubernetes.io/name=mysql -n <ns>`, and wait until the re-created claims are `Bound` and the pods `Running`; or `./playground/reset-mysql.sh`), or the reload stops on objects the first attempt created.
3. In-place: bump `spec.restoreInPlace.confirm` to a newer RFC 3339 timestamp to re-run the restore. It drops and reloads the same scope, which is what is needed when the earlier attempt dropped the schemas and never loaded them.

## 18. Bootstrap PITR restore fails with duplicate-key or missing-row errors

A PITR replay pipes archived binlogs, with their original GTIDs, into the new primary. The archive includes transactions the dump already contains. MySQL skips them only when the dump's GTID set is in the target's `gtid_purged`. The operator loads bootstrap PITR restores with `updateGtidSet: "replace"` and `skipBinlog: true` to install that set. Operators built before that change did not, so replay re-executed pre-dump transactions.

### Symptoms
- Restore Job logs `BLOODRAVEN_LOAD_COMPLETE`, then `BLOODRAVEN_PITR_FAILED` with MySQL error 1062 (duplicate entry), 1032 (row not found), 1050 (table exists) or 1396 (user exists).
- `status.restore.phase` is `Failed` with reason `PITRReplayFailed`.

### Diagnostic steps
1. Check the Job's load options:
   ```bash
   kubectl get job -n <ns> <restore-job> -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="BLOODRAVEN_LOAD_OPTIONS")].value}'
   ```
   A fixed operator shows `"updateGtidSet":"replace"` and `"skipBinlog":true` when `initFromBackup.pointInTime` is set.
2. On the target, compare `SELECT @@GLOBAL.gtid_purged` with the dump's `gtidExecuted` (`@.json` in the backup, or `status.gtidExecuted` on the MysqlBackup). After a fixed load they match.

### Safe remediation
1. Upgrade the operator.
2. The target was fresh, so nothing outside it was changed. Reset it and run the restore again:
   - Playground: `./playground/reset-mysql.sh`.
   - Production: scale the new group's MySQL Deployments to 0 (or delete their pods), then delete their PVCs, or recreate the group.
   - Delete the failed restore Job (`kubectl delete job -n <ns> <restore-job>`). While it exists the reconciler keeps reporting `Failed` instead of rebuilding it.
3. Error 1146 or 1049 during replay rather than 1062/1032 means the replay touched a schema the dump or load left out (`dumpSchemas`, `loadOptions.includeSchemas`/`excludeSchemas`). Restore from an instance dump, without load-time schema filters.
4. Do not work around it with `pointInTime.excludeGtids` set to the dump's GTID set. That skips the in-dump transactions but leaves `gtid_purged` without the dump's history.

## 19. In-place point-in-time recovery, or an earlier one stopped at the backup

In-place PITR cannot work against a live primary. The replay keeps the archived transactions' original GTIDs, and the primary has already executed every one of them, so MySQL skips them all. `spec.restoreInPlace` has no `pointInTime` field (removed in v1.7.0; the API server prunes it from a manifest). Operators before v1.7.0 accepted it: the earliest ran the replay anyway, logged `BLOODRAVEN_PITR_COMPLETE` and reported `Succeeded` with the data left at the backup, and later ones rejected it in Preflight (`RestoreInPlaceRejected`).

### Symptoms
- A manifest with `restoreInPlace.pointInTime` restores to the backup: the field was pruned, so the restore is a plain in-place restore.
- Or, after an older operator ran in-place PITR: rows written between the backup and `stopTime` are missing, although the restore reported success.

### Diagnostic steps
1. For a past run, compare the expected post-backup rows with what the primary holds. An older operator's `status.restoreInPlace.pitrReplayedBinlogCount > 0` does not mean anything was applied (the field is gone in v1.7.0).

### Safe remediation
1. Remove `pointInTime` from the `restoreInPlace` manifest so it says what runs.
2. To reach a point in time, restore into a new failover group with `spec.initFromBackup.pointInTime` (fresh target, so the dump's GTID set is installed and the replay applies). Then move the data or traffic across. For a single tenant schema, dump it from the recovered group with `mysqlsh util.dumpSchemas` and run a per-schema in-place restore from that dump.
3. Do not try to force replay on the live primary with `RESET BINARY LOGS AND GTIDS` or `mysqlbinlog --skip-gtids`. The first destroys unarchived binlogs and archive continuity. The second re-applies in-dump and cross-site duplicate transactions.

## 20. Peers not yet recloned after a full-instance in-place restore

A full-instance in-place restore replaces the primary's data. The other sites still have the old data until each is recloned from the restored primary. The operator tracks them in `status.restoreReclones`. The queue is `Armed` while the restore runs and `Draining` after it succeeds. It is `Held` when a later restore that replaced it ran on the primary and failed: nothing is cloned from that primary. The operator reclones the sites one at a time through the `reclone-site` annotation and removes each one only after its clone succeeds. `status.restoreInPlace.phase: Succeeded` does not mean the peers are done.

### Symptoms
- `status.restoreReclones.pending` is non-empty long after the restore succeeded.
- `RestoreRecloneFailed` events, or `status.restoreReclones.failures` lists a site.
- `status.restoreReclones.phase: Held` and a `RestoreReclonesHeld` event: a full-instance restore ran its Job on the primary, failed, and changed the primary. Every pending site keeps its replication receiver stopped and the data it had before the restore; nothing is cloned or replicated into it. A reclone annotation for a held (or `Armed`) site is refused with `RecloneRejected` and removed.
- Every queued site shows `Replica_IO_Running: No` (or `performance_schema.replication_connection_status.SERVICE_STATE = OFF`), `sourceConvergenceReason: RestorePeerHold`, `Degraded=RestoreReclonePending`, and `bloodraven_replication_running{thread="io"}=0`. This is by design: the restore stopped the receivers before its Job dropped anything, and only each site's reclone restarts replication.
- A full-instance restore waits in Preflight with `waiting for the running clone of <site> to finish before restoring`: a clone was running when the restore was accepted. It proceeds once the clone ends.
- `PromotionRefusedReclonePending` events, or `Degraded=NoPrimary` with a `pending post-restore reclone` message after the primary failed.
- Readers return old data from a site that is still queued. A queued site is labelled `bloodraven.shipstream.io/healthy=false`, which takes it out of the `-replica` Service and a `ReadOnly` site's Service; the per-site Service of a `PrimaryCandidate` or `DisasterRecovery` site does not select on it, so check for clients that connect there directly.

### Diagnostic steps
1. Queue state:
   ```bash
   kubectl get mfg <group> -n <ns> -o jsonpath='{.status.restoreReclones}' | jq .
   kubectl get mfg <group> -n <ns> -o jsonpath='{.metadata.annotations.bloodraven\.shipstream\.io/reclone-site}'
   ```
   `requested` is the site whose clone is running, and the annotation should name it. If the annotation names a different site, someone else's reclone request is pending. The queue waits for it and never overwrites it.
2. Clone progress and failures: the `Bootstrapping` condition (`True` while a clone runs; `False/NotBootstrapping` after a success, `False/BootstrapFailed` after a failure, which also sets `Degraded=True/BootstrapFailed`), and operator logs `msg="starting bootstrap" source="reclone"`, `bootstrap failed`, `bootstrap completed successfully`. A clone is done only when `Bootstrapping` is `False/NotBootstrapping` with a `lastTransitionTime` after it started and the site's `replication` is `Healthy` or `Lagging`.
3. Events: `kubectl get events -n <ns> --field-selector involvedObject.name=<group> | grep -E 'RestoreReclone|PromotionRefusedReclonePending|RecloneRequested'`.

### Safe remediation
1. A failing clone: fix the cause (donor reachability, the clone plugin, recipient disk space, a sealed keyring) and let the queue retry. Do not set your own reclone annotation for a queued site. The queue would wait for it, and a clone it did not request does not remove the site.
2. The primary failed while only queued sites remain: prefer bringing the primary back; the queue then resumes. Promoting a queued site means serving its pre-restore data:
   - With the primary up there is no in-place promotion: a queued site's receiver is stopped, so it cannot catch up and a planned failover to it is refused (`TargetUnhealthy`).
   - With the primary down: clear the queue with `kubectl patch mfg <group> -n <ns> --subresource=status --type=json -p '[{"op":"remove","path":"/status/restoreReclones"}]'`, after which automatic failover proceeds. Then reclone the remaining sites by hand from whichever primary you keep.
3. `RestoreReclonesDiscarded` after a failed restore means the restore left the primary unchanged: it never created its Job, or the primary's `gtid_executed` still equals `status.restoreReclones.fencedGtidExecuted` (for example the Job failed before its `DROP`). The peers resume replicating and are eligible for promotion again. If the failed restore had replaced an earlier restore's queue, a `RestoreReclonesRestored` event means that queue is draining again. If the event says the primary is unchanged but the Job ran, check the primary: a load without binlogging into a primary that had no user schemas changes data without GTIDs.
4. A `Held` queue (`RestoreReclonesHeld`): the primary may hold the failed restore's dropped or half-loaded data, and the held sites keep their pre-restore data with replication stopped. Do not `START REPLICA` on a held site and do not clone from the primary until its data is checked. Scheduled backups skip the held sites (they do not replicate) and fall back to the primary: suspend schedules whose retention could age out the last good backup. A `RestorePeerReplicationStoppedBehind` event names a held site that is missing its last pre-fence transactions. Then choose one:
   - Restore the held data into the primary: create a `MysqlBackup` with `spec.sourceSite: <held-site>`, then run a new full-instance restore from it (it reclones every peer once it succeeds).
   - Keep the primary's data once it is verified good: `kubectl patch mfg <group> -n <ns> --subresource=status --type=merge -p '{"status":{"restoreReclones":{"phase":"Draining"}}}'`; the held sites are recloned from it.
   - Remove `status.restoreReclones`: this resumes the held sites' replication from the current primary, which replicates whatever the failed restore did to it.

## 21. In-place restore stays fenced after `spec.restoreInPlace` was removed, or ends "cancelled"

Once a restore is accepted, `status.restoreInPlace` drives it, not the spec. Removing the field never interrupts a running restore Job. The `-primary` Service fence, the topology freeze (no failover or reclone), and the planned-failover, deployment and `MysqlDatabase` guards stay up until that Job finishes. A run removed before its Job existed is cancelled.

### Symptoms
- `spec.restoreInPlace` is gone, but `status.restoreInPlace.phase` is still `Restoring` and the message reads `spec.restoreInPlace was removed; holding the fence until restore Job ... finishes`. There is a `RestoreInPlaceSpecRemoved` event.
- Or `phase: Failed` with `cancelled: spec.restoreInPlace was removed ...` (`RestoreInPlaceCanceled`): nothing was restored.
- Or `phase: Failed` with `RestoreInPlaceJobLost`: the run's Job vanished before its outcome was seen. `confirmTokenUsed` is that run's confirm, because its Job may have run. This also applies when `jobName` is empty but `jobCreating: true`: the operator had started creating the Job when its status write or the process was lost.

### Diagnostic steps
1. `kubectl get mfg <group> -n <ns> -o jsonpath='{.status.restoreInPlace}' | jq .` for the phase, `jobName`, `scope`, `confirmTokenUsed` and, while a run is in flight, `runConfirm` (that run's confirm).
2. `kubectl get job -n <ns> <jobName>` and its pod logs (`BLOODRAVEN_DROP_*`, `BLOODRAVEN_LOAD_*` sentinels) show how far the restore got.

### Safe remediation
1. A held fence clears by itself when the Job finishes; the run then completes as `Succeeded` or `Failed`. Do not delete the Job to "unstick" it: that kills the loader mid-DROP or mid-load and leaves a partially restored primary.
2. After `RestoreInPlaceJobLost`, verify the primary's data (the restore may have partly run) before relying on it. Then re-run the restore with a newer `confirm`, or restore from backup.
3. Do not edit `status.restoreInPlace` to clear it. `confirmTokenUsed` is what stops an old manifest from re-running a restore that already ran.

## 22. Full-instance restore stuck in Fencing, or applications locked out after a restore (`offline_mode`)

A full-instance in-place restore fences the primary with `SET GLOBAL offline_mode = ON`. MySQL then disconnects and refuses every session without `CONNECTION_ADMIN`/`SUPER`, with error 3032 `ER_SERVER_OFFLINE_MODE`. The fence is recorded in `status.restoreOfflineFence` (`site`, `confirmToken`, `runID`, `priorOfflineMode`, `sessionsEvicted`). The restore Job starts only after no client session from before the fence remains. The operator puts the prior value back when the restore ends.

### Symptoms
- `Failed` with `restoreInPlace rejected: the operator MySQL account lacks <privileges>` (any of `CONNECTION_ADMIN`, `SYSTEM_VARIABLES_ADMIN`, `PROCESS`, `REPLICATION_SLAVE_ADMIN`): preflight refused the restore (nothing fenced, confirm not consumed).
- Stuck in `Fencing` with a message such as `waiting to read offline_mode`, `waiting to apply the offline_mode fence`, or `offline_mode is ON on <site>; waiting for N application session(s) to disconnect`.
- After the restore, applications get `ERROR 3032 ... offline mode`, with `RestoreOfflineFenceReleaseFailed` events and `status.restoreOfflineFence` still set.
- During the restore every peer's replication receiver is stopped (`RestorePeerReplicationStopped`), whatever the replication account, and non-admin monitoring of the primary has gaps. Both are expected.
- Stuck in `Fencing` with `waiting to stop replication on peer <site>: ...` (unreachable peer, refused `STOP REPLICA IO_THREAD`), `receiving the primary's last transactions` (a running receiver catching up, at most 30 s), or `waiting for the running ordered update / planned failover / clone`. After two minutes the run ends `Failed` with `RestoreInPlacePeerStopFailed`: nothing was restored, the confirm is consumed, and `offline_mode` is released.
- Stuck in `Preflight` with `waiting for peer <site> to be reachable: ...`: a peer is unreachable. Nothing is fenced yet. `Failed` with `... lacks REPLICATION_SLAVE_ADMIN on <peer>` rejects the restore without consuming the confirm.
- A new `confirm` does not start a new run while `status.restoreReclones` is still `Armed` from the previous restore: the previous queue is settled first (up to ten minutes while that restore's Job pods are still running), with a `RestoreInPlaceDeferredPreviousQueue` event. `status.restoreInPlace` keeps showing the previous run meanwhile, and `offline_mode` stays enforced on the primary (re-asserted after a `mysqld` restart) until the settlement finishes.
- `waiting for the planned failover to finish` in Preflight: a planned failover already in flight (one that owns the sites, is `Publishing`, or holds a provisional claim) is still driven to completion; the restore starts afterwards. A `Deferred` planned failover, or an `Admitting` one without a claim, does not block it and stays waiting while the restore runs.
- `waiting for a concurrent operation to finish (<reason>)` or `waiting for the automation hold of planned failover <runID> to be acknowledged (bloodraven.shipstream.io/acknowledge-planned-failover-hold=<runID>)` in Preflight: the deployment admission reservation is refused (a planned-failover claim or hold, an ordered update, a topology change not yet persisted). The restore waits instead of failing; a hold needs the acknowledgement ([playbook 38](#38-automation-held-after-a-planned-failover-automaticfailoverheld)).
- Stuck in `Preflight` or `Fencing` with `waiting for the clone admission of <site> to finish` or `waiting for the running clone of <site> to finish`: a clone was starting or running when the restore began. It finishes (or is refused with `BootstrapRefusedRestoreHold`) first.
- `status.restoreInPlace` is `Failed` but `offline_mode` stays ON and `status.restoreReclones` stays `Armed`: a pod of the failed restore's Job has not terminated (check `kubectl get pods -l batch.kubernetes.io/job-name=<job>`), or the primary is briefly unreadable. The operator waits up to ten minutes (two for an unreadable primary) before holding the peers, because the pod could still drop schemas. Do not lift `offline_mode` by hand meanwhile; delete a stuck pod only once you know it is not mid-restore.
- A `RestoreOfflineFenceSessionsKept` Warning: the operator account lacks `SYSTEM_USER` and left the named accounts' sessions connected.

### Diagnostic steps
1. `kubectl get mfg <group> -n <ns> -o jsonpath='{.status.restoreOfflineFence}{"\n"}{.status.restoreInPlace.phase} {.status.restoreInPlace.message}'`
2. On the primary: `SELECT @@GLOBAL.offline_mode;` and `SELECT ID, USER, HOST, COMMAND, TIME FROM information_schema.PROCESSLIST;`. Sessions that keep coming back belong to accounts with `CONNECTION_ADMIN`/`SUPER`; they are not fenced and do not block the wait. A session that will not die (still `Query` after KILL) does block it.
3. Operator account privileges: `SHOW GRANTS FOR '<operator-user>'@'%';` must include `ALL PRIVILEGES ON *.*`, or `CONNECTION_ADMIN`, `SYSTEM_VARIABLES_ADMIN`, `PROCESS` and `REPLICATION_SLAVE_ADMIN` (`SUPER` stands in for all but `PROCESS`).

### Safe remediation
1. Missing privilege: grant the ones the message names, e.g. `GRANT CONNECTION_ADMIN, SYSTEM_VARIABLES_ADMIN, PROCESS, REPLICATION_SLAVE_ADMIN ON *.* TO '<operator-user>'@'%';` (the restore also needs DDL and DML). The operator re-checks about once a minute with the same `confirm`; nothing else is needed.
2. Fencing waits on a stuck session: find it in the processlist and `KILL <id>`. Do not lift `offline_mode` while the restore is in `Fencing`, `Restoring` or `Resuming`; the operator re-asserts it every poll anyway.
3. Locked out after a terminal restore because the release keeps failing: fix operator reachability to the primary. If applications must come back now, run `SET GLOBAL offline_mode = OFF;` on the primary. If `priorOfflineMode` was `true`, the primary was already in offline mode before the restore; leave it ON. The operator clears the record once its release succeeds.
4. Applications running as accounts with `CONNECTION_ADMIN`/`SUPER` are not fenced at all. Remove those privileges from application accounts.
5. After an operator downgrade during a full-instance restore, nothing releases the fence (older operators do not know `status.restoreOfflineFence`). Once the restore is terminal, run `SET GLOBAL offline_mode = OFF;` on the primary unless `priorOfflineMode` was `true`.
6. Fencing waits on a peer, or the run ended `RestoreInPlacePeerStopFailed`: restore the operator's connectivity to the peer (or its `REPLICATION_SLAVE_ADMIN` grant), then bump `spec.restoreInPlace.confirm`. A site cannot be removed from `spec.sites`: admission refuses the edit, and dropping a site means replacing the group (site docs "Replacing a site"). Do not stop the peer's replication by hand to get past the check and then start it again: while a full-instance restore runs or its queue is `Armed`/`Held`, a running receiver pulls the restore's `DROP` statements. Do not downgrade the operator while `status.restoreReclones` is `Armed` or `Held`: an older operator restarts the held receivers.

## 23. Ordered update stalls on the active site (`maintenance handoff aborted`)

With `updateStrategy.type: OrderedUpdate`, the operator rolls drifted followers first. It then hands the primary role to a healthy standby, and only after that rolls the old active site. The handoff promotes the standby only once its `GTID_EXECUTED` contains everything the fenced active site committed. If that cannot be proven, the handoff aborts. Nothing is lost when it aborts, but the active site keeps its old spec until a later attempt succeeds.

### Symptoms
- Operator log: WARN `maintenance handoff refused; source not fenced` (refused before any write pause, usually a lagging standby) or `maintenance handoff aborted; source unfenced` (aborted after the fence), followed by `ordered update failed`, repeating on later reconciles. The active site's Deployment hash stays stale.
- Or ERROR `maintenance handoff: rollback failed: source may be left read-only`: the abort could not give the active site its writes back.
- Or, after a successful handoff, ERROR `ordered update: old active has transactions the new primary lacks; not repointing it`, or an `ordered update failed` error containing `before repointing`. The new primary is serving; the old active is fenced (`super_read_only=ON`, unless the error is `fence old active ... before repointing`) and is not replicating.

### Diagnostic steps
1. Read the reason from the `error` field:
   - `did not reach the source GTID set before fencing` (on the `refused` line): the standby is lagging; no write pause happened.
   - `did not reach the fenced source GTID set ... N transactions missing`: the standby is still behind after the 30s fenced budget.
   - `active primary became unreachable during ordered update`: the active site stopped answering. Emergency failover takes over; see the failover runbooks.
   - `no longer contains the fenced source set` (after `STOP REPLICA` or `RESET REPLICA ALL`): the standby's server lost history after the catch-up proof, for example a replaced pod. Treat it as a suspect replica and compare its data before anything else.
   - `transactions the source lacks` (on WARN `maintenance handoff refused; candidate has transactions the source lacks`, or in an `aborted` error after `STOP REPLICA` / `RESET REPLICA ALL`): the standby is divergent. Never promote it to get the update through: use playbook 33.
   - `is divergent (N transactions the primary lacks); reclone or reconcile before the active site can be updated` (on `ordered update failed`, or WARN `ordered update: active-site update stalled; standby is divergent`): the operator has recorded the divergence and does not pick the standby. Playbook 33.
   - `is not Ready` or `did not become Ready` (on `ordered update failed`, or WARN `ordered update: stalled; site pod not Ready`): a pod, not MySQL, holds the update. Playbook 34.
   - `candidate SQL thread error`: the applier stopped.
   - `unparseable GTID set`, or a read error.
2. Compare the sets. The standby's set should contain the active site's:
   ```bash
   kubectl exec -n <ns> <active-pod> -c mysql -- mysql -N -e "SELECT @@GLOBAL.gtid_executed"
   kubectl exec -n <ns> <standby-pod> -c mysql -- mysql -N -e "SHOW REPLICA STATUS\G" | grep -E 'Running|Seconds_Behind|Last_.*Error|Gtid_Set'
   ```
   `scripts/gtid-audit.sh -n <ns> <group>` prints every site's set in one pass.
3. After a rollback failure, check the active site: `SELECT @@GLOBAL.super_read_only, @@GLOBAL.read_only`.

### Safe remediation
1. Lagging standby: let it catch up. Reduce the write load or fix the receiver's network path. The handoff retries on its own, and nothing needs to be run.
2. Applier error: treat it as a broken replica (playbook 2). Do not skip transactions to make the handoff pass.
3. Rollback failed and the active site is read-only, and no other site is writable: clear the fence on the active site only (`SET GLOBAL read_only = OFF;`, which also clears `super_read_only`). Before you do, confirm that the standby is still read-only and still replicating from the active site.
4. Standby left without replication after the abort (WARN `failed to restore candidate replication channel after abort` or `failed to restart replication on candidate after abort`; `SHOW REPLICA STATUS` empty or threads stopped). Nothing re-points it automatically.
   - Confirm the standby is read-only.
   - Confirm its `@@GLOBAL.gtid_executed` is a subset of the active site's: `SELECT GTID_SUBSET('<standby set>', '<active set>')` on the active site returns 1.
   - If it is, run on the standby: `CHANGE REPLICATION SOURCE TO SOURCE_HOST='<active internal host>', SOURCE_USER='<repl user>', SOURCE_PASSWORD='<repl password>', SOURCE_AUTO_POSITION=1; START REPLICA;`. Add `SOURCE_SSL=1, SOURCE_SSL_CA='/etc/mysql/tls/ca.crt'` when `spec.tls` is set, otherwise `GET_SOURCE_PUBLIC_KEY=1`.
   - If it is not a subset, the standby has errant transactions: reclone it (`kubectl bloodraven reclone <group> <site> -n <ns>`) instead, or see playbook 33.
5. Old active not repointed after the handoff (`not repointing it` or `before repointing`). Leave it fenced and do not run `CHANGE REPLICATION SOURCE` on it by hand. Old-primary recovery picks it up on a later poll:
   - If the fence failed or a set was only unreadable, recovery fences the site itself, compares the sets, and rejoins it as a replica when nothing is missing from the new primary. Until then a site whose fence failed has `read_only=ON` but may lack `super_read_only`: check `SELECT @@GLOBAL.super_read_only, @@GLOBAL.read_only` on it.
   - If it really holds transactions the new primary lacks, recovery reports them in `status.sites[].divergentGtid` (`divergenceDetectedBy=OldPrimaryRecovery`) and blocks: follow playbook 2 to extract or replay them before recloning the site.
   - If the old active came back on an empty datadir (a replaced or wiped PVC), its set holds only server-init transactions under a new `server_uuid`. The rollout still refuses and pages, but recovery treats the site as empty and leaves it to auto-clone: no `divergentGtid` appears. Confirm with `SELECT @@server_uuid, @@GLOBAL.gtid_executed` on it (no server UUID shared with the new primary; GTID tags are ignored, so `uuid:tag` and `uuid` count as shared. A site that shares a server UUID but holds transactions the primary lacks is blocked as divergent, not cloned) and let the clone run.

## 24. One group stopped updating (no running topology manager)

A healthy operator pod does not guarantee that every group has a live topology manager. The runner restarts a group's manager when its spec or connection material changes. If the restart fails, the group has no manager, and so no polling, no status writes and no failover, until the runner's 30s retry succeeds.

### Symptoms
- One group's `status.sites[]` and conditions stop changing while other groups keep updating.
- Losing the primary does not trigger failover.
- Operator log: ERROR `failed to start topology manager` for that `fg`, repeating every 30s.
- Warning Event `TopologyManagerStartFailed` on the group, carrying the same error. It is rate-limited (at once when a running manager was torn down, otherwise after one 30s retry, then at most every 5 minutes), so read the log for the latest attempt. A Normal `TopologyManagerStarted` Event marks a recovery the same operator process saw.
- These Events are history, not current state, in either direction. A group that recovered and broke again within 5 minutes can still show `TopologyManagerStarted` last. The operator forgets a failure episode when it restarts, so a group fixed after an operator restart never gets `TopologyManagerStarted` and can still show `TopologyManagerStartFailed` last. Before declaring the manager absent, confirm it from current evidence: the ERROR repeating every 30s in the last couple of minutes of operator log, and `status.sites[].lastSeenTime` no longer advancing.

### Diagnostic steps
1. Read the error:
   ```bash
   kubectl get events -n <ns> --field-selector involvedObject.name=<group>,reason=TopologyManagerStartFailed
   kubectl logs -n bloodraven deploy/bloodraven | grep -E 'failed to start topology manager|topology manager exited|connection material unreadable' | grep '<ns>/<group>'
   # Current state: a line every 30s here means no manager right now.
   kubectl logs -n bloodraven deploy/bloodraven --since=2m | grep 'failed to start topology manager' | grep '<ns>/<group>'
   kubectl get mysqlfailovergroup <group> -n <ns> -o jsonpath='{range .status.sites[*]}{.name}{" lastSeenTime="}{.lastSeenTime}{"\n"}{end}'
   ```
2. Typical causes:
   - The operator Secret is missing (`get operator secret ... not found`).
   - It has no `username` or `password` key, or the key is empty (`operator secret <ns>/<name> missing required key "username"`, `... has an empty value for key "password"`).
   - The `spec.tls.secretName` Secret is missing or its material does not load.
3. A WARN `connection material unreadable; keeping the running topology manager` is not this failure. The manager is still running on its existing connections while the Secret is being replaced.

### Safe remediation
1. Restore or fix the Secret named in the error. The next sync starts a fresh manager. You do not need to restart the operator.
2. Do not promote by hand while the group has no manager unless the primary is actually down. Once the manager is back, it re-evaluates topology on its first poll.

## 25. Encrypted PITR sidecar fails to init archive store

### Symptoms
- Sidecar logs repeat `failed to init archive store` with `permission denied` for the passphrase file.
- Current sidecar images keep running: the pod stays Ready and in its Services, HTTP and self-fencing work, and `/archiver/status` reports `enabled:true`, `storeReady:false`, `lastError: archive store init: ... permission denied`. The group shows `PITRArchiveReady=False` with reason `ArchiveStoreInitFailed` and one Warning `PITRArchiveNotReady` Event. Archival is stopped on that site.
- Sidecar images that predate that behaviour exit instead: the container crash-loops, HTTP probes fail, the pod is unready, and archival and sidecar fencing are unavailable. The operator then reports `SidecarUnreachable`.

### Cause
Older operators project `pitr-passphrase` as root-owned mode `0400`, but PITR sidecars default to UID 999 and MySQL pods have no fsGroup by default. Archive-store initialization cannot read the file. A current sidecar retries it with backoff (5 s doubling to 5 min) and keeps every other service running; an older sidecar exits before starting its HTTP server or fencing monitor.

### Diagnostic steps
1. Read the sidecar log (add `--previous` for an older, crash-looping image):
   ```bash
   kubectl logs -n <ns> <pod> -c sidecar --tail=100 | grep 'archive store'
   ```
2. Inspect the pod's Secret mode and security contexts without reading Secret contents:
   ```bash
   kubectl get pod -n <ns> <pod> -o jsonpath='{.spec.volumes[?(@.name=="pitr-passphrase")].secret.defaultMode}{"\n"}{.spec.securityContext}{"\n"}{.spec.containers[?(@.name=="sidecar")].securityContext}{"\n"}'
   ```
   JSON reports decimal modes: `256` is `0400`; `292` is `0444`. Without fsGroup, a non-root UID cannot read the root-owned `0400` projection. On a current sidecar `/archiver/status` names the cause in `lastError`; on an older one it is unavailable while the container crash-loops, so use logs.

### Safe remediation
Upgrade to the operator that projects the live sidecar passphrase as `0444`, matching PITR S3 credentials. Current operators mount the binding's key snapshot (`status.pitrArchiveBindings[].encryption.keySecretName`), not the profile's Secret, with `optional: true`: a missing snapshot leaves the pod running and the archiver failing closed, reported as `PITRArchiveBindingReady=False/KeyMaterialLost` (playbook 26). Its PITR render-version change triggers ordered updates for existing deployments. Verify the new pod's projection mode, readiness, and `/archiver/status` (`storeReady:true`) after the rollout, and that `PITRArchiveReady` returns to `True`. Backup and restore Job projections keep their separate fsGroup-based permissions. Preserve the existing passphrase; adding fsGroup to an existing MySQL PVC can trigger a recursive ownership change and is unnecessary for this fix.

## 26. PITR archive encryption binding refused or key material lost

Covers: encryption or key change refused, key mismatch, key material lost, deferred render, check Job stuck, binding limit, release.

### Background
`status.pitrArchiveBindings[]` holds, per archive destination the group writes (the active PITR profile) or reads (`initFromBackup.pointInTime`, PITR verifications, `archiveProfileName`), the accepted encryption identity: `id` (`pab-` + 16 hex), `destination`, `archiveID`, `source`, `encryption{mode, algorithm, keySecretName}`, `state`/`reason`, `check{jobName, jobUID}`. Read `encryption` only when `state` is `Bound`; otherwise it is the candidate under test. For `Passphrase`, `keySecretName` is an immutable operator-made **key snapshot** `mysql-<group>-pitr-key-<h10>` holding the trimmed passphrase. Sidecars (mounted `optional: true`), restore, verification and check Jobs mount the snapshot, never the user's Secret. Snapshots have no owner reference and are never pruned. Condition `PITRArchiveBindingReady` summarises the bindings; `PITRArchiveReady` still reports what the sidecars observe.

A binding becomes `Bound` only by proof: a sidecar reports `Verified` for the destination with the binding's mode and exact `keyRevision` (the snapshot name, or `plaintext`), or a one-shot `pitr-archive-check` Job verifies the descriptor. Once `Bound`, pods render the binding whatever the profile now says, so an edit never changes what a pod loads.

### Symptoms and meaning
| Condition reason / Event | Meaning |
|---|---|
| `False/KeyChangeRefused`, Warning `PITRArchiveKeyChangeRefused` | The live passphrase of a profile in archive use (in-place rotation, alias profile with other bytes, Secret renamed while PITR was off) differs from the accepted snapshot. Pods are unchanged; archiving continues under the accepted key. |
| `False/EncryptionChangeRefused`, Warning `PITRArchiveEncryptionChangeRefused` | Encryption added, removed or changed on a profile in archive use. The admission rule refuses only a direct edit of the active PITR profile's encryption with its storage fields unchanged as written, so this reaches reconcile through any path it cannot see: an edit made while PITR was disabled, then re-enabled; a switch of `pitr.profileName` to an alias profile (same destination, other encryption); an encryption change bundled with a spelling-only storage edit (`prefix: pitr` to `pitr/`, endpoint case or default port); or an edit of a read profile (`archiveProfileName`, a verification's profile) whose binding is `Bound`. The message names the profile. The render keeps the accepted identity. Remedy: revert that edit (for an alias, switch `pitr.profileName` back), or rotate with a new prefix. |
| `True` + Warning `PITRArchiveKeySourceRemoved` | The user's Secret or key is gone. Nothing changes: pods use the snapshot. Restore it before the next rotation procedure. |
| `False/KeyMismatch` (entry `Refused`) | Not yet Bound and the candidate key does not open the archive. For a first upgrade the message reads "recover the archive's original passphrase into Secret `<name>` key `<key>`". |
| `False/ArchiveForeign`, `ArchiveCopy`, `ArchiveLegacy`, `ArchiveDescriptorMissing`, `ArchiveNotFound`, `ArchiveEncryptionMismatch`, `ArchiveVersionUnsupported` | The check Job or a sidecar refused the destination (playbook 39 explains each). `ArchiveForeign`/`ArchiveCopy` also come from writer eligibility: a Bound read binding of another group's archive, or of a copy, was made the write target. The previous writer render is held. |
| `False/KeyMaterialLost`, Warning `PITRArchiveKeyMaterialLost` | A Bound entry's snapshot is missing and the live bytes do not reproduce its name ("...; Secret <s> held the accepted key of archive <id>: restore it from a backup of it, or set a profile resolving to <dest> to the original passphrase so the operator re-creates it (...)"), or the snapshot is present but no longer holds the accepted key (wrong `pitr-accepted` `keyCheck`, labels or destination: "...; Secret <s> no longer passes the check of the accepted key of archive <id>: do not delete it, it may hold the only copy of that key; restore its labels and annotations, or its bytes, from a backup of it; delete it (the operator then re-creates it from a profile's passphrase) only once the original passphrase is safely kept elsewhere and a profile resolving to <dest> holds it (...)"). mysqld keeps running (optional mount); the archiver fails closed (`PITRArchiveReady` not True). |
| Warning `PITRArchiveKeySnapshotRecreated` | A deleted snapshot was re-created under the same name from live bytes that reproduce it; no roll. Neither reported nor clearing `KeyMaterialLost` when the create hit `AlreadyExists` (another object of that name appeared); the next pass checks that object. |
| Warning `PITRArchiveKeySnapshotReadFailed` | A snapshot's `pitr-accepted` annotation is malformed. It is never used for recovery and never deleted. |
| `Unknown/Authenticating` or `Unknown/Establishing` | A proof is pending. `Establishing` follows a check Job that found an empty prefix: the sidecar creates the descriptor and proves it. A check that could not list the prefix is no refusal: the entry stays `Pending/Authenticating` with the message "ArchiveListFailed: the check of `<candidate>` could not list the prefix at `<dest>`; grant list permission on the prefix (s3:ListBucket) or make the volume readable; retrying in 1m0s", retried every minute (restores and MBVs wait). A stored `Refused/ArchiveListFailed` from an older operator is retried too. |
| `False/BindingLimitReached`, Warning `PITRArchiveBindingLimitReached` | 16 entries that stay (`Bound`, or used by a target) exist; a new destination gets none and is never rendered (the group keeps its held render, or renders PITR disabled). A reader (MBV/restore) gets this only when all 16 are `Bound`; with a list full of non-`Bound` entries it waits: "the binding list is full of entries that are not Bound; waiting for the group's binding pass to make room for <dest>". |
| Warning `PITRArchiveBindingRefused` naming Deployments, log `pitr render deferred` | Site Deployments without a decodable `bloodraven.shipstream.io/pitr-render` annotation (from before bindings existed) are not updated, nor are their ConfigMaps (`my.cnf` keeps its pre-upgrade PITR settings), until a check Job authenticates the live key. The deferral holds while ANY site Deployment lacks a decodable annotation (the write binding not `Bound`/`Establishing`), so deleting or re-creating one site Deployment does not lift it; a site created meanwhile renders the held render (or the candidate). The note names only the unannotated ones: "Deployments <names> are not updated until the archive accepts a key: fix the passphrase Secret, switch to a new prefix, or disable PITR for this profile". A pre-v1.7.0 writer next to an annotated site counts as a running writer, so the write target gets a writer check. Backups of the active profile do not wait (`PITRArchiveRolloutPending`) while deferred; they run unstamped (`PITRArchiveStampSkipped`). |
| Warning `PITRArchiveBindingReleaseRefused` / `PITRArchiveBindingReleaseRejected` | A release annotation names a binding still in use / is malformed or has a wrong `confirm`. |

### Diagnostic steps
1. Bindings and condition (`scripts/triage.sh` prints both):
   ```bash
   kubectl get mfg <group> -n <ns> -o jsonpath='{range .status.pitrArchiveBindings[*]}{.id}{"\t"}{.state}{"/"}{.reason}{"\t"}{.encryption.keySecretName}{"\t"}{.destination}{"\n"}{end}'
   kubectl get mfg <group> -n <ns> -o jsonpath='{.status.conditions[?(@.type=="PITRArchiveBindingReady")]}{"\n"}'
   ```
2. Key snapshots, metadata only (never `-o yaml`; `data` is the passphrase and `pitr-accepted` is a verifier):
   ```bash
   kubectl get secrets -n <ns> -l app.kubernetes.io/name=mysql-pitr-key,bloodraven.shipstream.io/failover-group-uid=$(kubectl get mfg <group> -n <ns> -o jsonpath='{.metadata.uid}') \
     -o 'custom-columns=NAME:.metadata.name,BINDING:.metadata.labels.bloodraven\.shipstream\.io/pitr-binding-id,DEST:.metadata.annotations.bloodraven\.shipstream\.io/pitr-destination,IMMUTABLE:.immutable'
   ```
3. What the pods load: the Deployment's `pitr-passphrase` volume `secretName` must equal the Bound entry's `keySecretName`, and the sidecar env `BLOODRAVEN_PITR_KEY_REVISION` must equal it (or `plaintext`). `status.pitr.archive.keyRevision` is what the sidecars reported.
4. Check Jobs (`kubectl get jobs,pods -n <ns> -l bloodraven.shipstream.io/failover-group=<group>,bloodraven.shipstream.io/resource=pitr-archive-check`): the result line is the pod's termination message `BLOODRAVEN_PITR_ARCHIVE_CHECK v=2 state=<Absent|Verified|Refused> reason=... archiveID=... sourceUID=... sourceNamespace=... sourceName=... destinationDigest=<64 hex>` (the digest is of the normalized destination; the operator compares it with the Job's own `BLOODRAVEN_PITR_DESTINATION`, which must equal the binding's). Only the Job whose UID equals `check.jobUID` counts. The Job carries annotation `bloodraven.shipstream.io/pitr-key-secret-uid` (the candidate snapshot's UID at creation): a `Verified` verdict for a snapshot replaced under the same name is dropped and the entry re-checked ("the key snapshot the check verified was replaced: ...; checking it again"). A check Job for the other role (Writer vs Reader) is dropped and re-checked ("check Job <j> ran as <role>, but the destination is now checked as <role>; checking again"); an `Establishing` entry whose destination is no longer written becomes `Pending/Authenticating` ("the destination is no longer written; authenticating it for its readers") with a Reader check. Refusal messages read "<token> reported by the writer check of ..." / "... reported by the reader check of ...". A pod Pending more than 60 s, or a failure without a result line, is deleted and retried after 1 min; the entry stays `Pending/Authenticating` with the cause (image pull, quota, admission, storage egress). Also retried: an exit 0 with an unparsable line ("check Job <j> reported a verdict line that does not parse: <parser error>"), and exit 4 (`bloodraven pitr-archive-check` could not write the verdict to the termination log; ERROR "write the verdict to the termination log; the check fails"; reported as "check Job <j> failed: ..."). Finished check Jobs no entry records are deleted after each successful status write, never before it.
5. Operator log lines: `pitr archive binding changed` (`cause` = `proof`, `check`, `recovery`, `integrity`, `c3` or `input`; `input` is a spec, Secret or snapshot change, such as a new target's first entry, a missing key Secret, a retried `Refused` entry or a superseded candidate), `pitr archive binding released`, `pitr render annotation ignored` and `pitr render deferred` (both with `fg` and `namespace`).

### Safe remediation
- **Key or encryption change refused (rotation).** There is no in-place rotation. Revert the profile or Secret to the accepted bytes and settings (whitespace-only edits are accepted), or follow the procedure: create a profile with a **new storage prefix** and a new passphrase Secret, switch `spec.backup.pitr.profileName`, wait for the new binding to be `Bound` (a check Job reports `Absent`, then the sidecar proves the new archive), take a full backup, and keep the old binding (and the old prefix) for as long as you need its history. A shared dump + PITR profile whose rotation was refused leaves new dumps under the new key and the archive under the old one.
- **KeyMismatch on a not-yet-Bound entry.** Put the archive's original passphrase back into the Secret the message names; the binding is retried when the candidate changes (no timer). If the original key is lost, the archive is unreadable: switch to a new prefix.
- **Key material lost.** Read the message first. *Snapshot missing* ("held the accepted key"): restore the snapshot Secret from a backup of it (same name, same bytes), or set a profile resolving to the destination to the original passphrase so the operator re-creates it under the same name. *Snapshot present but failing the check* ("no longer passes the check"): do NOT delete it first, it may hold the only copy of the key; restore its labels and annotations, or its bytes, from a backup of it. Delete it (so the operator re-creates it from a profile's passphrase) only once the original passphrase is safely kept elsewhere and a profile resolving to the destination holds it. If the digest key was re-created, the old name cannot be reproduced; only a restored copy of the Secret helps. MySQL keeps serving meanwhile; only archiving is stopped.
- **Deferred render (pre-binding Deployments).** Fix the user's Secret so it opens the archive (the check then reports `Verified` and the Deployments roll once), switch to a new prefix, or disable PITR for the profile. Deleting or re-creating one site Deployment does not lift the deferral (any unannotated site Deployment holds it).
- **Check Job stuck.** Fix what the pod event names (image pull, quota, NetworkPolicy egress to the store). For `Pending/Authenticating` with `ArchiveListFailed: ... retrying in 1m0s`, grant `s3:ListBucket` (and `GetObject`) on the prefix or make the volume readable; it is retried every minute and clears on its own.
- **Binding limit.** Release unused bindings (below).
- **Release** removes an unused binding and deletes its key snapshots. One id per annotation:
  ```bash
  kubectl annotate mfg <group> -n <ns> bloodraven.shipstream.io/release-pitr-archive-binding="<id>:confirm=<group>:uid=$(kubectl get mfg <group> -n <ns> -o jsonpath='{.metadata.uid}')"
  ```
  It is refused (`PITRArchiveBindingReleaseRefused`, annotation kept, retried every minute) while the destination is a current target, or a snapshot is named by a site Deployment's render annotation, a site Deployment's decoded render targets the released destination ("Deployment <d> renders <destination>"), a site Deployment's pod template references a snapshot (even scaled to zero), a site Deployment carries a `pitr-render` annotation that does not decode ("... so what it renders cannot be told; repair or remove it"), a non-terminal pod references a snapshot (mounted, projected volume, env `secretKeyRef` or `envFrom`: an old ReplicaSet mid-roll, restore, verification, check, staging or a `pitr-quarantine` maintenance pod, which you must delete when done), a running Job is labelled `bloodraven.shipstream.io/pitr-key-secret`, or a live verification holds one (`verification-hold.bloodraven.shipstream.io/<uid>`). A malformed value, a wrong `confirm` or a `uid` that is not this group's (a request left over for an earlier group of the same name) is rejected and the annotation removed. Never commit this annotation in GitOps. Releasing a binding makes its archive unreadable unless you kept the passphrase elsewhere.
- **Group deleted.** Snapshots survive. When no archive needs them: `kubectl delete secret -n <ns> -l app.kubernetes.io/name=mysql-pitr-key,bloodraven.shipstream.io/failover-group-uid=<uid>`.

Never delete a key snapshot to "unstick" a binding, never edit its data (it is immutable; replacing it by hand trips `KeyMaterialLost`), never delete `archive.json` or rewrite manifests, and never enable plaintext fallback as a repair.

## 27. PITR history after a site reclone or data rebuild

### Symptoms
- A former primary is rebuilt, becomes primary again, and reuses `mysql-bin.000001` or other retained binlog names.
- Older archivers may report `backlogFiles=0` while missing the new files, because they recognized only the basename.
- After a lossy failover, an old primary that came back archived its writable-era tail (a read-only site uploads it), including the transactions the new primary never received. Once that site is recloned its server no longer holds them, and its audit records its own tail row as divergent: `divergentRows` above 0 in `/archiver/status`, INFO `archive divergence evidence recorded` once, and the site named in the message of `PITRArchiveReady`, which stays `True`. This is expected and needs no action on the archive: no quarantine, no acknowledgement. Restores across those transactions refuse `PITRArchiveDivergentHistory` until `excludeGtids` lists them (playbook 42).
- The same site reports `lastError` "site `<s>`: server_uuid `<uuid>` no longer has archived transactions `<set>` that it issued … and reuses their GTIDs when it writes …", a non-empty `reissuableGtids` and `PITRArchiveReady=False/ArchiverError`: its server kept the `server_uuid` it issued the lost transactions under. Follow playbook 41 before that site is promoted.

### Diagnostic steps
1. Record sidecar, operator, and backup/restore image versions. Confirm all archive readers and writers understand the additive manifest `id` field before using new archives with older images.
2. Inspect `/archiver/status`, upload logs, and the relevant site manifest through the configured archive reader. New rows use `(name, id)` where `id` is a SHA-256 of sealed plaintext bytes for plaintext stores, or a passphrase-keyed HMAC-SHA256 digest for encrypted stores, and new keys end in `<name>.<id>`. Two rows may share a name. Every row has an `id`: a manifest that holds a row, a quarantined row or a retirement record without one (written before archive identity, stripped by an older writer, or edited by hand) is refused by the sidecar, the restore and `pitr-quarantine` (`PITRArchiveObjectInvalid` "… has no id …"; with no `archive.json`, `PITRArchiveLegacy`). Nothing repairs such a row; the remedy is a new prefix plus a full backup.
3. Check that sealed files created after rebuild have corresponding objects and rows. New/changed rotations preempt historical work, including known names. A newest-first cursor re-identifies the archived local files after a sidecar restart and yields after 8 files, 64 MiB or two seconds, including mid-file; local fingerprints do not persist. Inspection/encryption/upload need no full-file scratch space. Inspect `archive binlog` errors, backlog and actual storage throughput if it cannot catch up.
4. Take a full backup after the rebuild becomes primary and verify an isolated restore with application markers: a `mode: Timestamp` verification of that backup to a marker written after it proves forward recovery from that backup, which is the usual recovery path. It does not cross the rebuild: replay starts after the backup. To prove the archive replays across the rebuild, verify a backup taken before the rebuild to a marker written after it; until such a verification succeeds, treat continuity across the rebuild as unverified. If the rebuilt site was recloned after a lossy failover and then promoted, follow playbook 41 first. A successful upload count or zero backlog cannot prove history that an older writer already omitted.

### Safe remediation
Preserve old manifests, objects, and encryption material. Do not delete a site manifest, purge local binlogs, or overwrite retained objects to make names appear new. Use compatible ID-aware readers and writers. If an older archiver already missed files, preserve any surviving local binlogs and investigate the recovery gap before declaring PITR coverage restored. Older writers discard IDs, coverage, purged bases, quarantine/reset guards and pending deletes on rewrite; their pruning can drop every same-name row. Re-upgrade recovers nothing an older writer dropped: a manifest it rewrote is refused (`PITRArchiveLegacy`, or `PITRArchiveObjectInvalid` for a row without an `id`). Stop the older writer and restore preserved manifests with compatible recovery images, then verify coverage. All rows use GTID predecessor order, including unique-name numbering resets. Empty frontiers/rotations are valid; XA, LOAD DATA and checksum-off logs use observed ordering GTIDs independently of strict retention coverage. Clone `baseGtids` bridges donor GTIDs missing from the first log header. Divergent boundaries fail before SQL. For missing or unreadable objects, follow [Repair PITR history safely](../../../../site/content/docs/7.backup-and-restore/7.pitr-history-repair.md): pause writers, preserve objects/manifests, verify a canonical full backup, quarantine the exact row with a reason/resume bound, then verify replay. Quarantine never deletes data and can be undone. Divergence is evaluated only with established server history: empty execution, running clone and read-only/no-channel bootstrap cannot flag the archive. Every eligible audit recomputes the missing sets, so a record ends when the server holds the transactions again. A divergent row is evidence, not an archiver error, and needs no quarantine; only a server that could reissue the missing GTIDs (they are under its own current `server_uuid`) is an error (playbook 41), and for that source the record is kept when the numbers return, because the replacement may still be in the active binlog. Recorded transactions block restores across them until excluded. Overlap verification of rows whose coverage is not bound to their `id` runs even without retention; unresolved issuance blocks unsafe compatibility and retirement. A true reuse guard compares conflicting issuance of the same GTIDs; names, retries and publication order are not evidence. Verified reuse still requires a new generation even after quarantine. For an obsolete upload-order guard, `pitr-quarantine --acknowledge-history` verifies every active stream is covered by a canonical full-backup bound, compares overlapping issuance and records prior evidence; it removes no rows and adds no global backup cutoff. Quarantine bounds restrict only windows needing excluded rows; earlier windows with a proven stop before them remain available. Prefer a new unused selected profile `storage.s3.prefix`/`storage.pvc.subPath` and full backup after RESET. Follow the repair guide for the exact procedure.

### Known limitations

- PITR can refuse ambiguous forks/empty stop frontiers, missing or corrupt
  objects, and unknown issuance provenance. Preserve evidence; recover exact
  bytes or use the canonical acknowledgement/quarantine procedure. Verified
  reuse requires a new prefix/full backup. Same-UUID numeric return cannot
  clear known loss before replacement bytes are sealed and verified.
- Original commit stamps or usable statement Query events are needed for
  issuance proof; missing observations and unrecorded historical reuse cannot
  always be reconstructed. Statement rewrites can trigger a conservative
  guard. Per-site planning does not prove cross-site completeness; verify all
  required sites rather than dropping a failing site without evidence.
- Older writers may already have lost rows, clone bases or recovery provenance.
  Preserve manifests, objects, original passphrases, compatible images and
  generation/backup pairing. Downgrades can discard those fields again; use
  the documented compatible recovery images or a new prefix/full backup.
- Inspecting rows whose coverage is not bound to their `id` costs storage
  reads/S3 egress. Restarts rebuild in-memory
  comparisons and cooldowns, repeating I/O and deferring pruning. Scheduling
  budgets do not bound network latency; reverse-ordered GTID histories still
  incur quadratic interval-index insertion. Watch archive capacity while any
  unresolved issuance blocks site-wide retirement.

## 28. PITR retention deferred because GTID coverage is unknown

### Symptoms
- `retention deferred` warnings with `backupMetadataUnknown`, `backupGTIDCoverageUnknown` or `binlogGTIDCoverageUnknown`, or `retention: coverage unavailable` with a read/identity error.
- Archive storage grows while uploads and restores can remain healthy. Unresolved issuance can pause all new retirements for the site; `archiveIssuanceUnverified` at WARN names the failed row and remedy, and `lastError` retains the failure during cooldown.

### Diagnostic steps
1. Inspect every successful, non-deleting backup matching the group's spec, across all profiles, including CRs without labels. Read `status.startTime` and `status.gtidExecuted`; the latter must be a trustworthy pre-dump lower bound. Completion time alone is not a replay boundary. A newer replica backup may cover fewer GTIDs than the oldest backup. Pending, Running, or unknown-phase group backups pause new retirements, including overlapping dumps with older snapshots.
2. From the protected pod network, inspect `/pitr-cutoff?namespace=<ns>&group=<group>&archive=<archiveID>&coverage=gtid-v1`. A proof includes the matching `coverage` marker, `retainedGTIDs` for all backups, and `cutoffTime` from the minimum known start. Missing metadata yields no cutoff and `retentionDeferredReason`. `noRetainedBackups` and `backupInFlight` are normal Info waits. The informer-backed handler checks only that the group exists and that `archive` is an ID this group creates (derived from the group's UID) at the destination of **any** entry in `status.pitrArchiveBindings`, whatever its state; it does not look at the active profile, PITR being enabled, or the rendered or observed archive. It returns 404 for an unknown group, and 404 `PITR archive is not one of this group's archives` when no binding entry's destination yields that ID, for example right after a status loss, before a plaintext binding is proven again, even though the sidecar's rendered ID is right; that wait is safe (no cutoff, nothing deleted) and ends when the binding is re-established. So for a 404, compare the sidecar's archive ID with `status.pitrArchiveBindings[].destination` (playbook 26), not with the profile. It coalesces lookups and permits one uncached lookup per transport source IP per 30 seconds. Junk query keys allocate no admission state. HTTP 429 carries no cutoff, logs `fetch retention cutoff` at Warn and retries after 30 seconds. Use direct Service access preserving pod source IPs; proxy/NAT callers share a limit and forwarding headers are ignored. Safe deferrals alone are cached; positive proofs and failed lookups are not. Older requests deliberately receive no cutoff; a new sidecar also rejects time-only responses from older operators.
3. Inspect affected manifest rows' `gtidCoverage.complete` and `gtidCoverage.gtids`. Every file transaction must be contained in every backup bound. Previous GTIDs and same-name successor files do not establish coverage. Anonymous transactions, truncation, disabled checksums, and unknown events remain retained. Complete empty files and valid ZSTD payloads immediately after GTIDs are supported.
4. The proof's `gtidCoverage.contentId` must match the row's `id`. Unbound proofs are re-inspected from streamed archived plaintext, and the bytes must match the row's `id`. Each batch reads at most eight objects, with soft 64 MiB/two-second limits, persists IDs/proofs together, and retries eligible remaining work after 30 seconds with Info progress logs. Failed identities retry only after the normal interval (one hour by default); their process-local cooldown cannot delay unrelated backfill and clears on identity change/removal or sidecar restart. Actual failed attempts and incomplete unsupported coverage remain Warn. Check archive read access and the original passphrase; no full-file inspection scratch is used. Unknown/unreadable objects remain retained. Capture `support-bundle.sh`; `backups.json` contains projected backup names, profiles, phases, times, and GTID bounds without reading Secrets.

5. Distinguish coverage backfill from issuance verification. A healthy pending issuance batch logs `archiveIssuanceUnverified` at Info. A failed row logs that reason at Warn with `error`, site/name/ID and the repair/quarantine remedy; `lastError` stays set throughout cooldown. One unresolved legacy identity can defer all new retirements for that site even while unrelated coverage backfill progresses. Repair its archive access/original bytes and allow verification to succeed, or preserve it and explicitly quarantine the exact identity behind a verified canonical backup. Do not leave a growing PVC unattended; observe free space. An unreadable manifest must retry on normal scan triggers, without a completed job causing an immediate scan loop.

### Safe remediation
Preserve files until coverage is proved. Repair missing backup metadata only from the original dump's trustworthy metadata; never invent GTIDs or replace the lower bound with an after-dump sample. Upgrade the operator before sidecars to suppress old time-only retention during rollout. Resolve archive-read failures, then allow the next sweep to retry. Unsupported or incomplete files remain retained; do not delete backups, manifests, or local binlogs to force cleanup.

## 29. Retention object deletion or acknowledgement keeps failing

### Symptoms
- Repeated `retention: delete object` warnings, or `retention: save delete acknowledgements` after a successful object delete.
- An object's live `files` row is absent while a `pruned` retirement record has `deleted: false`. The object may still exist, or may already be absent after an acknowledgement failure.

### Diagnostic steps
1. Read sidecar logs and inspect the site manifest through the configured archive reader. Active `files` rows are used for restore; `pruned` rows are retired and excluded. Pending records retain their name, original archive ID/key, and header metadata.
2. Check delete permissions/backend errors for `retention: delete object`, and manifest write access for `retention: save delete acknowledgements`. The support bundle already captures sidecar logs. The existing `sidecar-probe.sh ... archiver` reports active archival status, not a pending-delete count.
3. A pending record retries at the next retention sweep, including after process restart and before querying the operator. A successful retry writes `deleted: true`; deleting a missing object is idempotent. If a live `files` row still references the same key, the sweeper refuses physical deletion until a new proof retires that live reference.
4. Inspect compatible sidecar versions before a downgrade. Older writers drop retirement records on manifest rewrite. A missing pending record from an older release cannot be recovered by this retry mechanism; preserve evidence before considering any separate orphan audit.

### Safe remediation
Restore archive delete/write access and let the sweeper retry. Do not clear retirement records, toggle `deleted`, purge local binlogs, or remove live manifest rows to force cleanup. Stored `key` and `RemotePath` fields are not deletion authority: the sidecar derives the target from validated identity fields under its configured prefix/site. An already retired object can be deleted without a fresh backup proof; that does not authorize new retirements while coverage is unknown.

## 30. Retired local binlogs or growing retirement metadata

### Symptoms
- Older sidecars upload the same local bytes after a sweep retires them, wasting bandwidth and storage. Retirement-aware sidecars leave those bytes retired across restart and operator outages.
- Acknowledged retirement records remain while local files are still indexed/present, or `retention: compact retirement records` reports a manifest save failure.

### Diagnostic steps
1. Compare the local file's content identity (SHA-256 for plaintext storage; passphrase-keyed HMAC-SHA256 for encrypted storage) with the site's retirement record's `id`. Both pending and acknowledged records suppress matching bytes. A different content ID under the same name is new data and must remain eligible for archival; header timestamps or previous GTIDs alone are insufficient.
2. Inspect `/archiver/status` and the live `files` list. Retired records do not count toward backlog, active file count, bytes, or coverage timestamps. Zero backlog is expected for retired local bytes and still does not prove all historical coverage (playbook 27).
3. Verify the site's sidecar is running and can access its archive. Retention is independent of primary/replica status, role-query success, or sealed-file availability. A removed site without a sidecar has no remaining owner to maintain its archive.
4. Before expecting compaction, check `deleted: true`, a readable local binlog index without the name, and physical absence of the file. An unindexed file still on disk protects the record. Missing/unreadable indexes or filesystem errors defer compaction; pending deletes remain durable regardless of local-file state.
5. For a compaction save warning, check manifest write access and sidecar logs. The old durable records remain and a later scan retries the update. Existing support-bundle logs and archiver probes capture the relevant status; they do not expose a retirement-record count.

### Safe remediation
Upgrade older sidecars that ignore retirement records, restore archive access or local-index readability, and let maintenance retry. Do not clear records or issue `PURGE BINARY LOGS` to make bookkeeping disappear: local files support replication catch-up. The operator defaults local expiry to 14 days; remote retention can be shorter. Preserve a separate archive-lifecycle plan before removing the last sidecar for a site.

## 31. PITR enabled, pods Ready, `PITRArchiveReady=False`

### Symptoms
- `kubectl describe mfg <group>` shows `PITRArchiveReady=False` and a Warning `PITRArchiveNotReady` Event, while `Ready=True`, every MySQL pod is Ready, and applications are unaffected.
- `status.pitr.message` lists one or more `site: error` entries; `lastArchivedTime` / `newestArchivedTime` stop advancing for that site.

### Cause
Binlog archiving has stopped on at least one site. Archiving state is deliberately not coupled to pod readiness or liveness: an archive problem never removes a pod from Services or stops self-fencing. The condition reason names the class:
- `ArchiveStoreInitFailed`: the sidecar cannot build its archive store (unreadable passphrase or S3 credentials file, invalid S3 endpoint URL, empty PVC mount path, PVC mount path that is not a mount point or not writable). Immediate; the sidecar retries on its own with backoff from 5 s up to 5 min.
- `ArchiverError`: the store is up but uploads or manifest reads fail on 2 consecutive archiver scans (`consecutiveFailedScans` ≥ 2 in `/archiver/status`), or the same error stays for 10 minutes without a scan completing. A single failed scan never sets it. The site's history audit reports through the same reason: a recorded GTID reuse (`verified GTID namespace reuse …`, playbook 41), an issuance it cannot verify (`verify archived GTID issuance …`, playbook 28) and a server that could reissue archived GTIDs (`… no longer has archived transactions … that it issued …`, playbook 41). Divergent rows alone are not an error: the condition stays `True` and its message lists them (`...; <site>: N divergent rows (...)`), see playbook 27.
- `ArchiverDisabled`: the sidecar reports PITR disabled on 2 consecutive polls while the spec enables it, and no ordered update is pending for the site, or the PITR spec cannot render an archiver (empty or unknown `profileName`), or the rollout has been pending for 30 minutes. While a valid rollout is pending, for up to 30 minutes, the condition is `Unknown`/`AwaitingRollout` instead, with no Event; the same applies for the first sync after a topology manager starts, before its drift set is computed.
- `SidecarOutdated`: an enabled archiver reports no archive-identity block, so the pod runs a sidecar older than v1.7.0. Either `spec.sidecar.image` pins one (while that site's ordered update is pending, at most 30 minutes, the condition is `Unknown`/`AwaitingRollout`; after that, `False` on the second poll; upgrade or clear `spec.sidecar.image`), or, right after the upgrade to v1.7.0, the site Deployments' update is deferred (no `pitr-render` annotation, log `pitr render deferred`; playbook 26) and the old pods keep their old sidecars with no pin set. Then `spec.sidecar.image` is not the remedy: point `spec.backup.pitr` at a new prefix and the Deployments roll.
- `SidecarUnreachable`: the operator's poll of `/archiver/status` failed 3 times over at least 60 s, or over at least 5 minutes for a sidecar that has not answered since the operator process started. With a sidecar image older than this behaviour, this is how an archive init crash loop shows up.
- Archive identity refusals (`ArchiveForeign`, `ArchiveCopy`, `ArchiveLegacy`, `ArchiveDescriptorMissing`, `ArchiveEncryptionMismatch`, `ArchiveKeyMismatch`, `ArchiveVersionUnsupported`, `ArchiveStoreUnsupported`): the sidecar refused the archive's `archive.json` descriptor or the store's atomic create. Immediate, and they do not clear by retrying. See playbook 39.
- `ArchiveUnsafeHistory`: a site persisted its permanent history-integrity marker (Warning `PITRArchiveUnsafeHistoryDetected`). The condition does not preview `spec.mysqlConf`: unsafe keys there are refused by `PITRMysqlConfAccepted` instead, so a marker comes from an out-of-band change. See playbook 40.

When sites disagree, the reason is the first that applies in the order `ArchiveStoreInitFailed`, the archive refusals in the order above, `ArchiverError`, `SidecarOutdated`, `ArchiverDisabled`, `SidecarUnreachable`; the message lists every affected site. `status.pitr.archive` summarizes the identity: `state` is `AwaitingDescriptor`, `Verified` or `Refused` with `reason`.

Expected, self-clearing Warning `PITRArchiveNotReady` → Normal `PITRArchiveRecovered` pairs (no action unless the Normal never follows):
- a pod taking more than 60 s to come back in a rollout or node move (`SidecarUnreachable`);
- a new group or bootstrapped site taking more than 5 minutes to answer (`SidecarUnreachable`);
- enabling PITR with `updateStrategy.type: Recreate` (`ArchiverDisabled` until the pods restart);
- two failed scans in a row followed by a good one (`ArchiverError`);
- MySQL taking more than about a minute to accept connections after a pod start, for example a long InnoDB crash recovery (`ArchiverError` with `role check: ...`);
- a valid ordered update that is still progressing 30 minutes after a site first reported archiving disabled, for example a slow multi-site rollout after enabling PITR (`ArchiverDisabled` with `ordered update still pending after 30m`, then a Normal once the site rolls).

The PVC store checks its volume when it is built: the mount path must be a mount point in the sidecar's `/proc/self/mountinfo`, and the sidecar must be able to create a file under the archive prefix. A PVC volume left out of the pod spec therefore fails as `ArchiveStoreInitFailed` with `pvc archive store: <path> is not a mount point (it does not exist)` (the image has no such directory) or `... is not a mount point (it is inside <mount>, fstype <type>)`; an unwritable volume fails with `... is not writable`. A declared volume whose claim cannot be bound or mounted leaves the pod Pending, which shows as `SidecarUnreachable`. `PITRArchiveReady=True` still does not prove the archive is on a PVC: any volume mounted at the path passes (an `emptyDir` too), and the check runs only when the store is built, so check the pod's volume source and claim.

### Diagnostic steps
1. Read the condition and the per-site message:
   ```bash
   kubectl get mfg <group> -n <ns> -o jsonpath='{range .status.conditions[?(@.type=="PITRArchiveReady")]}{.status} {.reason}: {.message}{"\n"}{end}{.status.pitr.message}{"\n"}'
   ```
2. Probe the sidecars: `scripts/sidecar-probe.sh -n <ns> <group> archiver`. `storeReady:false` with `lastError: archive store init: ...` confirms `ArchiveStoreInitFailed`; `uploadFailures` rises once per failed attempt. For `ArchiverError`, `consecutiveFailedScans` and `lastScanAt` show whether scans keep failing or have stopped completing.
3. Read the sidecar log for `failed to init archive store` (ERROR when the cause changes, WARN while it repeats, with `attempt` and `retryIn`) or `archive binlog` errors.
4. If `PITRArchiveReady` does not change while `status.pitr.message` does, look for the operator log `PITR archive condition read failed; leaving the condition as persisted`: the operator cannot read the group uncached and keeps the persisted condition until it can.
5. For `SidecarUnreachable`, check `kubectl get pod <pod> -n <ns>` restart counts and `kubectl logs <pod> -c sidecar --previous`, and the sidecar image tag.
6. For an `Archive*` reason, read `status.pitr.archive` and each site's `/archiver/status` `archive` block (`scripts/triage.sh` prints both), then go to playbook 39 or 40.

### Safe remediation
- `ArchiveStoreInitFailed`: fix what the error names (Secret projection mode or content, endpoint URL, PVC mount path). The next retry picks it up; look for `archive store initialized after retries`. No pod restart is needed for Secret or endpoint fixes. For `is not a mount point`, compare the pod's `volumes`/`volumeMounts` with the Deployment's pod template: the operator always renders the `pitr-archive` volume, so a missing one was removed by a mutating webhook, policy engine or manual edit. Fix that; restoring the volume changes the pod spec and restarts the pod, which also restarts MySQL, so on the primary site run a planned failover away from it first (`kubectl bloodraven promote <group> <other-site>`). For `is not writable`, the error ends with the cause: `permission denied` means ownership or fsGroup (compare the volume's owner with the sidecar UID, 999 unless overridden); `read-only file system` means the claim's access mode or a filesystem the kernel remounted read-only after an I/O error (check the node's `dmesg`); `no space left on device` means the volume has no free inodes (expand or prune it). The probe creates an empty file, so a volume that is only out of data blocks does not fail init; it shows as `ArchiverError` while retention keeps freeing space. For passphrase `permission denied`, see playbook 25. Do not change an existing archive's passphrase to make the error go away; see playbook 26.
- `ArchiverError`: follow the error text (storage reachability, credentials, playbook 26 for authentication or BRV1 errors).
- `AwaitingRollout`: let the ordered update finish; check `status.updatePhase`. If it stays, the update is blocked (operator log `ordered update: deferring active-site update`, for example no healthy promotable standby, or `ordered update: deferring follower update; no drifted follower is a healthy direct replica` when only followers are drifted and none replicates directly from the active site yet, such as an old primary recovery has not rejoined). After 30 minutes the condition turns `False`/`ArchiverDisabled` with `ordered update still pending after 30m`; unblock the update (restore the standby) rather than restarting pods by hand.
- `ArchiverDisabled`: if it persists after the rollout, check the sidecar log and image first. Admission refuses `pitr.enabled: true` without a `profileName` naming an existing profile, so an empty or unknown profile appears only on an object stored before v1.7.0 tightened the CRD (look for a `BackupPITRInvalid` Event); with one the operator renders no PITR configuration into the sidecar, so it reports archiving disabled indefinitely. Fix the reference with the next edit, which admission now checks.
- `SidecarOutdated`: if `spec.sidecar.image` pins a pre-v1.7.0 sidecar, upgrade or clear it before pointing `spec.backup.pitr` at a new prefix; the ordered update rolls it. Without a pin it is the deferred v1.7.0 render: the new prefix itself is the remedy.
- `ArchiveStoreInitFailed` with `archive store init: … s3:ListBucket`: grant `s3:ListBucket` on the bucket (and `s3:GetObject`/`s3:PutObject`/`s3:DeleteObject` on the prefix); the sidecar lists the prefix before creating the descriptor. An inconclusive conditional-write probe (persistent `409 ConditionalRequestConflict`) also shows here and is retried on the next cycle.
- Archive identity reasons and `ArchiveUnsafeHistory`: playbooks 39 and 40.
- `SidecarUnreachable` on an older sidecar image: upgrade the sidecar image; the operator rolls it through the ordered update. The ordered update stops on a rolled pod that is not Ready (playbook 34), but a current sidecar stays Ready through archive failures, so watch `PITRArchiveReady` site by site instead of assuming a finished rollout means archiving works.
- Recovery to `True` needs 2 consecutive clean poll rounds (about a minute) and emits a Normal `PITRArchiveRecovered` Event ("PITRArchiveReady is True again: every site reports …"); the Warning reads "PITRArchiveReady is False (<reason>): <message>". Binlogs sealed during the outage are uploaded once the archiver runs, as long as MySQL still has them; check `backlogFiles` and the coverage fields afterwards.

---

## 32. PITR restore or verification refused (archive identity, dump, target, coverage)

### Symptoms
- `status.restore.reason`, or the verification's `Verified` condition reason, is a `PITR*` token, and the message starts with it. The failing step may be the `dump-meta` or `pitr-download` init container (before MySQL Shell starts) or `restore.py` in the main container.
- A restore with `allowPartial: true` completes with `status.restore.provenance.pointInTime.outcome: Partial` and a Warning Event `PITRReplayTruncated`: the archive held nothing at or after the target, so it replayed to the archive head.
- `PITRArchiveSiteOmitted`, `PITRCrossSiteOrder`, `PITRBackupBoundUnknown`, `PointInTimeCrossSiteUnverified` and `PITRSharedArchivePrefix` no longer occur. `PITRReplayUnsupported` now means only XA in the replay, an event the planner cannot certify `mysqlbinlog` replays (`LOAD DATA`, an incident, an unknown event type) inside a replayed transaction, an anonymous GTID, a transaction without an original commit time, or no `mysqlbinlog`.

### Diagnostic steps
1. Read `kubectl get mfg <group> -n <namespace> -o jsonpath='{.status.restore}'` or `kubectl get mysqlbackupverification <verification> -n <namespace> -o yaml`, the Events, and every container's termination message:
   ```bash
   kubectl get pod <job-pod> -n <namespace> -o jsonpath='{range .status.initContainerStatuses[*]}{.name}: {.state.terminated.message}{"\n"}{end}{range .status.containerStatuses[*]}{.name}: {.state.terminated.message}{"\n"}{end}'
   ```
   The earliest failing container's message is the cause. A successful main container records `BLOODRAVEN_PITR_VERDICT v=2 …` and `BLOODRAVEN_PITR_RESULT v=1 …`. `kubectl logs <job-pod> -c dump-meta` (`dump metadata`, or `read dump metadata` with the token) and `-c pitr-download` (`pitr plan`, or `download site binlogs` with the token, plus `BLOODRAVEN_PITR_DIVERGENT_SET gtids=` with the full set to exclude) add detail. `scripts/triage.sh` prints `status.restore.provenance.pointInTime` and each verification's `status.pointInTime`.
2. **Reconcile-time refusals** (`RestoreBuildFailed`, no Job; a verification records the token as its `Verified` reason): `PITRTargetInvalid` (unparsable target, a fractional second, a missing or unknown `archiveProfileName`, an `excludeGtids` entry that does not start with a server UUID or a canonical set over 64 KiB, or a verification with `pointInTime` and no `archiveProfileName` against a group with `spec.backup.pitr` disabled: "... so group <g> has no active PITR archive to replay; set pointInTime.archiveProfileName to the profile of the archive to replay (a retained archive needs no writer), enable spec.backup.pitr, or verify without pointInTime"), `PITRBackupIdentityUnknown` (no recorded source group UID, or a direct S3/PVC source at the root of its store), `PITRBackupFiltered` (`loadOptions.includeSchemas`/`excludeSchemas` with `pointInTime`), `PITRArchiveDestinationShared` (`archiveProfileName` resolves to the group's own active PITR destination, compared normalized).
   - `PITRArchiveBindingNotReady`: the archive's **read binding** is not `Bound`, so no Job is created; the message carries the binding's reason. Read the entry for the archive profile's destination in `status.pitrArchiveBindings` and follow playbook 26. It **waits** (restore `status.restore.phase: Pending`; verification requeued every 15 s) while the binding is being proven (`Authenticating`) or is refused for a fixable reason: `KeyMismatch` or `ArchiveEncryptionMismatch` (put the archive's original passphrase, or its encryption settings, into the archive profile; the check reruns when the candidate changes), `Pending/Authenticating` with an `ArchiveListFailed: ...` message (grant list access; retried every minute), a `Bound/KeyMaterialLost` snapshot (restore the snapshot Secret from a backup, or put the original bytes back into the profile's Secret so it is re-created), and for a restore `BindingLimitReached` (release an unused binding). It **fails** when the archive can never verify (`ArchiveNotFound`, `ArchiveDescriptorMissing`, `ArchiveLegacy`, `ArchiveVersionUnsupported`: playbook 39), and a verification also fails on `BindingLimitReached`. A restore into a new group cannot use the source group's key snapshots: if the source's original Secret is gone, the archive profile must name the source's retained snapshot (`key: passphrase`).
   - `PITRBackupDumpKeyMissing`: the dump is encrypted and its key is not available: the Secret the backup recorded in `status.artifactStorage.passphraseSecretKeyRef` (its dump-key snapshot) is gone, or the backup recorded none (taken before v1.7.0). A restore takes `initFromBackup.decryption.passphraseSecretKeyRef` (or `restoreInPlace.decryption`) holding the original dump passphrase, or the restored snapshot Secret; a verification has no override, so restore the snapshot Secret, or verify a newer backup. A verification fails with it at launch, before any Job: "backup <b> is encrypted and the Secret <s> it recorded as its dump key is gone; restore that Secret". The profile's live Secret is never a fallback.
3. **Archive identity** (`pitr-download`, checked before any planning, so they name the real problem; `dump-meta` refusals in step 4 come earlier still, because that container runs first):
   - `PITRArchiveNotFound`: no descriptor and no manifests. Wrong `archiveProfileName`, or the archive is empty.
   - `PITRArchiveLegacy`: the prefix predates v1.7.0. This build cannot read it; restore with a pre-v1.7.0 image if the target needs it.
   - `PITRArchiveDescriptorMissing`: version 2 manifests but no `archive.json`. For a replicated copy, wait for replication or copy `archive.json` from the source. Never write one by hand.
   - `PITRArchiveIdentityMismatch`: a manifest names another archive, or the dump's stamp names another archive or source group than the archive read. Point at the archive the backup was taken against (`MysqlBackup.status.pitrArchiveID`).
   - `PITRArchiveEncryptionMismatch` / `PITRArchiveKeyMismatch` (from `pitr-download`, after the read binding was `Bound`): the Job reads with the read binding's mode and key snapshot (`status.pitrArchiveBindings[].encryption.keySecretName` for the destination, the Secret the Job's `bloodraven.shipstream.io/pitr-key-secret` label names), not the profile's Secret, so editing the profile changes nothing here. The descriptor no longer matches what was accepted: it was replaced after the binding became `Bound`, or the snapshot's bytes changed (then the binding turns `KeyMaterialLost`). Compare the snapshot's acceptance (`pitr-accepted` annotation, metadata only) with the descriptor, restore the original `archive.json` if it was replaced, and see playbook 26.
   - `PITRArchiveVersionUnsupported`: written by a newer release (or an unknown encryption mode): upgrade. If the message says `archive.json is damaged …` (undecodable, trailing data, wrong kind, a malformed field, source names outside Kubernetes bounds, over 64 KiB), restore the original object (bucket versioning, a copy) or point the profile at a new prefix; upgrading does not help.
   - `PITRArchiveHistoryReset`: a reset epoch (including verified GTID reuse) refuses the whole archive. New prefix plus full backup; older targets need the pre-reset generation. See playbook 41 for the reclone cause.
   - `PITRArchiveListFailed`: only a listing refused for permission; the message names the cause: S3 403 (`AccessDenied`), grant `s3:ListBucket`; a PVC permission error (`EACCES`), make the archive volume readable by the Job's user. Other listing and manifest read failures (S3 5xx, I/O errors) are retried in the container and, when they persist, end the container without a token; they are never `PITRArchiveLegacy`. A row "retired" while the Job planned (`the archive changed during planning (row retired); retry`) is transient too: retention pruned it after the Job read the manifests.
4. **The dump** (`dump-meta`): `PITRBackupIdentityUnknown` (no backup stamp: the dump predates v1.7.0 or was not taken by a Bloodraven backup Job; take a new backup), `PITRBackupIdentityMismatch` (the stamp's backup or source UID differs from the `backupRef`, or `status.gtidExecuted` is not in the dump's `@.json`), `PITRBackupInconsistent` (`@.json` has no `gtidExecuted`, or is marked `gtidExecutedInconsistent`), `PITRBackupFiltered` (schema-filtered dump), `PITRBackupIncomplete` (no `@.json` or `@.done.json`, or the stamp has no `serverClock.end`: the dump did not finish). An encrypted dump read without a passphrase (`… is encrypted …, but the restore has no decryption passphrase …`), a plaintext dump read with one (`… is not encrypted, but the restore decrypts this dump …`) and a wrong passphrase (`… does not decrypt with the configured passphrase …`) refuse `PITRBackupIdentityUnknown`: fix the restore source's encryption setting, not the backup. `larger than its bound` (a stamp over 1 MiB: `PITRBackupIdentityUnknown`; `@.json`/`@.done.json` over 16 MiB: `PITRBackupIncomplete`) means the object was replaced. A `dump-meta` failure without a token is a store error that persisted through its retries (about 31 s): check the store and retry. The dump's end counts as its whole second plus 0.999999 s (mysqlsh truncates it), so a target inside the dump's last second refuses `PITRBackupAfterTarget`.
5. **The target** (`pitr-download`):
   - `PITRExcludeIntersectsBackup`: `excludeGtids` overlaps the backup. Use a backup taken before the excluded transaction, or on a site that never executed it.
   - `PITRBackupAfterTarget`: the dump finished after T, the backup holds a commit at or after T, or T is earlier than the dumped server's `serverClock.end + 1 s` while the backup's newest commit time is not proven from the bytes (some backup transactions are no longer archived, or "proven only up to …": rows holding them were not read and their manifest hints are unverified). The message names the bound. Use an earlier backup, or a later target.
   - `PITRTargetNotCovered`: nothing outside the backup and `excludeGtids` is archived at or after T. The active binlog is archived only after rotation: run `FLUSH BINARY LOGS` on the source, wait for the upload, and retry; or set `allowPartial` on a restore.
   - `PITRStopAmbiguous`: two candidates at the same microsecond on different histories ("choose a target ≠ …"), clock skew around T, or a witness archived only as a replica copy whose predecessors are not proven on its origin (the message names the GTID to exclude, or the promotion-record window and the safe targets). An anchor is also set aside when a later record of its site (promotion or incarnation `Reset`) may predate the witness, or when another server's promotion record between the anchor and the witness does not hold it; the message says which record (`gtid-audit.sh --manifest` prints each record's kind, `observedAt`, `probeInterval`, promotion set and confirmation). A witness in a new primary's first seconds (committed before its promotion record was observed, or within 5 s after it) has no anchor and refuses with the window and the safe targets, whatever the record carries: the record's promotion set and confirmation are recorded, and the planner does not anchor with them in this release, so nothing about the record (a missing promotion set, a missing confirmation, a failed promotion-time rotation) is the cause, and nothing can be repaired. Pick a target the message offers (at or before the promotion, or after the record plus 5 s), or exclude the named GTID only when it is errant. `The confirmation of the promotion record …, observed at …, holds <w>, yet the archived <w> was committed at …, after it` means a record of the witness's own server saw that GTID before the archived transaction was committed: the GTID was issued twice (a server that lost its binlog tail), or the node's clock was set back. No exclusion lifts it; pick a target before `<w>`, and after a reissue replace the archive. A replica copy that an anchor accepts is also held to that anchor, and refuses when the copy's server and the origin did not have the same history, which is what a lost binlog tail leaves behind (an old primary that crashed with an unsynced tail, rejoined by replication and never got its own transactions back; promoted again it issues their GTIDs to other transactions): `Its copy in <row> does not follow <gtids>, which <w>'s origin had executed before it` (the copy is on the server that lost them), and `Its copy in <row> follows <gtid>, which is outside the promotion set … Yet the copy of <gtid> in <row> was committed at …, before that episode can have begun …: the GTID was issued twice` (the copy is on a replica that kept the lost transactions and skipped the ones issued again; this check needs the promotion record of the episode that issued them again to carry a promotion set, and without one such a restore is not refused). Each ends with `Exclude <gtids> (the restore then holds none of them)`: that is the remedy when the named transactions are the lost ones (compare with `gtid-audit.sh` on both sites: the server that lost them lacks them or holds them with a later commit time); otherwise choose a target before the loss. After a reissue the archive should be replaced: start a new archive prefix and take a full backup (playbook 41 is the same hazard for a site that had archived the lost transactions itself).
   - `PITRBackupNotOnLineage`: the backup holds transactions that are not on the target history, or an injected empty transaction's GTID that the backup server executed with effects (take the backup on the site that injected it).
   - `PITRArchiveDivergentHistory`, `PITRArchiveSplitBrain`, `PITRArchiveGap`, `PITRArchiveOrderConflict`, `PITRArchiveOrderInversion`, `PITRArchiveNamespaceReuse`: playbook 42.
   - `PITRArchiveUnsafeHistory`: the plan needs a site with a history-integrity marker. Playbook 40.
   - `PITRPlanTooLarge`: the message names the limit. Since issue #266 the planner spills its per-transaction data (index, copy comparison, plan search, order check) to unlinked files under `.pitr-spill` on the staging volume once they pass about half of its memory budget, so a large window refuses on memory only for what cannot spill.
     - `… of memory for data it cannot spill to the staging volume (<what>; …)`: GTID sets, manifest rows or source identifiers exceed the budget (60% of the container's memory limit, 512 MiB without one). Raise `spec.backup.resources.limits.memory`.
     - `planning must spill … more to the staging volume … sizeLimit of …`: raise `spec.backup.stagingVolumeSizeLimit` (about 150 bytes per downloaded transaction, counted per site copy; the spill is gone before staging, so size for the larger of the spill and the staged files), or raise the memory limit so that less is spilled.
     - `planning must spill … the node filesystem's floor …` or `the staging volume ran out of space while planning spilled to it`: the node lacks ephemeral storage; run the Job on a node with more.
     - `… which fit neither limit: the memory budget of … has … left, and planning must spill …`: both are exhausted; the second half is the staging refusal above, and either remedy works.
     - The event-size bound: raise the memory limit while the message says so; at the fixed 64 MiB cap more memory does not help: choose a target whose plan does not read that file.
     - The staged files: the staging volume's `sizeLimit` (raise `spec.backup.stagingVolumeSizeLimit`), or the node's free ephemeral storage, which staging keeps above max(10 %, 256 MiB) (`… would leave less than its floor …`: run the Job on a node with more).
     - `the plan search covers … positions`: more than 2³¹ − 1 transactions in the files the search considers; only a backup closer to the target helps.
     - A backup closer to the target helps in every case. A restore that planned logs `memoryPeakBytes`, `spilledPeakBytes` and `spillDiskPeakBytes` on its `pitr plan` line (init container `pitr-download`), which is what to size from. A `pitr-download` that ends with `pitr plan: … spillfile: … I/O error` or `write-back failed` (no token) means the node's disk failed under the spill: retry on another node.
   - `PITRArchiveObjectInvalid`: an archived object is missing or does not match its digest, framing, CRC, hints or parse; also a manifest row whose `size` disagrees with its authenticated bytes, a manifest key naming a site outside `^[a-z0-9-]+$`, and a file that ends inside a transaction or has a transaction without its terminator (an event after a terminator, an `INCIDENT` say, is `PITRReplayUnsupported`). Preserve it and inspect. Restore the object (in an archive copy, copy it again from the source archive); if it is lost at the source, quarantine the row there with `bloodraven pitr-quarantine`, and let a copy take the source's updated manifest. On a replicated copy, first check the object has arrived.
6. **The replay** (`restore.py`): `PITRCoverageInvalid` (`coverage.json`, the staged files or a plan check do not match the request; also "pod is gone" when the Job's pod was deleted before the operator read its verdict, which is final), `PITRNoArchivedReplay` (latest or partial mode with nothing beyond the backup: rotate on the source and retry), `PITRReplayMismatch` (the server's `gtid_executed` after replay is not the planned target history: preserve the Job and its `coverage.json` and report it), `PITRReplayFailed` with the message `BLOODRAVEN_PITR_FAILED: mysqlbinlog|mysql exit=<rc> segment=<k>` (read the main container log around `BLOODRAVEN_PITR_SEGMENT k=<k>`; for `mysql`, `<k>` is where the failure was detected and the failing statement may be in an earlier segment), `PITRReplayFailed` with `BLOODRAVEN_PITR_FAILED: cannot open a session …` / `cannot read the replayed gtid_executed …` (the target became unreachable for a proof: operational, retry on an emptied target), `PITRCoverageInvalid` saying the target `already holds transactions of other servers` or that its `server_uuid … is a server of the restored history` (the target is not fresh: empty it by deleting its pod and PVC together; its own first-boot transactions are fine and are logged as `BLOODRAVEN_PITR_TARGET_LOCAL`), `PITRCoverageInvalid: R2 …` / `PITRReplayMismatch: R6 … extra <target uuid>:…` (something wrote to the target during the load or the replay; the operator defers its credential writes and `MysqlDatabase` DDL until `initFromBackup` succeeds, so look for a manual or external writer), `PITRReplayUnsupported` (XA in the replay or no `mysqlbinlog`; choose a target before the XA transaction), `PITRArchiveIdentityMismatch` (the staged plan's archive does not belong to the dump's source group or stamp; a Job rendered by another operator version, or a tampered staging directory), `PITRExcludeIntersectsBackup` (the server's own GTID functions found `excludeGtids` inside the baseline).
7. For `PITRReplayTruncated`, compare `status.restore.provenance.pointInTime.stopTime` with `newestReplayedCommitTime`, and confirm the earlier point meets the recovery objective before routing traffic.

### Safe remediation
Correct the cause and retry through the normal restore/verification workflow. A failed bootstrap restore has no automatic retry. A cause in the request itself (the target, `excludeGtids`, `allowPartial`, `archiveProfileName`, the load filters) cannot be fixed by editing it: create a new `MysqlBackupVerification` (its spec is immutable), or, for `spec.initFromBackup` (fixed at creation), delete the never-bootstrapped group and create it again with the corrected block, after checking that `status.restore.phase` never reached `Succeeded`; delete it as in site docs Operations, "Deleting a group without losing backups" (foreground, after its own backups are gone) and re-apply only once its PVCs and backups are gone (site docs: Backup and restore, "A failed bootstrap restore"). `restore.py` refusals after the load (`PITRCoverageInvalid` from R2, `PITRReplayMismatch`, `PITRReplayFailed`) leave the target loaded: empty it (delete the group's MySQL pods and PVCs together and wait until the re-created claims are `Bound` and the pods `Running`) before deleting the failed restore Job to retry. Refusals in `dump-meta` and `pitr-download` leave the target untouched. `allowPartial` relaxes only the missing-witness case. Do not delete manifest rows, rewrite `archive.json`, rewrite archived timestamps, or point a restored group's own PITR profile at the source archive.

**Upgrading:** a Job rendered by an older operator writes no version 2 verdict and fails `PITRCoverageInvalid`; delete it and retry. Backups taken before v1.7.0 refuse `PITRBackupIdentityUnknown`.

---

## 33. A healthy replica reports divergent transactions, or an ordered update/planned failover refuses a divergent target

A replica can apply every new transaction from the primary while holding transactions the primary never had: an earlier lossy promotion (issue #202, v1.5.0 ordered updates), a write made on the replica without `sql_log_bin=0`, a split-brain loser repointed by hand, or a restore of stale history. Its IO and SQL threads stay healthy until a conflicting row stops it. Since v1.6.0 the operator audits every read-only follower with a replication channel against the confirmed primary (follower read first, then the primary; about every 30s and on the first poll after start or a primary change), records the extra set, and will not promote that site in an ordered update or a planned failover. Emergency failover is never refused: it ranks divergent sites last and promotes one only when no other candidate remains.

### Symptoms
- `Degraded=True`, reason `DivergentTransactions`, message naming the site, count and set; `Ready` stays `True` and the deployment API keeps granting leases (an old primary's divergence, `divergenceDetectedBy=OldPrimaryRecovery`, still makes it answer `423 unstable`).
- `status.sites[<site>]`: `recoveryState=RecoveryBlocked`, `divergenceDetectedBy=ReplicaAudit`, `divergentGtid`, `divergentTransactionCount`, `divergenceComparedTo=<primary>`; `replication` often `Healthy` or `Lagging`.
- Warning Event `DivergentReplicaDetected`; WARN `divergence detected` with `detectedBy=ReplicaAudit`; `bloodraven_divergent_transactions{site} > 0`.
- Ordered update: `ordered update failed` with `<site> is divergent (N transactions the primary lacks); reclone or reconcile before the active site can be updated`, WARN `ordered update: active-site update stalled; standby is divergent`, or WARN `maintenance handoff refused; candidate has transactions the source lacks`. The active site keeps serving, unfenced, with its old spec. During the v1.6.0 upgrade itself this stall is expected on installs damaged by #202.
- Planned failover: `status.plannedFailover.result.reason=DivergentTarget` (refused before fencing, or rolled back from the lag wait with `rollback.reason=DivergentTarget`).
- After an emergency: `DivergentPromotion=True` condition and Warning Event `DivergentSitePromoted` (a divergent site is now primary).

### Diagnostic steps
1. Confirm the set independently, follower first:
   ```bash
   ./.agents/skills/bloodraven-doctor/scripts/gtid-audit.sh -n <ns> <group>   # exit 2 = DIVERGENT, 3 = UNKNOWN
   ```
   It reads the follower's `@@GLOBAL.gtid_executed`, then runs `SELECT GTID_SUBTRACT('<follower set>', @@GLOBAL.gtid_executed)` on the confirmed primary, and reports UNKNOWN (not clean) on any failed read, a non-writable primary, or a `server_uuid` change between the reads.
2. Decode what the transactions are, on the divergent replica, before anything destroys them:
   ```bash
   kubectl exec -n <ns> <replica-pod> -c mysql -- sh -c 'MYSQL_PWD="$MYSQL_ROOT_PASSWORD" mysql -uroot -N -e "SHOW BINARY LOGS"'
   kubectl exec -n <ns> <replica-pod> -c mysql -- sh -c 'mysqlbinlog --include-gtids="<divergentGtid>" --base64-output=DECODE-ROWS -vv /var/lib/mysql/<binlog files>' > errant-<site>.sql
   ```
   Keep `errant-<site>.sql` with the incident. A GTID UUID equal to the replica's own `@@server_uuid` means a local write on the replica; the old primary's UUID means writes the primary lost in a promotion (#202).
3. Ask the data owner which outcome is right. Treat the transactions as real data until proven otherwise: a GTID set cannot tell a stray `CREATE USER` from an order.

### Safe remediation
Pick one. Both end with the audit clearing the block on its own (INFO `divergence cleared: replica holds no transactions the primary lacks`, Event `DivergenceResolved`, `Degraded` back to `Healthy`) within about 30s, and a stalled ordered update then resumes without further action. Re-annotate a refused planned failover.

1. **Discard** (the transactions are not needed, or have been re-applied by hand): reclone the replica from the primary. The plugin builds the value, with the group's name and uid and the required `divergentGtid` prefix:
   ```bash
   kubectl bloodraven reclone <group> <site> -n <ns>
   # equivalent annotation: bloodraven.shipstream.io/reclone-site=<site>:confirm=<group>:uid=<group uid>:gtidPrefix=<first 8+ characters of the divergentGtid's source UUID>
   ```
   (`reclone-site=<site>:confirm=<group>:uid=<group uid>` is the cold form for a site with no recorded `divergentGtid`.) The replica is unavailable for the clone and the group has one copy less until it finishes.
2. **Keep** (the transactions belong in the primary's history): make the primary contain the set. Either replay the decoded events on the primary (review for conflicts first), or, when the transactions must not change the primary's data, commit one empty transaction per GTID on the primary:
   ```sql
   SET GTID_NEXT='<uuid>:<n>'; BEGIN; COMMIT;
   -- repeat for every GTID in divergentGtid, then:
   SET GTID_NEXT='AUTOMATIC';
   ```
   Empty transactions make the sets agree without moving the rows: the replica keeps rows the primary lacks. Use them only for transactions whose effects are absent from both, harmless, or already reconciled.

If an emergency failover promoted a clean site over a divergent one and the new primary was then fenced back by its own sidecar, the operator re-asserts it without holding the divergent peer's recorded `divergentGtid` against it (those transactions stay on the peer, which keeps its report). Anything the peer holds beyond its recorded set still blocks the re-assert: compare with `gtid-audit.sh` and resolve as above.

Do not:
- Force the update or the planned failover through (for example by editing status or repointing replication by hand): promoting the replica would make its extra history authoritative and the primary's newer writes collide with it.
- Expect replication restarts to fix it: thread health does not clear the block, and there is no setting to ignore divergence.
- Run maintenance on a replica with binlogging on. Use `SET SESSION sql_log_bin=0` for replica-local work.

### After an emergency promotion of a divergent site (`DivergentPromotion=True`)
The site was the only candidate left, and its extras are now the primary's history. A later divergent promotion is appended to the same condition (one record per line, newest last, the five newest kept); each also has its own `DivergentSitePromoted` Event. Review every set named in the condition with the data owner (step 2 above, on the new primary), then remove the condition:
```bash
kubectl get mysqlfailovergroup <group> -n <ns> -o json | jq '.status.conditions | map(.type) | index("DivergentPromotion")'
kubectl patch mysqlfailovergroup <group> -n <ns> --subresource=status --type=json -p '[{"op":"remove","path":"/status/conditions/<index>"}]'
```
The operator does not set it again for the same promotion.

---

## 34. Ordered update stalled on a pod that is not Ready (`OrderedUpdateStalled`)

The ordered update reaches MySQL through each site's `-internal` Service, which publishes not-ready addresses, so MySQL can answer and replicate while the pod is out of every application Service. A rolled site therefore counts as updated only when its new pod is also `Ready` (every container) and has been continuously for 30s, counted from the later of the pod's last transition to `Ready` and its newest container start (native sidecar init containers included). A crash-looping container passes its readiness probe between crashes, so a pod that is `Ready` at one moment is not enough. Before it rolls another site or hands off the active site, the operator checks again, by the same rule, that every site already running the new pod template is `Ready`, including the site it just rolled and one an earlier attempt rolled. A template that breaks pods stops at the first site it broke.

### Symptoms
- `Updating=True` with reason `OrderedUpdateStalled` (still updating, stopped); the message names the site, the pod and why it is not Ready, for example `site iad was updated but its pod mysql-<group>-iad-<hash> did not become Ready within 5m0s (sidecar not ready: crash looping, last exit code 1 (Error))` or `site iad already runs the new pod template but its pod ... is not Ready (...); not handing off the active site pdx`. A restarting container reads `crash looping, last exit code …` in every phase of its loop (back-off, running, `Ready` again for a few seconds, terminated), so the message stays the same across retries while the cause does.
- A Warning Event `OrderedUpdateStalled` with the same text, once per stall; operator log WARN `ordered update: stalled; site pod not Ready` (fields `site`, `pod`, `reason`, `error`) on every retry, about every 30s, sometimes after an `ordered update failed` carrying the same text.
- Argo CD shows the group **Degraded**. `Ready` may still be `True`: the active site keeps serving.
- The stalled site's pod is `NotReady` or crash-looping and, while not Ready, out of its Services. With a broken sidecar it also has no self-fencing. A crash-looping pod can show `Ready` for a few seconds at a time; that does not satisfy the update.

### Diagnostic steps
1. Read the cause from the condition message (`scripts/triage.sh` prints it), then look at the pod it names:
   ```bash
   kubectl get mysqlfailovergroup <group> -n <ns> -o jsonpath='{.status.conditions[?(@.type=="Updating")].message}{"\n"}'
   kubectl describe pod -n <ns> <pod>
   kubectl logs -n <ns> <pod> -c sidecar --previous   # or -c mysql, or the init container the reason names
   ```
2. `pod readiness unknown: ...` means the operator could not read the Deployment or its pods; check the API server and the operator's RBAC for pods and deployments. It does not roll on an unknown.
3. `pod not scheduled: ...` is a scheduling problem (zone selector, capacity, taints), not the template's containers.
4. `pod Ready for less than 30s` means the pod is `Ready` but has not stayed so for 30s and no container restarted recently: it just started, or its readiness probe flaps. A stall with this reason clears on the next retry once the pod holds `Ready` for 30s. If it persists, watch the pod's `Ready` condition (`kubectl get pod <pod> -n <ns> -w`) for a flapping readiness probe. If the pod has been `Ready` for minutes, compare the node's clock with the operator's: the 30s are measured from times the node records, so a node clock ahead of the operator's delays the verdict by the difference.
5. Compare the rendered template with the previous one (`kubectl rollout history deploy/mysql-<group>-<site> -n <ns> --revision=<n>`) to find what the spec change altered: image, Secret references, probe paths, resources.

### Safe remediation
1. Fix the cause the pod reports: a missing or unreadable Secret, a wrong image tag, an invalid setting in the spec. A crash-looping container picks a fixed Secret up on its next restart; deleting the pod skips the back-off. The operator retries every 30s and resumes on its own once the pod has been Ready for 30s; the condition then returns to `NotUpdating` (INFO `ordered update: stalled site pod is Ready` when nothing was left to roll). A crash-looping pod that is briefly Ready between crashes does not clear it.
2. If the template itself is wrong, fix or revert the spec. The broken site then drifts again and the update rolls it like any other site.
3. Leave the active site alone: it was not rolled or fenced, and it keeps serving.

Do not:
- Restart the operator to push the update through: the new process repeats the same check on its first attempt.
- Change the update strategy to `Recreate` to get past it: that rolls every site at once onto the template that broke one.
- Promote the stalled site by hand: its pod is out of the application Services and, with a broken sidecar, cannot self-fence.

## 35. Deleted `MysqlBackup` stuck Terminating (artifact cleanup)

### Symptoms
- A `MysqlBackup` deleted by hand or by retention stays Terminating with the `bloodraven.shipstream.io/mysqlbackup` finalizer.
- `ArtifactCleanupFailed` or `ArtifactCleanupBlocked` Warning Events; `bloodraven_backup_artifact_cleanup_failures_total` rising.
- After an upgrade from an operator whose cleanup ran `cleanup.py`: old Terminating S3 backups with Failed `mysqlbackup-<name>-cleanup` Jobs (new cleanup Jobs are named `mysqlcleanup-<name>`; an old-named Job counts only if the backup owns it). That script needed `util.rmdump`, which MySQL Shell does not have, so S3 cleanup never worked. The new operator replaces those Jobs on its own; let it.

### Diagnostic steps
1. Read the cause and the recorded destination (`scripts/triage.sh` section 8 prints both):
   ```bash
   kubectl get mysqlbackup <name> -n <ns> -o jsonpath='{.status.conditions[?(@.type=="ArtifactCleanup")]}{"\n"}{.status.artifactStorage}{"\n"}'
   kubectl get pods -n <ns> -l bloodraven.shipstream.io/resource=backup-cleanup,bloodraven.shipstream.io/mysqlbackup=<name> \
     -o jsonpath='{range .items[*]}{.metadata.name}{" exit="}{.status.containerStatuses[0].state.terminated.exitCode}{" "}{.status.containerStatuses[0].state.terminated.message}{"\n"}{end}'
   ```
2. Reasons:
   - `CleanupFailed`: exit 75 (storage error), a crash, deadline, eviction or no recorded exit. A new Job runs 5 minutes after the failure.
   - `CleanupRejected`: exit 64. The message says why: missing credential keys, a refused path, a cross-bucket location, an invalid endpoint. A new Job runs 30 minutes after the failure.
   - `CleanupPending`: the pod cannot start; the message carries the waiting reason (`ImagePullBackOff`: the operator image must be pullable in the backup namespace with `spec.backup.imagePullSecrets`), or since when it has been unscheduled or `ContainerCreating` (check `kubectl describe pod` for `FailedMount`/`FailedAttachVolume`, typically a ReadWriteOnce claim attached on another node).
   - `ForegroundDeletion` / `OrphanDeletion`: deleted with `--cascade=foreground` (Argo CD's default prune) or `--cascade=orphan`; cleanup starts once the garbage collector has processed the backup's dependents.
   - `CleanupJobInvalid`: the recorded destination cannot be turned into a Job (unknown storage type, no bucket or claim recorded). No Job ran; find and delete the artifact by hand as for `DestinationMismatch`.
   - `CredentialsUnavailable`: the profile no longer has an S3 `credentialsSecretName`.
   - `OperatorImageUnset`: the operator cannot name its own image; check its Deployment.
   - `DestinationMismatch`: the recorded `status.artifactStorage.location` does not end in the backup name, is outside the recorded bucket or `/backups`, or differs from the producing Job's destination: storage type, location, bucket, endpoint, region, claim or passphrase key (checked while that Job exists). The same comparison fails a backup whose Job succeeded elsewhere (`Ready=False/DestinationMismatch`, phase `Failed`) and refuses a replacement Job built from a profile whose destination changed since launch (`Ready=False/BuildJobFailed`, "create a new backup").
   - Warning `ArtifactCleanupSkipped` "no recorded artifact destination" (not a blocked reason): a successful backup with no `status.artifactStorage` (every successful backup records one, so this is an edited status or a backup from an older operator). The finalizer is released and nothing is deleted; find the artifact from the producing Job's inputs and storage inspection, then delete it by hand.
   - `CleanupJobConflict`: a Job named `mysqlcleanup-<name>` exists but is not owned by this backup.
   - A new backup stuck `Pending` with `Ready=False/BackupJobConflict` (not a cleanup problem): a `mysqlbackup-<name>` Job it does not own exists, usually orphaned by an earlier same-name backup deleted with `--cascade=orphan`. Delete that Job if nothing owns it; the backup fails after 30 minutes otherwise.
   - `ProducerStopStuck` (Warning `ArtifactCleanupStalledProducerStop`): before releasing an abandoned backup (never succeeded, or its group or profile is gone) the operator stops the producing backup Job and waits for its pods; a pod has not stopped after 10 minutes, usually because its node is unreachable. The deletion stays held. A lost status update after the Job was created is recovered from the Job itself, running or failed. A never-succeeded encrypted backup's leftovers are read with `status.artifactStorage.passphraseSecretKeyRef` (the Secret the producing Job mounted), then the profile's. The cleanup Job has a third verdict for such a backup: exit 0 with `BLOODRAVEN_CLEANUP_SKIPPED: no backup stamp at <stamp key> names this backup, but <n> objects remain under <prefix>; nothing was deleted` (PVC: `... but <path> still exists; nothing was deleted`), WARN log `artifact cleanup skipped`. The finalizer is then released WITHOUT deleting the dump-key snapshot and a Warning `ArtifactCleanupSkipped` names the location, the stamp key and the kept snapshot Secret: the objects may be that backup's encrypted artifacts, so inspect and delete them by hand, and delete the snapshot only once none remain. A foreign stamp, or no stamp over an empty location, is still `BLOODRAVEN_CLEANUP_COMPLETE objects=0` (the snapshot is deleted).
   - `HeldByVerification`: a verification is still reading the artifact; see [playbook 36](#36-backup-kept-past-retention-or-deletion-waiting-on-a-verification).
   - `HeldByRestore`: a restore (`initFromBackup` or `restoreInPlace` with `backupRef`) of the group named in the message is still reading the artifact; see [playbook 36](#36-backup-kept-past-retention-or-deletion-waiting-on-a-verification).
3. Cleanup deletes from `status.artifactStorage` (captured when the backup ran), never from the live profile. A changed profile bucket, endpoint or claim does not move the delete; only credentials come from the current profile.

### Safe remediation
1. For `CleanupFailed`, `CleanupRejected` and `CredentialsUnavailable`, fix the cause (Secret keys, IAM `s3:ListBucket`/`s3:DeleteObject` on the prefix, endpoint reachability, the operator image pull) and wait. Retries never stop, and a credential rotated inside the same Secret is picked up by the next Job. A replacement starts only after the failed Job's pods are gone.
2. For `DestinationMismatch` and `CleanupJobInvalid`, find the artifact using the original Job inputs and storage inspection (see [playbook 13](#13-succeeded-backup-cannot-be-found-at-its-recorded-location)). Delete it by hand, then remove the finalizer:
   ```bash
   kubectl patch mysqlbackup <name> -n <ns> --type=json -p '[{"op":"remove","path":"/metadata/finalizers"}]'
   ```
3. For `ProducerStopStuck`, confirm the producer pod's node is down (`kubectl get node <node>`, the cloud console), then `kubectl delete pod <pod> -n <ns> --force --grace-period=0`. Never force-delete a pod on a live node: it may still be writing the location.
4. `ArtifactCleanupNothingFound` releases the finalizer without deleting anything. If the artifact should have existed, inspect the bucket or claim the backup actually used. A Warning `ArtifactCleanupSkipped` for an abandoned backup (`BLOODRAVEN_CLEANUP_SKIPPED`) keeps its dump-key snapshot: delete the leftover objects it names, then the snapshot.

Do not:
- Remove the finalizer while the artifact is still in storage: nothing will clean it up later.
- Edit `status.artifactStorage` to point cleanup somewhere else: the recursive delete would follow it.
- Delete a running cleanup Job to hurry it: the operator replaces only failed Jobs, after their pods are gone.

## 36. Backup kept past retention, or deletion waiting on a verification

### Symptoms
- A successful `MysqlBackup` outside its profile's `retention.keepLast`/`keepWithinDays` windows is not pruned, with `RetentionDeferred` Events. Without `RetentionDeferred`, it is not a hold: see [playbook 43](#43-backups-or-verifications-kept-or-deleted-against-expectation-retention).
- A deleted `MysqlBackup` stays Terminating with `ArtifactCleanup=False/HeldByVerification` or `HeldByRestore` and `ArtifactCleanupDeferred` Events.
- A deleted `MysqlBackupVerification` stays Terminating.
- The `RetentionDeferred` or condition message names "the restore of group <name>": a restore of that group reads the backup (steps 4 and 5).

### Diagnostic steps
1. List the holds on the backup and the verifications that reference it:
   ```bash
   kubectl get mysqlbackup <backup> -n <ns> -o json | jq '.metadata.annotations // {} | with_entries(select(.key | startswith("verification-hold.bloodraven.shipstream.io/")))'
   kubectl get mysqlbackupverifications -n <ns> -o 'custom-columns=NAME:.metadata.name,UID:.metadata.uid,PHASE:.status.phase,BACKUP_UID:.status.backupRef.uid,JOB:.status.jobName'
   ```
2. For each holding verification, check whether anything still runs:
   ```bash
   kubectl get job mysqlverify-<verification> -n <ns> -o jsonpath='{.status.conditions[*].type}{"\n"}'
   kubectl get pods -n <ns> -l bloodraven.shipstream.io/mysqlbackup-verification=<verification> -o 'custom-columns=POD:.metadata.name,PHASE:.status.phase,DELETING:.metadata.deletionTimestamp'
   ```
3. A hold is live while the verification is not finished, or while its Job is unfinished, or while any labelled pod is `Pending` or `Running`. A hold whose verification is gone counts only while that verification's pods still run. An unfinished verification that is not yet pinned holds every successful backup of its group and profile.
4. For a restore, list the restore holds on the backup and the restores in the namespace that name it (any group of the namespace can restore from it):
   ```bash
   kubectl get mysqlbackup <backup> -n <ns> -o json | jq '{uid: .metadata.uid, holds: (.metadata.annotations // {} | with_entries(select(.key | startswith("restore-hold.bloodraven.shipstream.io/"))))}'
   kubectl get mysqlfailovergroups -n <ns> -o json | jq -r '.items[] | [.metadata.name, (.status.restore.phase // "-"), (.status.restore.backupRef.name // .spec.initFromBackup.source.backupRef.name // "-"), (.status.restore.backupRef.uid // "-"), (.status.restoreInPlace.phase // "-"), (.status.restoreInPlace.backupRef.name // .spec.restoreInPlace.source.backupRef.name // "-"), (.status.restoreInPlace.backupRef.uid // "-")] | @tsv'
   kubectl get jobs,pods -n <ns> -l app.kubernetes.io/name=mysql-restore,bloodraven.shipstream.io/failover-group=<group> -o wide
   ```
5. A restore hold (value: the restoring group; key suffix: that group's `backupRef.holdID`) is live while the group's `status.restore` or `status.restoreInPlace` carries that `holdID` and is neither `Succeeded` nor `Failed`. Afterwards, or when the group is gone, it counts only while a restore Job of that group is unfinished or one of its restore pods is `Pending` or `Running`. A restore pinned to another `uid` than the backup's does not hold it (a same-name backup created later is not protected). A restore that has not pinned yet counts for the live `Succeeded` backup its `backupRef` names.

### Safe remediation
1. During a run this is expected; retention and cleanup resume on their own after the run stops (the hold release triggers a backup reconcile).
2. If a finished run still holds, it is usually a failed run kept by `keepOnFailure: true` whose pod is still running, or a pod stuck terminating on an unreachable node. Delete the verification (its finalizer waits for its pods) or resolve the stuck pod.
3. A verification that stays Terminating is waiting for its pods to be gone; it releases its hold and finalizer once they are.
4. A restore releases its hold on the group's first reconcile after it is `Succeeded` or `Failed` and its restore pods have stopped. A restore that never gets there keeps holding: a bootstrap restore waiting in `Pending` (seed not writable, archive binding not ready, or its Job's creation refused: `status.restore.message` starts "creating the restore Job failed: …"), or an in-place run waiting in `Preflight`. A backup deleted meanwhile stays Terminating, and the restore reads it once it can run: the hold was taken before the delete. The group's `status.restore.message` then ends with "MysqlBackup `<b>`, which this restore has pinned, is being deleted, and the deletion waits until this restore has ended". Fix what it waits for, delete the abandoned group (`spec.initFromBackup` cannot be removed), or remove `spec.restoreInPlace` (a run without a Job is cancelled). A hold left by a deleted group needs nothing: it stops counting once that group's restore pods are gone and the backup's reconciler removes the annotation within a minute.

Do not:
- Remove a `verification-hold.bloodraven.shipstream.io/*` annotation by hand while a pod of that verification runs, or a `restore-hold.bloodraven.shipstream.io/*` annotation while a restore pod of that group runs: retention or cleanup could then delete the artifact under it.
- Remove the verification finalizer to hurry deletion. The hold then stays on the backup; it stops counting once the pods are gone, but retention only notices on its next pass.

## 37. Planned failover left the source fenced

A planned failover ended `Failed` with `result.outcome: SourceLeftFenced`. Nothing was written to the target, and the run did not make the source writable again. Usually the source is read-only behind the run's fence. When the run did not prove its fence (the source stopped answering first, or the operator restarted before it recorded the fence), `message` says `source "<site>" left as it is` and what the run knows: the fence was sent but never read back, this run sent none, or an earlier operator process may or may not have sent it. Read the source's flags before assuming it is fenced.

### Symptoms
- `status.plannedFailover.result.outcome: SourceLeftFenced`; Warning Event `PlannedFailoverFailed`.
- `NoPrimary` when no other site is writable.

### Diagnostic steps
1. Read the run:
   ```bash
   kubectl get mfg <group> -n <ns> -o json | jq '{run: .status.plannedFailover | {runID, phase, sourceSite, targetSite, result, message, fence: (.fence // {} | {state, sourceServerUUID})}, lastFailover: .status.lastFailover, activeSite: .status.activeSite, marker: .metadata.annotations["bloodraven.shipstream.io/planned-failover-terminal"]}'
   ```
2. On every site: `SELECT @@GLOBAL.read_only, @@GLOBAL.super_read_only, @@GLOBAL.server_uuid, @@GLOBAL.gtid_executed;`.
3. Operator log for the run: `planned failover: named fenced source for re-assert` (the source was handed to the re-assert), and later WARN `re-asserting fenced promoted primary`.

### Safe remediation
Decide by `result.reason` and `result.warnings`:
1. **Another site is writable** (`ForeignWritable`, or a rollback that found a writable peer): that site is the primary. Never unfence the source; old-primary recovery repoints it as a replica. If you are not sure which site holds the newest data, escalate.
2. **`SourceLost`**: the source was unreachable; emergency failover takes over once the run releases its claim. Restore connectivity. If nothing failed over and the source comes back read-only, step 3 applies: the run named it.
3. **An attributable fence** (no `FenceNotAttributable` warning; for example `StatusStalled`, `ManagerUnavailable`, `SourceLost`, or a rollback that could not confirm the unfence within 30s), every site reachable, and `status.lastFailover.target` empty or the source: the topology manager re-asserts the source on its own, rate-limited by `failoverCooldown`. Wait for the re-assert. With a peer unreachable, the normal unreachable-plus-read-only promotion path decides. The fence is the run's on the server `fence.sourceServerUUID` names, whether `fence.state` reads `Applied` or only `Intended`. Two findings keep the re-assert from lifting it, and both are manual:
   - the source answers under another `server_uuid` (a replaced pod): operator log `planned failover: the fenced source is not the server the run's fence names; its fence is not lifted`. That server's fence is not the run's. Decide as in step 4;
   - the source lacks transactions a peer holds (`primary re-assert refused: peer has transactions the target lacks`, `recoveryState: RecloneRequired`): follow playbook 48, do not unfence it.
4. **Manual cases** (the run's message ends `check which site is writable; if none is, unfence the site with the most complete GTID set by hand (confirm no other site is writable first); re-annotate only once a site is writable`): `FenceNotAttributable`, `LegacyRunRefused`, `UnknownSite` (the source or target was removed from `spec.sites` mid-run; nothing was named), `AuthorityChanged` before `fence.state` was `Applied`, or `status.lastFailover.target` naming a site other than the source. The fence is not the run's to clear, and nothing in the operator will lift it. With explicit approval, after confirming no site is writable, unfence the site with the most complete GTID set (`SET GLOBAL super_read_only=OFF; SET GLOBAL read_only=OFF;`). Do not re-annotate a planned failover instead: with no writable site, admission waits with `waitReason: AuthorityUnsettled` and ends `NotStarted/AdmissionTimeout` after 2 minutes while writes stay refused. A planned failover can follow once one site is writable.
5. Verify `status.activeSite`, DNS and an application write.

Do not:
- Unfence the source while any other site answers `read_only=0`.
- Clear only `super_read_only`: the site stays `read_only=ON`.

## 38. Automation held after a planned failover (`AutomaticFailoverHeld`)

A planned failover may have written to its target and could not explain what it found (a foreign writable site, also one that answered writable and then stopped answering before the run confirmed; transactions outside the fenced set; a replication channel on a target the same operator process had made writable; a replaced server after a possible write or after the run's failover record was durable). Two findings that look alike do not hold. A target found replaced (read-only, nothing outside the fenced set, and either another `server_uuid` or part of the fenced set missing because its volume came back older) before the run's record was durable, by the operator process that sent the clear or by a restarted one (in its promotion attempt or its re-proof), with the source still holding the fenced set under the run's fence and every other promotable site answering read-only, rolls back (`TargetChanged`); without that evidence it holds with `ExecuteFailed`. A run also holds, whatever it sent, when its rollback finds the source writable without transactions it held (`SourceUnfenced`; see "The source lost transactions" below). A target found writable with its replication channel still configured, by a process that sent no clear or by a restarted one, was made writable by someone else: the run ends `SourceLeftFenced{ForeignWritable}` (playbook 37) and the topology manager adopts the writable target. It ended `ManualRecoveryRequired` and holds automation for the group instead of letting split-brain resolution pick a winner, which could fence the only holder of acknowledged writes.

### Symptoms
- `status.plannedFailoverHold` is set and the condition `AutomaticFailoverHeld=True`, reason `PlannedFailoverManualRecoveryRequired`, summarises it; `kubectl bloodraven status` summary ends `+held`. Argo CD shows the group `Degraded` (its health check reads `AutomaticFailoverHeld=True`). The deployment API answers `unstableReason: AutomaticFailoverHeld` (leases refused) for every form of the hold below.
- Or, when status writes had stalled, neither is set and only the `bloodraven.shipstream.io/planned-failover-terminal` annotation carries `"outcome":"ManualRecoveryRequired"`; it holds exactly the same (`triage.sh`: `held by the terminal marker only`). In this marker-only form Argo CD can still show the group `Healthy`, because its health check does not read annotations: check the annotation (or run `triage.sh`) rather than trusting Argo CD health.
- Dragonfly is held too: a failed Dragonfly master is not replaced, replicas are not re-attached, Dragonfly rollouts do not promote, and a snapshot-restore upgrade waits (Normal `DragonflyUpgradeDeferred`, `a planned failover run or automation hold owns the sites`) until the hold is acknowledged. A Dragonfly failure during the hold is a cache outage until the hold is acknowledged.
- Warning Events `PlannedFailoverManualRecoveryRequired`, then `PlannedFailoverHoldPersisted` every 10 minutes.
- Operator log `planned failover: automation held pending acknowledgement` (once per hold, not per pass) and, every poll, `cross-site action deferred: planned failover in progress`. Backups, schedules, verification, keyring work and pod labels keep reconciling during the hold.
- New planned-failover requests are refused with a `PlannedFailoverRejected` Warning (`ManualRecoveryPending`) and the annotation is removed. While `status.plannedFailover` shows the held run it stays there (the refusal is Event-only); otherwise the refusal is recorded as a `NotStarted/ManualRecoveryPending` block. A request refused during the hold never runs after the acknowledgement: re-annotate once the hold is cleared.

### Diagnostic steps
1. Read the hold:
   ```bash
   kubectl get mfg <group> -n <ns> -o json | jq '{hold: .status.plannedFailoverHold, condition: [.status.conditions[]? | select(.type == "AutomaticFailoverHeld")], run: .status.plannedFailover | {runID, sourceSite, targetSite, result, message}, marker: .metadata.annotations["bloodraven.shipstream.io/planned-failover-terminal"], ack: .metadata.annotations["bloodraven.shipstream.io/acknowledge-planned-failover-hold"]}'
   ```
   `hold` names the run (`runID`), its `reason`, `sourceSite`, `targetSite` and the `ambiguousSites` that were writable or unreadable. Read the run from there, not from the condition message. When status writes had stalled, a marker `{"schemaVersion":1,"runID":"<runID>","outcome":"ManualRecoveryRequired","reason":"<reason>"}` carries the same hold instead.
2. On every site: `read_only`, `super_read_only`, `server_uuid`, `gtid_executed`, `SHOW REPLICA STATUS`. Run `gtid-audit.sh`.
3. Compare the source and the target both ways: `SELECT GTID_SUBTRACT('<target set>', '<source set>'), GTID_SUBTRACT('<source set>', '<target set>');`.

### The source lost transactions (`reason: SourceUnfenced`)
The hold's reason is `SourceUnfenced` and the run's `message` starts `the rollback found source "<site>" writable without transactions it held before (<gtid set>)`. The run did not cause this and wrote nothing that matters to it: its source restarted on older data under the same `server_uuid` (the volume came back older) and was then made writable (a pod that boots writable, or an external `read_only=0`). It is serving writes, and it issues the lost GTIDs again for other transactions. The originals were acknowledged before the fence; the target, or another site, holds the only copies.

- Operator log, ERROR `planned failover: rollback found the source writable without transactions it held`: `lostGtid` is the set; `fenceGtid`, `sourceGtid` and `targetGtid` are three of the sets that were compared. The rollback also reads every other site that answers (a disaster-recovery site, a reader), so `lostGtid` can name transactions that neither the fenced set nor the target shows: step 2's `gtid_executed` of every site tells which site holds them.
- GTID sets stop showing the loss once the source has issued as many transactions as it lost: step 3's `GTID_SUBTRACT` may then return nothing although the two sites hold different transactions under the same GTIDs. Trust the message and the log line, not a later comparison.
- The run sent nothing to the source and did not create or start the target's channel. If the target's channel was running all along (the run was still in its lag wait), stop it now, with approval: `STOP REPLICA;` on the target. It would skip what the source writes under the lost GTIDs.

Remediation, with approval for every step:
1. Do not reclone the target, or any site that holds the lost set, until the lost transactions are saved: `mysqlbinlog --include-gtids='<lostGtid>'` against that site's binary logs (a replica logs what it applies: every site runs with `log_replica_updates=ON`).
2. Decide which history stays. Keeping the source (it has served writes since): review the saved transactions and re-apply what still matters by hand, as new transactions; then reclone every site that holds the old ones from the source. Keeping the target's history instead: fence the source, make the target the primary by hand, and reclone the source.
3. Continue with steps 3 to 5 below (one writable primary, DNS, acknowledge).

### Safe remediation
Get explicit approval for every step; this is a data decision.
1. Choose the primary from the data: the site holding the transactions that must be kept. Preserve the other site's extra transactions (`mysqlbinlog --include-gtids=<extra set>`) before anything reclones it.
2. Fence every other site: `SET GLOBAL super_read_only=ON;`.
3. Make sure exactly one `PrimaryCandidate` site is writable and DNS points at it; the operator does not move DNS while the hold stands.
4. Acknowledge with the held run's id:
   ```bash
   kubectl bloodraven ack-planned-failover-hold <group> -n <ns>
   # or: kubectl annotate mfg <group> -n <ns> bloodraven.shipstream.io/acknowledge-planned-failover-hold=<runID>
   ```
5. Verify: `status.plannedFailoverHold` is removed and the condition turns `False` with reason `PlannedFailoverHoldAcknowledged`, the operator deletes the annotation, Event `PlannedFailoverHoldAcknowledged`, log `planned failover: hold acknowledged` then `planned failover: claim released` with `why=ack`. Old-primary recovery then repoints the fenced site, or reports divergence ([playbook 33](#33-a-healthy-replica-reports-divergent-transactions-or-an-ordered-updateplanned-failover-refuses-a-divergent-target)).

Do not:
- Acknowledge before the sites are resolved: the next poll acts on whatever state they are in.
- Delete the `planned-failover-terminal` annotation, `status.plannedFailoverHold` or the condition by hand to clear a hold that names a run; the acknowledgement clears them in a restart-safe order. The exception is a `True` condition without `status.plannedFailoverHold`: it names no run, no acknowledgement can match it, so remove it by hand after resolving the sites. The operator then releases the hold without a restart (log `planned failover: released an automation hold no durable record backs`, then `planned failover: claim released` with `why=orphanHold`).
- Expect `PlannedFailoverHoldAcknowledgmentIgnored` to clear anything: it means the value names the wrong run, no hold stands for the run it names, or the planned-failover claim is held for another run.

---

## 39. PITR archive identity refused (`ArchiveLegacy`, `ArchiveForeign`, `ArchiveCopy`, `ArchiveDescriptorMissing`, encryption, store)

### Symptoms
- `PITRArchiveReady=False` with an `Archive*` reason and a Warning `PITRArchiveNotReady`; `status.pitr.archive.state: Refused` with the same `reason`.
- `/archiver/status` on the refusing site: `archive.state: Refused`, `lastError` starting with the token (`PITRArchiveLegacy: archive … at s3://…: …; <procedure>`). The site uploads nothing; other sites may still be `Verified` or `AwaitingDescriptor`.
- Sidecar WARN `archive descriptor changed` when a descriptor that verified earlier no longer does.

### Cause
Every archive has a write-once plaintext descriptor `<prefix>/binlogs/archive.json` naming the source group (`namespace`, `name`, `uid`), an archive ID derived from that UID and the normalized destination, and the encryption mode with a passphrase `keyCheck`. A writer accepts it only when it names its own group's UID, an ID derived for its own destination, and its profile's encryption and key:

| Reason | Meaning |
|---|---|
| `ArchiveLegacy` | The prefix predates v1.7.0: version 1 manifests or other objects, no descriptor. Seen when a v1.7.0 group's PITR profile reuses a prefix an older group wrote. |
| `ArchiveForeign` | The descriptor names another group's UID: another group archives here, or this group was deleted and re-created under the same name (new UID). |
| `ArchiveCopy` | This group's UID but an archive ID for another destination: the profile points at a copy (a replicated bucket, a moved prefix, a changed endpoint spelling). Copies are read-only. |
| `ArchiveDescriptorMissing` | Version 2 manifests but no descriptor: it was deleted, or a replicated copy has not received it yet. |
| `ArchiveEncryptionMismatch` | The profile's encryption mode or algorithm differs from the descriptor's (encryption was enabled or disabled on an existing archive). |
| `ArchiveKeyMismatch` | The mounted passphrase does not verify the `keyCheck` (the passphrase or Secret key changed). |
| `ArchiveVersionUnsupported` | Descriptor or manifest from a newer release (a downgrade), or an unknown encryption mode. Also a damaged `archive.json`: undecodable, trailing data, wrong kind, a malformed field, source names outside Kubernetes bounds, or larger than 64 KiB; the message then reads `archive.json is damaged …` with the repair instead of "upgrade bloodraven". |
| `ArchiveStoreUnsupported` | The store does not enforce atomic create: the probe's second `If-None-Match: *` create succeeded or the header was rejected, or a PVC filesystem lacks `link(2)`. |

### Diagnostic steps
1. Read the observation and the per-site reports:
   ```bash
   kubectl get mfg <group> -n <ns> -o jsonpath='{.status.pitr.archive}{"\n"}'
   scripts/sidecar-probe.sh -n <ns> <group> archiver
   ```
2. Read the descriptor (plaintext on every archive; it holds no secret):
   ```bash
   aws s3 cp s3://<bucket>/<prefix>/binlogs/archive.json - | jq .
   kubectl get mfg <group> -n <ns> -o jsonpath='{.metadata.uid}{"\n"}'
   ```
   Compare `source.uid` with the group's UID and `archiveID` with `status.pitr.archive.archiveID` and the sidecar's `BLOODRAVEN_PITR_ARCHIVE_ID`.
3. For `ArchiveStoreUnsupported`, check the store's conditional-write support (AWS S3, MinIO and RustFS 1.0+ support it; active-active multi-zone buckets do not certify it).
4. For `ArchiveKeyMismatch` / `ArchiveEncryptionMismatch`, compare the Bound binding (`status.pitrArchiveBindings[]`, `PITRArchiveBindingReady`) and the snapshot the pods mount with what the archive was written with (playbook 26). A Bound binding is never demoted by a sidecar report: a `KeyMismatch` against it means the descriptor was replaced. Do not print the passphrase.

### Safe remediation
- Most reasons need **a new prefix and a full backup**: change the active profile's `storage.s3.prefix` (or `pvc.subPath`), or select another profile in `spec.backup.pitr.profileName`, wait for the rollout, then take and verify a full backup. The first upload creates a new descriptor.
- `ArchiveLegacy`: the profile reuses a pre-v1.7.0 prefix. There is no in-place upgrade from v1.6.0; a v1.7.0 group archives to a prefix no older group used, so point the PITR profile at a new prefix. Keep the old prefix, backups, passphrase and the old cluster for older targets.
- `ArchiveDescriptorMissing`: restore `archive.json` from a bucket backup or, for a copy, from the source; never write a descriptor by hand (the ID and `keyCheck` are derived and a wrong one makes the archive foreign).
- `ArchiveKeyMismatch` / `ArchiveEncryptionMismatch`: with a Bound binding the pods already load the accepted key, so a profile edit cannot cause this (it shows as `PITRArchiveBindingReady=False/KeyChangeRefused` instead); the archive's descriptor was replaced. Before the binding is Bound, put the archive's original key and settings back, or start a new prefix. There is no in-place re-encryption. See playbook 26.
- `ArchiveStoreUnsupported`: move the archive to a store that enforces conditional writes.
- `ArchiveVersionUnsupported`: run the release that wrote the archive. For a damaged `archive.json`, restore the original object (bucket versioning, a copy of the archive) or point the profile at a new prefix; never write a descriptor by hand.

Do not delete `archive.json`, rewrite manifests, or point two groups at one prefix. A restored group must archive into its own prefix (`PITRArchiveDestinationShared` refuses the restore otherwise). The sidecar needs `s3:ListBucket`, `s3:GetObject`, `s3:PutObject` and `s3:DeleteObject`.

### DR standby (`MysqlStandbyCluster`, Warning `PITRArchiveRefused`)

A standby only reads the source archive; there is no `MysqlFailoverGroup`, sidecar or `/archiver/status` on that side, and no target group may exist yet. The refusal shows on the standby:

1. Read the condition and the configured source:
   ```bash
   kubectl get mysqlstandbycluster <name> -n <ns> -o jsonpath='{range .status.conditions[?(@.type=="SourceConfigKnown")]}{.status} {.reason}: {.message}{"\n"}{end}'
   kubectl get mysqlstandbycluster <name> -n <ns> -o jsonpath='{.spec.source}{"\n"}{.status.discovered}{"\n"}'
   ```
   While the archive is refused, `status.discovered.latestDump` still names the dump but there is no `status.discovered.binlogArchive` (identity and window are published together or not at all): recovery is limited to the dump.
2. Read the descriptor at the source prefix (`aws s3 cp s3://<bucket>/<prefix>/binlogs/archive.json - | jq .`, with `spec.source.s3.bucket` and `prefix`) with the standby's credentials, and compare its encryption mode with `spec.source.decryption` (set: `Passphrase`/`AES-256-GCM` expected; unset: `None`).
3. By reason (the `SourceConfigKnown` reason is the token without `PITR`):
   - `ArchiveDescriptorMissing` on a replicated bucket: usually replication lag; wait, or copy `archive.json` from the source.
   - `ArchiveEncryptionMismatch` / `ArchiveKeyMismatch`: fix `spec.source.decryption` (Secret name and key) to match what the source archive was written with.
   - `ArchiveListFailed`: on an S3 403 grant `s3:ListBucket` to the standby's credentials; otherwise it is retried.
   - `ArchiveVersionUnsupported`: a newer source release (upgrade the standby's operator), or a damaged `archive.json` (restore it at the source or in the copy).
   - `ArchiveIdentityMismatch`, `ArchiveObjectInvalid`, `ArchiveLegacy`, `ArchiveHistoryReset`, `ArchiveUnsafeHistory`, `ArchiveUnreadable`: the source archive itself is the problem; work it from the source group with the sections above and playbooks 40 to 42. On a replicated copy, check the copy is complete before applying a permanent remedy (new prefix, quarantine): a manifest can arrive before the objects it lists.
4. `ScanIncomplete` ("manifest … was listed but is gone; rescanning") is transient.

See [Multi-cluster DR](../../../../site/content/docs/5.architecture/3.multi-cluster-dr.md).

---

## 40. PITR unsafe history marker (`ArchiveUnsafeHistory`)

### Symptoms
- `PITRArchiveReady=False` / `ArchiveUnsafeHistory` and a Warning `PITRArchiveUnsafeHistoryDetected` (edge-triggered, besides `PITRArchiveNotReady`).
- A site's `/archiver/status` has a non-null `historyIntegrity{violation, detail, at}` and `lastError: PITRArchiveUnsafeHistory: <site> <violation> <detail>; this site's binlogs cannot certify history; start a new archive prefix and take a full backup`. Sidecar WARN `archive history unsafe`.
- Not to be confused with `PITRMysqlConfAccepted=False`/`PITRUnsafeSettingRefused` (Warning `PITRMysqlConfRefused`): the operator refused such a key in `spec.mysqlConf` and left it out of my.cnf, so no marker is set. Remove the key.

### Cause
PITR certifies history from binlog order and row images. At store init and every audit cycle each site checks: no replication filters (`performance_schema.replication_applier_filters` / `_global_filters`), no binlog filters (`Binlog_Do_DB`, `Binlog_Ignore_DB`), `binlog_format=ROW`, `binlog_row_image=FULL`, `replica_preserve_commit_order=ON`. On the first violation (`ReplicationFilter`, `BinlogFilter`, `BinlogFormat`, `BinlogRowImage`, `CommitOrder`) it persists the marker in its own manifest and stops uploading. The marker is permanent for that prefix; removing the setting never clears it. It removes every restore target that needs that site's bytes, including targets before the violation, since nothing proves which archived files the setting affected; a new prefix restores only targets after it. The operator refuses these options in `spec.mysqlConf` while PITR is enabled (`PITRMysqlConfAccepted`), so a marker means an out-of-band change: `SET GLOBAL`, a hand-edited my.cnf, a session in the pod. `replica-preserve-commit-order` is operator-owned (`ON`), so `CommitOrder` means something changed it at runtime (`SET GLOBAL`). When the check itself fails, `lastError` reads `history integrity: …` and no marker is set; fix the probe (privileges, connectivity).

### Diagnostic steps
1. Find the marked sites and the offending key:
   ```bash
   kubectl get mfg <group> -n <ns> -o jsonpath='{range .status.conditions[?(@.type=="PITRArchiveReady")]}{.reason}: {.message}{"\n"}{end}'
   kubectl get mfg <group> -n <ns> -o json | jq '{group: .spec.mysqlConf, sites: [.spec.sites[] | {name, mysqlConf}]}'
   scripts/sidecar-probe.sh -n <ns> <group> archiver
   ```
2. On the site, confirm the live values:
   ```sql
   SELECT @@global.binlog_format, @@global.binlog_row_image, @@global.replica_preserve_commit_order;
   SELECT * FROM performance_schema.replication_applier_filters;
   SELECT * FROM performance_schema.replication_applier_global_filters;
   SHOW BINARY LOG STATUS;
   ```

### Safe remediation
1. Undo the out-of-band change (and remove any refused key from `spec.mysqlConf`); the persisted marker stays.
2. Point `spec.backup.pitr` at a new prefix and wait for the rollout.
3. Take and verify a full backup in the new archive.
4. Keep the marked prefix and its manifest as evidence. Do not edit the manifest to remove `historyIntegrity`.

---

## 41. Reissue risk: a server that could reissue archived GTIDs (`PITRArchiveReissueRiskDetected`)

### Symptoms
- A site's `/archiver/status` reports a non-empty `reissuableGtids`, with `lastError` "site `<s>`: server_uuid `<uuid>` no longer has archived transactions `<set>` that it issued (row `<name>` id `<id>`) and reuses their GTIDs when it writes; once the reused GTIDs are archived, every restore from this archive refuses PITRArchiveHistoryReset. Reclone this site before it is promoted (a clone starts with a new server_uuid); if it is already the primary, start a new archive prefix and take a full backup", and `PITRArchiveReady=False/ArchiverError` names the site.
- Warning `PITRArchiveReissueRiskDetected` on the group after a promotion: "site `<s>` was promoted while its archive holds transactions its server no longer has and issued under the server_uuid it still has: `<set>`. Its new writes reuse those GTIDs, and once they are archived every restore from this archive refuses PITRArchiveHistoryReset. Start a new archive prefix and take a full backup."
- The same Warning in its fallback form: "… holds N divergent/quarantined rows, and its sidecar has not reported whether its server can reissue their GTIDs …" (a sidecar image that predates `reissuableGtids`, or one whose audit had not evaluated its server yet).
- Later, the site's `lastError` reads "site `<s>`: verified GTID namespace reuse within `<set>` …" and restores from the archive fail `PITRArchiveHistoryReset`.

### Cause
A server reissues GTIDs when it lost transactions it had issued and still has the `server_uuid` it issued them under: its next write takes the lowest free number of that `server_uuid`, which the archive already holds for another transaction. The archive then holds two transactions under one GTID, and every reader refuses the whole archive. A clone gives its recipient a new `server_uuid`, so a site recloned after a lossy failover cannot reissue what it lost; its divergent rows are evidence only (playbook 27). The risk remains for a data directory restored from a snapshot or reset in place (`auto.cnf` kept), for a clone that did not rotate the `server_uuid` (Warning `CloneServerUUIDNotRotated`), and for a site cloned by an earlier release. The sidecar's history audit computes `reissuableGtids` every scan from the active and the quarantined rows of its own manifest (a quarantine moves only metadata, so a quarantined row's transactions can be reissued too) and keeps the record for that source when the numbers return, because the replacement transactions may still be in the active, unarchived binlog. The Warning fires when the active site changes (failover, planned failover, re-promotion, also after a `NoPrimary` interval on a group that has failed over before) to a site whose last archiver status reports a non-empty `reissuableGtids`.

### Diagnostic steps
1. Read every site's verdict:
   ```bash
   scripts/sidecar-probe.sh -n <ns> <group> archiver   # divergentRows, quarantinedRows, reissuableGtids, lastError
   scripts/triage.sh -n <ns> <group>                   # prints reissuableGtids= per site
   ```
   `reissuableGtids` `""` (triage: `none`) means the server cannot reissue anything, whatever `divergentRows` says. A set means it can. Absent (triage: `not-evaluated`) means the audit has no verdict: the server has no history yet (empty `gtid_executed`, a clone in progress, read-only with no replication channel), the sidecar's last maintenance pass could not evaluate the server (no audit has run since the sidecar started, the manifest or the server's state could not be read, or the manifest carries a recorded GTID reuse), or the sidecar image predates the field. A verdict is never kept from an earlier pass, so read `lastError` as well when it is absent.
2. Confirm the server's identity: `kubectl exec -n <ns> deploy/mysql-<group>-<site> -c mysql -- mysql -N -e 'SELECT @@server_uuid'` and compare it with the source UUID of the listed GTIDs. Check for `CloneServerUUIDNotRotated` Events and the site's last reclone (`kubectl logs -n <operator-ns> deploy/bloodraven | jq -c 'select(.msg=="starting bootstrap" and .source=="reclone")'`).
3. Check whether the reuse already reached the archive: fetch the plaintext `manifest-<site>.json` and read `epoch.resetReason` (`scripts/gtid-audit.sh -n <ns> <group> --manifest manifest-<site>.json` prints the divergent rows and records).

### Safe remediation
1. **The site is a replica** (the error stands, nothing reissued yet): reclone it, `kubectl bloodraven reclone <group> <site> -n <ns>`. The clone starts with a new `server_uuid`; on the next scans `reissuableGtids` becomes `""`, the error clears and the condition recovers. The divergent row stays as evidence. If the reclone raises `CloneServerUUIDNotRotated`, fix what it names (usually the site's pod has not rolled onto the current pod template yet) and reclone again. Do not promote the site before that.
2. **The site is already the primary** (the Warning fired, or the error stands on the active site): point `spec.backup.pitr` at a new prefix, wait for the rollout, then take and verify a full backup. Keep the old prefix and its backups for targets before the reissue; it stays usable only until a reissued GTID is archived there.
3. Quarantining the row does not remove the risk (`reissuableGtids` still lists its transactions), and neither quarantine nor acknowledgement clears a verified reuse.
4. A fallback Warning with `reissuableGtids` `""` on the promoted site is a false positive: nothing to do.

---

## 42. PITR refused: divergent history, lost at promotion, split brain, gap, order conflict or inversion, reuse

### Symptoms
A restore or verification fails with one of `PITRArchiveDivergentHistory`, `PITRArchiveSplitBrain`, `PITRArchiveGap`, `PITRArchiveOrderConflict`, `PITRArchiveOrderInversion` or `PITRArchiveNamespaceReuse`. The archive, the dump and the target were accepted; the archive does not prove a single history from the backup to the target.

### How the planner decides
- Λ is the history of the witness (the first archived transaction at or after T outside the backup B and `excludeGtids` X), or the archive head in latest mode.
- **D ⊆ X.** D is every archived transaction (active or quarantined row) committed after the backup's newest transaction and before T that is outside Λ ∪ B ∪ X. A non-empty D refuses `PITRArchiveDivergentHistory` and prints D (1 KiB; the full set is in the `pitr-download` log line `BLOODRAVEN_PITR_DIVERGENT_SET` and in `coverage.json`). Transactions before the backup's newest transaction never count.
- **Frontier.** The backup's newest commit is read from archived bytes; a member of B archived with a commit time after the dump finished means the GTID was reissued (`PITRArchiveNamespaceReuse`). Any downloaded copy, quarantined rows included, counts.
- **Anchors.** A witness w archived only as a replica copy is accepted when its predecessors are proven on its origin by a promotion record of that server that does not contain w and was observed more than δ (the fixed 5 s margin) before w committed (the whole record anchors), or by an archived origin file of that server (no margin: same server's binlog). An anchor is set aside when a later record of its site (a promotion or an incarnation `Reset` record) was observed no later than w's commit time + that record's `probeInterval` + δ, or when another server's promotion record observed between the anchor and w (±δ) does not hold w. A record that would anchor by time and whose confirmation (`gtidExecuted` plus `confirmedGtidAdded`) holds w although w committed after `confirmedAt` refuses the target outright, whatever the other copies and anchors say. A record that holds w, or that was observed within δ of w, anchors nothing: its promotion set (`promotionGtidExecuted`) and confirmation are recorded, and the planner does not anchor with them in this release (the promotion-record lag window still refuses). An accepted copy must then agree with its anchor: it follows everything the anchor holds, and every transaction it follows under the origin's `server_uuid` outside the promotion set of a promotion record of the origin observed more than δ before w (the anchor, or an earlier record) has no archived copy committed before that record's episode began (`observedAt` − `probeInterval`). A record without a promotion set dates nothing, so a GTID issued twice by a server that lost its binlog tail and rejoined by replication is not detected there. Clone bridge rows and file times play no part; `excludeGtids` sets no record aside and makes none an anchor, and waives a transaction in the copy checks.
- **Loss evidence (latest mode).** A head transaction g of any source is lost at a promotion record R when g committed more than δ before R was observed, R lacks g, g ∉ X, and g has an archived copy on a site other than R's. Commits after R, or within δ of it, are not evidence. The divergence audit's `divergent` rows (recorded by each sidecar, with no archiver error; playbook 27) are evidence too, and both are reported together. A record is never set aside: the refusal clears only when X holds every flagged transaction.
- **The marker.** Any row of a site with a history-integrity marker refuses `PITRArchiveUnsafeHistory` when the plan, witness or anchor needs it (playbook 40).

### Diagnostic steps
1. Read the refusal message in full; it names the sets, sites and files. Get the full D from the pitr-download log: `kubectl logs <job-pod> -c pitr-download | grep BLOODRAVEN_PITR_DIVERGENT_SET` (or the `pitr plan`/`download site binlogs` record, or `coverage.json` `divergent`/`lost`); latest-mode loss refusals print it too.
2. Correlate with failovers: `kubectl get mfg <group> -n <ns> -o jsonpath='{.status.lastFailover.time} {.status.lastFailover.target} {.status.lastFailover.promotionGtidExecuted}'`, operator Events, and each site's records and divergent rows. Fetch each plaintext `manifest-<site>.json` from the archive prefix and run `scripts/gtid-audit.sh -n <ns> <group> --manifest manifest-a.json --manifest manifest-b.json`: it prints each record (kind, `observedAt`, `probeInterval`), the active and quarantined divergent rows, and a candidate `excludeGtids` union. Prefer the exact set the refusal printed; never exclude anything in the dump's own GTID set (the `gtidExecuted` of the dump's `@.json`, which can hold more than `MysqlBackup status.gtidExecuted`).
3. On a replicated archive copy (DR), first check the copy is complete: a manifest can arrive before the objects it lists, so a missing or unreadable object, a `PITRArchiveNamespaceReuse` "cannot be read and verified", or a `PITRArchiveGap` naming an unreadable row may be replication lag. Wait for the named object to arrive, or compare with the source archive, before applying a permanent remedy (new prefix, quarantine, exclusion).
4. Classify:
   - **`PITRArchiveDivergentHistory` (timestamp)**: D was lost at a failover (in the old primary's archived tail, not in the new primary's promotion record), written on a replica (errant), or another branch of a split brain.
   - **`PITRArchiveDivergentHistory` "lost at the promotion of `<site>`" (latest)**: the newest archived state holds transactions the promoted site never had. Only a promotion record is this evidence: a writable episode whose sidecar was down for all of it leaves none, and a latest-mode run then restores the newest archived state (known limitation "Unrecorded writable episodes in latest mode"); a timestamp target is not affected. `coverage.json` `lost` lists the excluded transactions the records prove lost, also those in files the plan did not stage. A WARN `pitr loss report incomplete` in the `pitr-download` log (`rows`, `count`, `error`) means some archived files could not be read for that list only: the plan or refusal stands, and the list may lack transactions whose copies are in those files.
   - **`PITRArchiveSplitBrain` (latest)**: two archived heads each hold transactions the other lacks; the message lists each head's exclusive set.
   - **`PITRArchiveGap`**: the history needs transactions no archived file holds: a quarantined row ("it is in quarantined row …"), or a tail that was never archived ("committed on `<site>` and never archived": a primary that died and never came back, or a site recloned before its tail was archived, which the Warning `PITRArchiveTailNotArchived` announces; playbook 46).
   - **`PITRArchiveOrderConflict`**: every transaction is archived but no archived binlog continues from the furthest state; needs commit orders no server produced.
   - **`PITRArchiveOrderInversion`**: two archived binlogs apply the same pair of GTIDs in opposite orders (both named, with their files); usually an errant transaction.
   - **`PITRArchiveNamespaceReuse`**: two copies of a GTID differ, a row that overlaps the history cannot be verified, or a GTID was reissued after the dump (a reclone kept `server_uuid`). "differs between `<site/file>` and `<site/file>` (…)" lists what differs: commit time, origin server, "one copy is empty", or "row changes differ: …". Row changes alone (same commit time and origin) mean one copy is not a faithful copy of the other. "row changes differ: they change different tables": the copies change other tables, or the same tables in another way or order; a repair transaction with other changes was injected under that GTID with `SET GTID_NEXT`, or one server applied only part of it. "row changes differ: the same tables with different row data": a repair with other values was injected, or the two servers' data had already diverged when they applied it (its before images differ). The message ends with both copies' effect digests (tables, encoding and row data parts). The comparison covers the copies the restore downloads and ignores the order of a table's rows (a replica logs the rows of a table without a primary key in its own order). It does not compare the row data of sites that encode the table differently (a column added or widened on one site first, a generated invisible primary key on one site, another `binlog_row_image`), so a rolling schema change does not refuse; it does not see a change that only rearranges a table's row bytes; and it skips statement and DDL text, transactions with `SAVEPOINT`/XA, partial JSON updates, tables with a `JSON` column, and copies that were not both downloaded.

### Safe remediation
- **"Retry" with a changed `pointInTime`** (or any other change to the restore or verification inputs) never edits the existing object. A verification's spec is immutable: create a new `MysqlBackupVerification`. `spec.initFromBackup` is fixed at creation: delete the never-bootstrapped group and create it again with the corrected block, after checking that `status.restore.phase` never reached `Succeeded`; delete it as in site docs Operations, "Deleting a group without losing backups" (foreground, after its own backups are gone) and re-apply only once its PVCs and backups are gone (site docs: Backup and restore, "A failed bootstrap restore").
- **Divergent history**: if D is history you mean to drop (the lost tail, the errant, the other branch), copy it into `pointInTime.excludeGtids` (restore or verification) and retry. Otherwise choose a target at or before the earliest time in D, or use a backup taken after the latest; a backup taken after a lossy failover needs no exclusion. A successful run reports `excludedDivergentTransactionCount` and a Warning `PITRDivergentHistoryExcluded`.
- **Split brain (latest)**: decide which branch the cluster kept and put the other branch's exclusive set into `excludeGtids`.
- **Gap**: un-quarantine a recovered row, or use a backup taken after the gap, or a target before it, as the message says. An unarchived tail cannot be recovered from the archive. The operator rotates the outgoing primary at a planned failover or ordered-update handoff and before a reclone, and a read-only site archives its writable-era tail; when that did not happen, see playbook 46.
- **Order conflict or inversion**: exclude the errant transaction the message names, or use a backup and an archive prefix that start after it. Do not edit binlogs.
- **Reuse**: start a new archive prefix and take a full backup (playbook 41 when a server that kept its `server_uuid` reissued GTIDs it had lost). Never delete or rename archived objects to force a plan.
- **Row changes differ**: no `excludeGtids` accepts one of the copies. Restore from a full backup taken later, once the backup's newest transactions are in binlog files after the two the message names (the planner then no longer reads them). Find out which site diverged before trusting it: compare the named transaction on both sites (`mysqlbinlog --include-gtids='<gtid>' --base64-output=DECODE-ROWS -vv <binlog>`; "different tables" shows as other `### INSERT INTO`/`UPDATE`/`DELETE FROM` targets, "different row data" as other values in the `@n=` lines), run `scripts/gtid-audit.sh`, and reclone the site whose data is wrong (playbook 33).
- After excluding transactions that are on the target history (for example a bad `UPDATE`), the result is not certified for them: check `pointInTime.onHistoryExclusions` and the Warning `PITROnHistoryTransactionsExcluded`, and inspect the affected rows.

## 43. Backups or verifications kept or deleted against expectation (retention)

### Symptoms
- More successful `MysqlBackup` CRs (and artifacts) remain than the profile should keep, with no `RetentionDeferred` Events.
- Failed `MysqlBackup` or `MysqlBackupVerification` CRs disappear as soon as they finish, or Succeeded verifications vanish right after their run.

### How retention decides
- It runs whenever a finished (`Succeeded` or `Failed`) backup or verification is reconciled, over the CRs of the same group and `spec.profileName` (labels are not used), skipping CRs already being deleted.
- A successful backup is kept when **any** rule keeps it: it is one of the newest `spec.backup.profiles[].retention.keepLast` (default 7, at least 1, so the newest successful backup is never pruned), or it completed within `keepWithinDays` (unset: no age rule). It is deleted only outside both, and then only when no verification may be reading it (playbook 36).
- Failed backups are capped by `keepFailed` (default 10), separately. An explicit `keepFailed: 0` keeps none: a failed backup is deleted once a newer backup of its schedule exists (its retry or the next run), and its leftovers go through artifact cleanup (playbook 35). That is configuration, not a fault. The latest failed backup of a schedule in `spec.backup.schedules` is never pruned, whatever `keepFailed` says, so `spec.backup.retry` can retry it and `status.backupSchedules` still reports it; the last backup of a removed or renamed schedule is pruned like any other. Under `keepFailed: 0` any other failed backup, an ad-hoc one included, is pruned two minutes after the operator records its verdict, or after an operator start, whichever is later (the grace lets `kubectl bloodraven backup --wait` read the verdict, also when the operator saw the Job finish late).
- Verifications use `verification.retention.keepLast` (default 30) for Succeeded and `keepFailed` (default 10) for Failed runs; `0` keeps none, two minutes after the operator records each run's verdict. They apply to hand-made verifications of the profile too.
- A backup whose profile is no longer in the group keeps every successful CR (only the default failed cap applies): no rules, no deletion.
- There is no "never prune" setting. Released manifests with `retention: 0` (or `retentionPolicy` with `count: 0` and `maxAgeDays: 0`) disabled pruning; that has no equivalent and must become an explicit `keepLast`.

### Diagnostic steps
1. Read the effective retention (defaults are stored, so what you see is what applies):
   ```bash
   kubectl get mfg <group> -n <ns> -o json | jq '.spec.backup.profiles[] | {name, retention, verificationRetention: .verification.retention}'
   ```
2. List the profile's backups newest first, with phase and deletion state:
   ```bash
   kubectl get mysqlbackups -n <ns> -o json | jq -r '.items[] | select(.spec.failoverGroupRef.name=="<group>" and .spec.profileName=="<profile>") | [.status.completionTime, .metadata.name, .status.phase, (.metadata.deletionTimestamp // "-")] | @tsv' | sort -r
   ```
3. A backup outside both windows that stays with no `RetentionDeferred` Event: check that a finished backup of the same profile has been reconciled since the profile changed (a new backup finishing is enough), and that the CR names the profile exactly. One that is Terminating is in artifact cleanup (playbook 35), not retention.

### Safe remediation
- Change `retention` on the profile; it applies the next time a finished backup or verification of that profile is reconciled. To keep failed runs for investigation, set `keepFailed` above 0 (or omit it for 10).
- Do not delete a held backup by hand to enforce retention (playbook 36), and do not delete backups to free the PITR archive faster than retention would: the binlog cutoff follows the oldest successful backup still kept.

## 44. A site workload is refused (`WorkloadSelectorMismatch`) or an internal payload is unreadable (`MetadataVersionUnsupported`)

### Symptoms
- `Degraded=True` with reason `WorkloadSelectorMismatch` and a `WorkloadSelectorMismatch` Warning Event naming a Deployment (`mysql-<group>-<site>`) or a Dragonfly StatefulSet (`<group>-dragonfly-<site>`). Spec changes no longer reach that site; its Services may have no endpoints when the old pods carry other label keys. The deployment API reports `unstableReason: WorkloadSelectorMismatch`.
- `Degraded=True` with reason `MetadataVersionUnsupported` and a Warning Event of the same reason naming `bloodraven.shipstream.io/planned-failover-terminal` or a deployment Lease (`bloodraven.shipstream.io/deployment-lease`). Lease requests fail while a lease record is unreadable. On a `MysqlBackupVerification`, `Verified=False/MetadataVersionUnsupported` ends the run.

### Diagnostic steps
1. Compare the live selector with what the message says the operator renders:
   ```bash
   kubectl get deployment mysql-<group>-<site> -n <ns> -o jsonpath='{.spec.selector.matchLabels}{"\n"}'
   kubectl get statefulset <group>-dragonfly-<site> -n <ns> -o jsonpath='{.spec.selector.matchLabels}{"\n"}'
   ```
   The operator renders `app.kubernetes.io/name`, `app.kubernetes.io/instance` and `bloodraven.shipstream.io/site`. A selector with `shipstream.io/site` is an object left by an earlier release; one with other keys was created by hand under the operator's name.
2. For `MetadataVersionUnsupported`, read the payload's `schemaVersion`. A higher number than this operator reads means a newer operator wrote it (a downgrade, or two operator versions running against one group):
   ```bash
   kubectl get mfg <group> -n <ns> -o jsonpath='{.metadata.annotations.bloodraven\.shipstream\.io/planned-failover-terminal}{"\n"}'
   kubectl get leases -n <ns> -l bloodraven.shipstream.io/deployment-kind -o json | jq -r '.items[] | [.metadata.name, (.metadata.annotations["bloodraven.shipstream.io/deployment-lease"] | fromjson? | .schemaVersion)] | @tsv'
   ```

### Safe remediation
- **Selector mismatch.** The selector is immutable, so the operator never updates the object and does not retry in a loop. Delete the object and the operator re-creates it with the rendered selector:
  ```bash
  kubectl delete deployment mysql-<group>-<site> -n <ns>      # the PVC mysql-<group>-<site>-data is kept
  kubectl delete statefulset <group>-dragonfly-<site> -n <ns>
  ```
  Delete one site at a time, and never the active primary's Deployment while the other sites cannot take over: its MySQL pod stops until the new Deployment's pod starts. Prefer a planned failover away from the site first. The condition clears on the next reconcile after the object is re-created. A snapshot-restore Dragonfly upgrade never touches a refused StatefulSet: it waits (`DragonflyUpgradeDeferred`) while any site is refused, and a run in flight ends `status.dragonfly.upgrade.phase=Failed` with `reason=WorkloadSelectorMismatch` before its next SAVE, pod delete or image change; after the fix, retry it as for any failed run (playbook 4, steps 5 and 6; the retry steps are in the docs' Snapshot-restore upgrade section).
- **Unknown payload version.** Run the operator release that wrote it. A terminal marker naming a run with outcome `ManualRecoveryRequired` still holds automatic failover (fail closed): resolve the run and acknowledge it (playbook 38); any other terminal marker proves nothing and is ignored. A Lease record of an unknown version: once no deployment client uses it, delete the Lease; the refusal and its `Degraded` reason clear without an operator restart once the operator sees the Lease is gone. A refused verification: create a new verification.

Do not:
- Patch the selector of a refused workload (the API server rejects it) or edit a payload's `schemaVersion` by hand: a payload of a known version but foreign content can be read as proof.

## 45. Binary log rotation failed at a promotion, planned handoff or reclone (`promotion binlog rotation failed`)

### Symptoms
- Operator log WARN `promotion binlog rotation failed` (`site`, `durationMs`, `timedOut`, `error`) during an emergency failover, an ordered-update handoff or a planned failover. The promotion is unaffected: it went on with the same steps and succeeded or failed for its own reasons.
- WARN `outgoing primary binlog rotation failed` (same fields, plus `runId` for a planned failover or `candidate` for an ordered-update handoff): the same statement, sent to the fenced source of a planned handoff.
- INFO `planned failover: fenced source binlog not rotated` (`runId`, `site`, `reason`): a planned failover proved its fence on the source without sending the rotation, because only the pass that sends the fence and reads it back rotates. `reason` is `fenceReplyLost` or `fenceSentByEarlierProcess` (an operator restart mid-fence). The consequence and the remedy are those of a failed rotation.
- WARN `pre-reclone binlog rotation failed` (`site`, `durationMs`, `timedOut`, `error`): the same statement, sent to a site before its reclone. It is retried on every topology poll; if it is never acknowledged the reclone ends in playbook 46.

### Cause
The operator sends one best-effort `FLUSH BINARY LOGS`, bounded at 1 second (connection setup included), on a connection of its own:
- to the site being promoted, after `RESET REPLICA ALL` and before its writes are enabled, so everything it logged as a replica is in sealed files its sidecar can archive;
- to the fenced source of a planned failover or ordered-update handoff, so the last transactions it took as the primary are in a sealed file;
- to a site before `CLONE INSTANCE` wipes it (PITR enabled only).

A failed or timed-out rotation changes nothing else. The site's last binary log stays unsealed until the next rotation (`max_binlog_size`, a mysqld restart, a manual flush), so the PITR archive lags until then. The matching INFO lines (`promotion binlog rotated`, `outgoing primary binlog rotated`, `pre-reclone binlog rotated`) carry the measured `durationMs`.

### Diagnostic steps
1. Read the record:
   ```bash
   kubectl logs -n <operator-ns> deploy/bloodraven | jq -c 'select(.msg | test("binlog rotation failed$"))'
   ```
   - `timedOut: true` (`error`: `no answer within 1s; continuing without it`): the site or the path to it was slow at that moment. Check the site's load then (commit stalls, disk latency, a slow connection handshake across regions). The server may still have completed the rotation afterwards.
   - `error` containing `Error 1227` (`Access denied; you need (at least one of) the RELOAD privilege(s)`): the operator account lacks `RELOAD`. Check `SHOW GRANTS FOR CURRENT_USER()` as the operator account. `ALL PRIVILEGES ON *.*` includes it; `SUPER` does not.
   - A connection error (refused, TLS, authentication): the rotation uses its own connection, so compare with the operator's probes of that site at the same time.
   - `context canceled`: the pass or the topology manager stopped while the rotation was in flight.
2. Check whether the file has been sealed and archived since: `scripts/sidecar-probe.sh -n <ns> <group> archiver` prints each site's `tail` (`activeFile`, `newestSealedFile`, `state`, `pendingFiles`, `reason`); compare `tail.activeFile` with `SHOW BINARY LOG STATUS` on the site.

### Safe remediation
- Nothing is needed for the failover, handoff or reclone itself.
- To seal the file now, run `FLUSH BINARY LOGS` on the site that holds it (the new primary for the promotion WARN, the fenced former primary for the outgoing-primary WARN). The statement is not written to the binary log, creates no GTID and works under `super_read_only=ON`:
  ```bash
  kubectl exec -n <ns> <pod> -c mysql -- mysql -e "SHOW BINARY LOG STATUS; FLUSH BINARY LOGS;"
  ```
  Then confirm the upload: the site's `/archiver/status` reports `tail.state: Drained` with `tail.newestSealedFile` naming the file that was active, or (plaintext archives) `manifest-<site>.json` lists it under `files[].name`. A read-only former primary uploads its tail too; if it reports `tail.state: Blocked`, continue with playbook 46.
- Missing `RELOAD`: restore the operator account's documented grant (`ALL PRIVILEGES ON *.*`), or at least `GRANT RELOAD ON *.*` to it on the primary.

Do not:
- Restart mysqld or run `PURGE BINARY LOGS` to force a rotation, and do not treat this WARN as a failed failover.

## 46. A reclone started without its binary log tail archived (`PITRArchiveTailNotArchived`), or a read-only site does not archive its tail

### Symptoms
- Warning Event `PITRArchiveTailNotArchived` on the group: "site `<s>` is being recloned although its binary log tail was not confirmed archived in time: `<cause>`. CLONE wipes the site's binary logs; a point-in-time restore whose window needs transactions only they held refuses PITRArchiveGap."
- Operator log INFO `pre-reclone archive tail wait started` (`site`, `timeout`) followed by WARN `pre-reclone archive tail not archived` (`site`, `waitedMs`, `reason`) instead of INFO `pre-reclone archive tail archived`.
- Sidecar log WARN `binlog tail not archived while read-only` (`reason`, `pendingFiles`), and `/archiver/status` `tail.state: Blocked` on a read-only site.
- Later, a restore or verification refuses `PITRArchiveGap` with "committed on `<site>` and never archived".

### Cause
A read-only site's sidecar archives the tail of its last writable era: every unarchived sealed binlog up to the last one that holds a transaction its own server originated, never pure replica copies. It uploads only with a verified archive session, no history-integrity marker, no pending incarnation-reset record, and when the latest record in its manifest is a promotion record of the same server incarnation. `/archiver/status` reports the result in `tail{state, activeFile, newestSealedFile, pendingFiles, reason}`: `Drained`, `Pending` or `Blocked`. The state is about the index in which `activeFile` was the active file: compare `activeFile` with `SHOW BINARY LOG STATUS` before trusting `Drained` for a file sealed recently. A scan that could not read the index reports no file names. The block is absent until a scan has read the binlog index, and from older sidecars. A tail upload that fails sets `lastError` and `backlogFiles`, which stay set until a scan uploads the file or finds nothing left to upload.

Before a reclone (PITR enabled only) the operator reads that block (the baseline), sends the site one `FLUSH BINARY LOGS`, reads the name of the file the server writes afterwards (`rotatedFile` in the operator log), and waits for `tail.state: Drained` from a scan that saw the rotation: `tail.activeFile` is that file or a later one. A `Drained` that names an earlier file is a scan from before the rotation and does not count. The sidecar gets 2 minutes from the acknowledged rotation; an unacknowledged rotation is retried for 2 minutes, and the wait never lasts longer than 4 minutes. Then the operator clones regardless, and records the Event when the tail was not confirmed. The wait runs in the background: a site that does not answer does not slow failover detection. The cause in the message is one of:
- `the site's sidecar did not answer: …`: the sidecar was unreachable (playbook 31, `SidecarUnreachable`).
- `the site's sidecar reports no archive tail …`: no scan has read the binlog index yet, or the sidecar image predates the `tail` block.
- `FLUSH BINARY LOGS on the site was not acknowledged, or the name of the file it opened could not be read`: playbook 45 (`pre-reclone binlog rotation failed`; its `error` says which). `FLUSH BINARY LOGS on the site was not sent: …` means another topology change (a planned failover, an ordered update, a promotion) held the sites.
- `the site's sidecar has not reported a scan of the rotated binary log yet (its last scan saw <file> as the active file; the rotation opened <file>)`: the sidecar has not scanned since the rotation, or its scans name earlier files than the rotation opened (the binlog numbering started again after the rotation).
- `the site's sidecar last scanned with <file> as the active binary log, but the rotation opened <file>: the numbering of the site's binary logs started again …`: the sidecar's last scan before the rotation names the rotated file or a later one, so it is a scan of an earlier numbering (`RESET BINARY LOGS AND GTIDS`, a new data directory). The rotation is sent again until the sidecar has scanned the current files.
- `… which cannot be compared with <file> …`: the two names have different base names or no numeric suffix.
- `the status request to the site's sidecar or the rotation of its binary log did not return within 10s`: a call hung past its own timeouts.
- `the site's archiver still had N sealed binary log(s) to archive`: the upload did not finish in time (a large file, a slow store).
- `the site's archiver cannot archive the tail: <reason>`: `Blocked`. The reason is a failed upload (`lastError`), or a refusal: the site's latest record is not a promotion record (it was recloned or reset since it last wrote), it has no promotion record, the archive session is not verified, or a binlog file cannot be read (`read the header of mysql-bin.NNNNNN: …`; when the text names the keyring or an encrypted binlog, see playbook 49).
- `the site's sidecar runs no binlog archiver`: the pod has not rolled onto the PITR configuration. This one ends the wait at once.

### Diagnostic steps
1. Read the Event and the operator's records of the wait:
   ```bash
   kubectl get events -n <ns> --field-selector involvedObject.name=<group>,reason=PITRArchiveTailNotArchived
   kubectl logs -n <operator-ns> deploy/bloodraven | jq -c 'select(.msg | startswith("pre-reclone"))'
   ```
2. Read every site's tail now: `scripts/sidecar-probe.sh -n <ns> <group> archiver` (or `scripts/triage.sh`, which prints `tail=` per site). For a site that has not been recloned yet, `tail.reason` and `lastError` say what blocks it.
3. Decide which restore targets are affected. Only windows that need transactions that tail alone held are affected; the candidates are the transactions the site committed as the primary after its last archived binlog. Fetch the plaintext `manifest-<site>.json` and note its newest `files[].name` and time range (`scripts/gtid-audit.sh -n <ns> <group> --manifest manifest-<site>.json` prints the records). To test a specific target, run a `MysqlBackupVerification` with `pointInTime`; a `PITRArchiveGap` refusal lists the safe targets and backups. Targets served by a backup taken after the reclone are not affected.
4. If the reclone followed a lossy failover and the tail was archived (no Event), the recloned site's audit records its own tail row as divergent: `divergentRows` above 0, no `lastError`, `PITRArchiveReady` still `True` with the site named in its message. That is expected evidence (playbook 27), not a failure, and needs no quarantine. Only a site whose `/archiver/status` reports a non-empty `reissuableGtids` (its server kept its `server_uuid`) needs action: playbook 41.

### Safe remediation
- Nothing recovers a wiped tail. Take a full backup now and verify it, so later targets do not depend on it: `kubectl bloodraven backup <group> -n <ns>`.
- For a target inside the lost tail, use the safe targets or backups the `PITRArchiveGap` message names (playbook 42).
- Before recloning an old primary, check its tail first: `tail.state: Drained` means nothing is left to archive. For `Blocked`, fix the reason when it is fixable (store unreachable, archive session not verified, rotation not acknowledged). A site whose latest record is a reset record, or that has no promotion record, has no tail the archive accepts. A site with `binlog_encryption=ON` archives its tail like any other; when its reason is a decryption error, follow playbook 49 first.
- A tail that was archived with transactions the new primary never received makes restores across it refuse `PITRArchiveDivergentHistory` until they are in `excludeGtids` (playbook 42).

Do not:
- Delete or edit manifest rows, or copy binlog files into the archive by hand, to close a gap.

---

## 47. A cloned site kept its `server_uuid` (`CloneServerUUIDNotRotated`)

### Symptoms
- Warning Event `CloneServerUUIDNotRotated` on the group: "site `<s>` was cloned and kept its server_uuid `<uuid>`: the mysql container's launcher did not remove auto.cnf on the restart that installed the clone. …".
- Operator log WARN `clone recipient kept its server_uuid` (`site`, `serverUuid`) where a healthy clone logs INFO `clone recipient server_uuid rotated` (`site`, `previousServerUuid`, `serverUuid`). INFO or WARN `clone recipient server_uuid rotation not verified` (`reason`) means the operator could not read the value before or after the clone; it says nothing either way.
- Nothing else: the bootstrap completes, the site replicates and stays a failover candidate.

### Cause
`CLONE INSTANCE` does not copy `auto.cnf`, so a recipient keeps its `server_uuid` unless something removes that file before mysqld starts on the cloned data. The mysql container's `command` is the operator's launcher (a bash script that ends by running the image's entrypoint). On the start that installs a finished clone (`/var/lib/mysql/#clone/#replace_files` present, `#status_error` absent) it removes `/var/lib/mysql/auto.cnf`, mysqld generates a new `server_uuid`, and the recipient's own transactions start at `<new uuid>:1`. That is what keeps a recloned old primary from reissuing the GTIDs of transactions it lost at a failover (playbook 41). The operator reads `@@server_uuid` before the clone and after the restart, and reports an unchanged one. The launcher did not rotate because:
- the pod was created before the operator release that introduced the launcher (its mysql container has no `command`); it rolls with the group's pending update;
- `spec.image` starts mysqld in a way that bypasses the launcher;
- the launcher could not remove `auto.cnf` or read `#clone` (an unusual volume ownership or security context);
- with encryption at rest and an encrypted binary log, the clone was installed by a sealed pod (the keyring is read-only): a new `server_uuid` needs a new binary log encryption key, which mysqld could not store, so the launcher keeps the old identity on purpose. The operator keeps a recipient `Unsealed`/`Clone` until its clone's restart is confirmed, also across an operator restart, so this takes a lost hold (the `bloodraven.shipstream.io/clone-server-uuid` annotation removed by hand). With `coverage.binaryLog: false` a sealed pod rotates, unless `mysqld-auto.cnf` names `binlog_encryption`.

The Event does not need the operator that issued the clone: the pre-clone `server_uuid` is recorded in the group annotation `bloodraven.shipstream.io/clone-server-uuid` before `CLONE INSTANCE`, and a clone that finished after an operator restart is compared by the next operator (its next bootstrap of the site, or a poll). The annotation goes away when the clone has its outcome. An entry that stays means the site's clone is still copying, the site does not answer, or mysqld was killed during the copy (the site is then empty and needs a clone anyway).

An ordinary restart, a pod replacement and a clone that failed or was killed never change `server_uuid`; none of them raises this Event.

### Diagnostic steps
1. Read the Event and the operator's verdict:
   ```bash
   kubectl get events -n <ns> --field-selector involvedObject.name=<group>,reason=CloneServerUUIDNotRotated
   kubectl logs -n <operator-ns> deploy/bloodraven | jq -c 'select(.msg | startswith("clone recipient") or startswith("clone restart not confirmed"))'
   kubectl get mysqlfailovergroup -n <ns> <group> -o jsonpath='{.metadata.annotations.bloodraven\.shipstream\.io/clone-server-uuid}{"\n"}'
   ```
2. Check that the pod runs the launcher and read its line (it logs only on a start that finds a clone staged):
   ```bash
   kubectl get pod -n <ns> <pod> -o jsonpath='{.spec.containers[?(@.name=="mysql")].command[0]}{"\n"}'   # /bin/bash
   kubectl logs -n <ns> <pod> -c mysql | grep bloodraven-launcher
   kubectl get mysqlfailovergroup -n <ns> <group> -o jsonpath='{.status.conditions[?(@.type=="Updating")]}'
   ```
   `removed auto.cnf so mysqld generates a new server_uuid` is the healthy line. `WARNING: … NOT rotated` names the reason (the file could not be removed, `#clone` could not be read, the keyring is sealed). No line and no `command` means the pod has not rolled onto the current pod template.
3. Decide whether it matters now: `scripts/sidecar-probe.sh -n <ns> <group> archiver` and read the site's `reissuableGtids`. A set means the site lost transactions it issued under the `server_uuid` it still has (playbook 41). `""` means nothing can be reissued today; the site still should get a new identity at its next clone.

### Safe remediation
- Let the pending update finish (or fix the image or the volume ownership the launcher's line names), then reclone the site: `kubectl bloodraven reclone <group> <site> -n <ns>` (add `--cold` when it has no recorded divergence). The operator logs `clone recipient server_uuid rotated`.
- Until then, do not promote a site whose `reissuableGtids` is not empty. If it is promoted, follow playbook 41 (new archive prefix and a full backup).
- A custom `spec.image` needs `/bin/bash` and its entrypoint at `/usr/local/bin/docker-entrypoint.sh` or `/entrypoint.sh`, or `docker-entrypoint.sh` on `PATH`. Without one the mysql container exits with code 127 and `bloodraven-launcher: no MySQL image entrypoint found …` (the line names the remedy), the pod reads `mysql not ready: crash looping, last exit code 127`, and an ordered update stalls on that site with the primary untouched; under `updateStrategy.type: Recreate` every site crash-loops at once. Check an image with `docker run --rm --entrypoint /bin/bash <image> -c 'test -x /usr/local/bin/docker-entrypoint.sh || test -x /entrypoint.sh || command -v docker-entrypoint.sh >/dev/null' && echo ok`.

Do not:
- Delete `auto.cnf` by hand on a running or stopped site to "fix" the identity. On a site that was not just cloned, its own transactions are still in its `gtid_executed`, and a new identity on the primary starts a new archive incarnation. Reclone instead.
- Remove the mysql container's `command` or edit the Deployment: the operator renders it and rolls the pod back.
- Remove the `bloodraven.shipstream.io/clone-server-uuid` annotation while a clone has no outcome: the clone is then never checked, and with encryption at rest the recipient may be sealed before the restart that installs the clone.


## 48. A site lacks transactions it issued itself (`OwnTransactionsLost`, `recoveryState: RecloneRequired`)

### Symptoms
- Warning Event `OwnTransactionsLost` on the group: "`<site>` lacks N transactions it issued itself that `<other site>` holds (`<set>`); …". The rest of the message says which of two cases this is (see Cause): "replication cannot bring them back … The operator does not replicate or promote it and reclones it", or "the primary does not hold them either, so the site keeps replicating and is not recloned automatically; it is not promoted while `<other site>` holds them".
- `status.sites[].recoveryState: RecloneRequired` with `lostGtid`, `lostTransactionCount` and `lostComparedTo` (the site that holds the transactions); `RecoveryPending=True` and `Degraded=True` with reason `OwnTransactionsLost`; `bloodraven_own_transactions_lost{site}` above 0; the deployment API reports `unstableReason: RecloneRequired`. `kubectl bloodraven status <group>` lists the set and the whole condition message under "Site reports".
- Operator log WARN `own transactions lost` (`site`, `lostGtid`, `comparedTo`, `serverUUIDs`, `detectedBy`, `remedy`), then, for a site that lacks what the primary holds, INFO `reclone requested for a site that lacks transactions it issued itself` and the usual `starting bootstrap` with `source=reclone`.
- A site that lacks what the primary holds is read-only and fenced. If it has no replication channel, none is configured; `sourceConvergenceReason: OwnTransactionsLost` on a site whose stopped channel is not restarted. A site whose report rests on another site keeps replicating.
- A site with `lostGtid` and `recoveryState: RecoveryBlocked` both lacks transactions it issued and holds transactions the primary lacks: playbook 2 or 33 applies first, and nothing is cloned over it automatically. WARN `automatic reclone not started: the check before the clone does not allow it` with `extraGtid` is the operator finding that when a clone was about to start.
- Warning `OwnTransactionsLostRecloneFailing`: the automatic reclone has failed three or more times in a row.
- A planned failover that ended `SourceLeftFenced` with operator WARN `planned failover: rollback leaves the source fenced, it lacks transactions it held` found the same condition on its source during the rollback (playbook 37 for the run; the source is then reported here). Its `lostGtid` is what the source lacks by the evidence of the fenced set, the target or any other site that answered. An ordered update that logged ERROR `maintenance handoff: source left read-only: …` left a restarted active site read-only for the same reason. ERROR `maintenance handoff: source is writable and no longer holds transactions it held before the handoff` is the same finding on an active site that came back writable: it is the primary, nothing fences it, the standby that holds the transactions reads `RecoveryBlocked`, and ERROR `the primary lacks transactions it issued itself, which another site holds` names the pair. Decide which history to keep (playbook 33 for the standby; to keep the standby's, stop writes, make it the primary by hand and reclone the former primary).
- With the primary lost: Warning `PromotionRefusedOwnTransactionsLost`, `Degraded` reason `NoPrimary` with "refusing to promote `<site>` (lacks transactions it issued itself; …)". WARN `primary re-assert refused: a peer holds transactions the target issued itself and lacks` is the same refusal for the last primary.
- A reported site that turned up writable beside another writable site: WARN `split-brain: fencing a writable site that lacks transactions it issued itself`. This also holds when `status.lastFailover.target` names the reported site. When every writable site carries a report nothing is fenced (`PromotionRefusedOwnTransactionsLost`): decide by hand.
- INFO `replica GTID audit: the follower lacks transactions under its own server_uuid that the primary holds, and its applier has not executed the primary's log that far yet; …`: not a report. The site's replication runs and it is behind; this is what a replay of its transactions onto the primary looks like. Nothing is reported unless the transactions are still missing once its applier has passed the primary's position.
- INFO `own transactions lost kept: the primary does not hold the transactions, the report stands until every other site has been compared`: a report that was restored after an operator restart, or made against a former primary, was compared with the primary and found clean there. It stays, as a report that rests on another site. If the stored report named the primary itself, it now names no site (`lostComparedTo` is empty): the primary is known not to hold the transactions.
- On an installed CRD from before this state existed, the site reads `recoveryState: RecoveryBlocked` with no `divergentGtid` and no `lostGtid`, and the operator logged WARN `status written with the values an older CRD accepts; the installed CRD is older than the operator` once. The conditions and Events still say `OwnTransactionsLost`. An operator that restarts on that status restores the report without its set (INFO `restored own transactions lost state from CR status without its set: the installed CRD is older than the operator`): the site stays out of promotion, the `RecoveryPending` message says the set was not stored, and the report ends once every other site has been compared and none holds transactions the site lacks. Apply the CRDs of the running release. With the CRDs applied such a report reads `recoveryState: RecloneRequired` with no `lostGtid` until it ends or a comparison finds the set again, and a further operator restart restores it from that (INFO `restored own transactions lost state from CR status without its set`).

### Cause
The site came back with fewer transactions than it issued: its volume came back older (a snapshot rollback, a restore outside the operator, `RESET BINARY LOGS AND GTIDS`), or a crash lost the end of the redo log and the binary log together. Another site still holds them. A MySQL replication receiver drops every event that carries the replica's own `server_id` (the filter is by `server_id`, not by `server_uuid`; verified on MySQL 9.7: the events never reach the relay log, `Retrieved_Gtid_Set` never lists them, both threads report `Yes`, no error). So replication never returns transactions the holder received from the site, and promoted under the same `server_uuid` the site would issue the same GTIDs for other transactions.

One exception matters for remediation: transactions that were applied on the primary from a decoded binary log (`mysqlbinlog | mysql`) keep their `server_uuid` and are logged with the primary's `server_id`, so every replica receives them, the site that issued them included (verified on MySQL 9.7). While such a replay runs, that site lacks transactions under its own `server_uuid` that the primary holds, and is only behind.

Two cases, told apart by who holds the transactions (`comparedTo` in the log line, the site named in the Event):

1. **The primary holds them.** The site has a hole replication cannot fill. The operator does not start its replication, does not promote it, and reclones it. It finds this when it compares a site with the primary: before it configures or restarts a site's replication, in the replica GTID audit (every 30 s per replicating site), before an ordered update starts a restarted site's replication, and when a clone is about to start. This comparison runs under every `server_uuid` the site has had (group annotation `bloodraven.shipstream.io/server-uuid-history`, because a site keeps its `server_id` across reclones and changes its `server_uuid` at each). For a site whose replication from the primary runs, the operator counts a deficit only once the site's applier (`Relay_Source_Log_File`, `Exec_Source_Log_Pos`) has executed the primary's binary log past the position at which the primary held the transactions (`SHOW BINARY LOG STATUS`): before that it may be lag. A site without a running channel is compared once.
2. **Only another site holds them** (a witness: a replica or disaster-recovery site that itself reads `RecoveryBlocked` because it holds transactions the primary lacks, or any site that answers while there is no primary). Against the primary the reported site lacks nothing, so it replicates normally; it is not promoted, because it would issue those GTIDs again while the witness holds the first ones. The operator finds this when the audit or recovery finds the divergent site, and when it chooses a promotion target with the primary gone. A witness counts only under a `server_uuid` of the site that no clone has superseded (the annotation records a `clone` entry when a clone is known to have installed, never for one that failed; a `server_uuid` the site is seen under after a `clone` entry is recorded again): what a site lacks of an identity it had before a clone, it gave up at that clone. A site listed in `status.restoreReclones.pending` is never a witness.

What ends a report: the site's reclone; for case 1, a clean comparison with the same primary; for case 2 (and for a report restored after an operator restart, or made against a former primary), one pass in which every other site of the group answers (the primary as the writable site, every other site read-only) and none holds transactions the site issued and lacks. The operator makes that pass every 30 seconds. A clean comparison with the primary alone does not end a case-2 report, and neither does a site that does not answer. A failed primary is a site that does not answer: no such report ends during a primary outage, and if the reported site is the only candidate there is no automatic failover until the primary returns (the last three items of Safe remediation are the ways out; no annotation overrides a report). `status.sites[].lostComparedTo` stores the holder, so the report survives an operator restart with it.

### Diagnostic steps
1. What the operator recorded:
   ```bash
   kubectl bloodraven status <group> -n <ns>
   kubectl get mysqlfailovergroup <group> -n <ns> -o jsonpath='{range .status.sites[*]}{.name}{": "}{.recoveryState}{" lost="}{.lostTransactionCount}{" "}{.lostGtid}{" divergent="}{.divergentGtid}{"\n"}{end}'
   kubectl get mysqlfailovergroup <group> -n <ns> -o jsonpath='{.status.conditions[?(@.type=="RecoveryPending")].message}'
   ```
   The `RecoveryPending` message names the site that holds the transactions (also in `status.sites[].lostComparedTo`) and says whether the operator is recloning the reported site, or why not ("automatic reclone waiting: …", or "the transactions are held by `<site>`, not by the primary").
2. Confirm on the servers (read-only): `./scripts/gtid-audit.sh -n <ns> <group>` prints `OWN TRANSACTIONS LOST` with the set for a follower that lacks transactions under its current `server_uuid` which the primary holds (case 1), and says whether that is proven (the follower's applier has passed the primary's position) or the follower has no running channel. It prints `BEHIND, NOT LOST (so far)` for a follower whose replication runs and whose applier has not reached the primary's position: wait and run it again. For case 2 it prints that the operator records `lostGtid` (with `lostComparedTo`) although the primary holds nothing the site provably lacks, and reports the witness as `DIVERGENT`. By hand, on the site: `SELECT @@server_uuid, GTID_SUBTRACT('<holder gtid_executed>', @@GLOBAL.gtid_executed)` and keep the intervals of the site's own uuid.
3. The recorded identities: `kubectl get mysqlfailovergroup <group> -n <ns> -o jsonpath='{.metadata.annotations.bloodraven\.shipstream\.io/server-uuid-history}'`. A lost set under a uuid the site no longer runs under means its volume came back from before a reclone.
4. Is an in-place restore unfinished? `kubectl get mysqlfailovergroup <group> -n <ns> -o jsonpath='{.status.restoreReclones}'`. Sites in `pending` hold the data from before the restore. They are never the right copy to promote or to clone from, whatever their GTID sets say.
5. Why the volume regressed is outside the operator: check the storage layer (snapshot restores, a PV re-bound to an older volume) and the site's mysqld log around its last start.

### Safe remediation
- Case 1, usually none: the operator reclones the site from the primary and the report ends (`OwnTransactionsLostResolved` with "it was recloned", INFO `own transactions lost cleared: the site was recloned`). Before the clone starts the operator fences the site and compares it with the primary once more; the clone runs only if the primary holds everything the site holds. With PITR the site's binary log tail is archived first (playbook 46 if that fails).
- Case 1, the automatic reclone is waiting (the `RecoveryPending` message says for what):
  - "the last automatic reclone did not complete" / "the last N automatic reclones did not complete": `Degraded` reads `BootstrapFailed` with the clone's error. Fix the cause (the `BootstrapFailed` row of the alert matrix). The operator asks again after 5, 10, 20, 40 and 80 minutes and then every 2 hours; the message gives the next time. To reclone now: `kubectl bloodraven reclone <group> <site> -n <ns> --cold`.
  - "an in-place restore holds the site": its reclone queue reclones the site (playbook 20).
  - "there is no confirmed writable primary to clone from": restore the primary first (below).
  - "another reclone is pending" / "another clone is running": it starts when that one ends.
  - "another topology change holds the sites": an ordered update, a planned failover or its automation hold. It starts when that ends (an automation hold needs its acknowledgement, playbook 37).
  - "the report was restored from status …" / "the report was made against `<site>` …": the operator has not compared the site with the current primary yet. It does within about 30 s once nothing holds the sites. With no writable site and the reported site being the recorded primary, the primary re-assert makes that comparison against every peer once all of them answer read-only: the report then either names the peer that holds the transactions, or ends (INFO `own transactions lost cleared: no site holds transactions the site lacks`) and the primary is made writable again.
- Case 2: no automatic reclone is coming. Resolve the witness, which is `RecoveryBlocked` (playbook 2 or 33). Reclone it to discard the transactions (the report on the other site ends within 30 s), or keep them: replay them onto the primary (`mysqlbinlog --include-gtids=… | mysql` on the primary). The primary logs them with its own `server_id`, so the reported site receives them by replication like every other replica; while the replay runs the operator reports nothing new about it (INFO `replica GTID audit: the follower lacks transactions under its own server_uuid …` once), and its report ends when it has applied them. Do not stop the reported site's replication during the replay: a site without a running channel is compared once, reported against the primary and recloned (harmless, but a full copy). Recloning the reported site instead also ends its report, because the clone gives it a new `server_uuid`: `kubectl bloodraven reclone <group> <site> -n <ns> --cold`. Until one of these happens the reported site is not a failover candidate, and an ordered update or planned failover onto it is refused with the same instructions.
- A report stays although the site it names was repaired. The report ends only when every other site of the group has answered in one pass: look for a site that is unreachable or not classified read-only (`status.sites[].state`), or another site that holds the same transactions (the report then moves to it: check `lostComparedTo`).
- The site that held the transactions is gone for good and the report stays. A report is evidence the operator had, and it is not dropped because a site stopped answering (a site with no report, by contrast, is promoted when no holder answers: silence is not evidence either way). A case-2 report ends by itself when every other site answers without the transactions; a site that was removed from `spec.sites`, or queued for a post-restore reclone, is not asked; a site that is merely unreachable keeps it. If the holder will not return, either remove it from `spec.sites` (case 2), or reclone the reported site if a primary exists, or follow the next item if none does.
- The primary is lost and the remaining site carries a report. The operator will not promote it. Prefer bringing the primary back: a case-1 site is then recloned from it. If the primary is gone for good, the site is the best copy left and you accept the loss of everything it lacks: first make sure no other site is writable and that the site is **not** listed in `status.restoreReclones.pending` (step 4); then stop its mysqld, remove `auto.cnf` from its data directory so it starts under a new `server_uuid` (this is the one case where that is right: under its old identity it would issue the lost GTIDs again), start it, and make it writable (`SET GLOBAL super_read_only=OFF; SET GLOBAL read_only=OFF`). The operator adopts the unique writable site. It records `OwnTransactionsLostSitePromoted` and drops the report only once every other site of the group is seen read-only; while the dead primary is still in `spec.sites` and does not answer, the adopted site stays reported (`RecoveryPending`: "the site was made writable without a reclone; the report stays until every other site is seen read-only"), so that it, not the old primary, is fenced if the old primary turns up writable. Remove the dead site from `spec.sites` to end that. Start a new PITR archive prefix and take a full backup afterwards (playbook 41).
- The recorded primary itself restarted without part of its history and a replica holds it (all sites read-only, the primary `RecloneRequired` compared with the replica): the operator elects nobody. Check step 4 first. If the replica is listed in `status.restoreReclones.pending`, it holds the data from before an in-place restore: do **not** make it writable (that undoes the restore, and the operator would then clone the restored primary from it); the operator does not report a restored primary on the evidence of such a site, so in that state look for another cause (playbook 20). Otherwise make the replica that holds everything writable by hand; the operator adopts it and reclones the former primary.

Do not:
- Configure or start replication by hand on a site that lacks what the primary holds (`CHANGE REPLICATION SOURCE`, `START REPLICA`). It will run without errors and never apply the lost transactions; the replica GTID audit reports it again.
- Inject the lost GTIDs as empty transactions on the site to make the sets equal: the site then claims transactions whose changes it does not hold.
- Replay a divergent replica's transactions onto the primary of a group that logs WARN `own transactions lost: the primary's binary log position could not be read` while the replicas replicate. The line means the primary will not report a position (its binary log is off, or the operator's account on the primary gets error 1227 for `SHOW BINARY LOG STATUS`): without the position the operator cannot tell lag from loss, reports the sites that issued those transactions and reclones them. Grant `REPLICATION CLIENT` first. (`SHOW REPLICA STATUS` needs the same privilege. An operator account that lacks it on the replicas too cannot read their channels, and then no loss is concluded for any replica until it is granted; the line is not logged in that case. A read that fails for a passing reason, such as a timeout, is retried and concludes nothing.)
- Remove the `bloodraven.shipstream.io/server-uuid-history` annotation or edit its `clone` entries: a site back on a volume from before a reclone is then compared under its current `server_uuid` only, and a recloned site can be refused as a failover target for transactions it discarded on purpose.
- Remove `status.sites[].lostGtid` by hand to make the site a failover candidate.
- Make a reported site writable while another site is writable: the operator fences it again.
- Promote, or clone from, a site in `status.restoreReclones.pending` to "get the lost transactions back" after an in-place restore. Those transactions were discarded by the restore.

---

## 49. PITR archiver cannot decrypt a binary log (encryption at rest)

On a group with `spec.encryptionAtRest` MySQL encrypts the binary log (`binlog_encryption=ON`) unless `coverage.binaryLog` is `false`. The sidecar decrypts each local binlog file with the binary log encryption key the file's header names, read from the site's keyring file (the file mysqld uses, mounted read-only into the sidecar), and archives the plaintext, exactly what a group without `binlog_encryption` archives. An encrypted file is read correctly or not at all: this playbook is for a file that is not, and for the report about an archive without encryption of its own.

### Symptoms
- `PITRArchiveReady=False` with reason `ArchiverError` and a Warning `PITRArchiveNotReady`; `status.pitr.message` reads `<site>: archive mysql-bin.NNNNNN: <text>`. Pods are Ready and applications are unaffected. `/archiver/status` of the writable site shows the same `lastError`, a rising `backlogFiles` and `consecutiveFailedScans`; sidecar WARN `archive binlog` with the text in `error`. The texts:
  - `the keyring does not hold the binary log encryption key the file names: MySQLReplicationKey_<server_uuid>_<n>`
  - `the binlog file is encrypted by MySQL (binlog_encryption=ON) and the sidecar has no keyring file to decrypt it with (BLOODRAVEN_KEYRING_FILE is not set)`
  - `encrypted binlog header version N is not supported (only version 1 is)`, or another text that starts with `encrypted binlog header:`
  - `the binlog file does not decrypt to a binlog with keyring key <key id> (magic …)`
  - `read the keyring file: …` or `the keyring file is not the JSON component_keyring_file writes`
  - `invalid binlog magic`, with `filesArchived: 0` or an archive that stops where binary log encryption began: the pod's sidecar predates the release that archives encrypted binlogs.
- `/archiver/status` of a read-only site: `tail.state: Blocked` with `reason` `read the header of mysql-bin.NNNNNN: <one of the texts above>` and WARN `binlog tail not archived while read-only`. A reclone of that site waits 2 minutes for its tail and then raises Warning `PITRArchiveTailNotArchived`.
- Sidecar INFO `promotion set not recorded` with `reason="read the header of mysql-bin.NNNNNN: …"` at a promotion; that promotion record has no `promotionGtidExecuted`.
- Separately, with archiving healthy: a Warning Event `PITRArchiveNotEncrypted`, and the `EncryptionAtRestReady` condition's message ends with `; the PITR archive (backup profile "<name>") has no encryption of its own: archived binary logs are protected only by what the store provides (archive through a profile that has spec.backup.profiles[].encryption, or encrypt the bucket or volume)`. Operator INFO `PITR archive of an encrypted group has no client-side encryption` with `profile`.

### Cause
By error text:
- **`the keyring does not hold the binary log encryption key the file names`**: on an unsealed site (first start, a clone, a keyring rotation) mysqld has not written the key to the keyring file yet; it passes within a few scans. If it stays, the file was encrypted under another keyring: a binlog file copied onto the volume from another server, or a volume paired with the wrong keyring.
- **`the sidecar has no keyring file to decrypt it with`**: the sidecar was started without `BLOODRAVEN_KEYRING_FILE`, which the operator renders only with `spec.encryptionAtRest`, yet the server wrote encrypted binlog files. `binlog_encryption` was turned on outside the operator.
- **`encrypted binlog header version N is not supported`** and the other `encrypted binlog header:` texts: the MySQL image writes an encrypted binlog file format this sidecar does not know. The reader knows version 1 (MySQL 8.0.14 through 9.7.4) and refuses anything else.
- **`does not decrypt to a binlog`**: the key the header names does not decrypt the file. A header read while `ALTER INSTANCE ROTATE BINLOG MASTER KEY` rewrote it passes on the next scan; a lasting error means the file or the keyring file is damaged.
- **`read the keyring file` / `not the JSON component_keyring_file writes`**: the keyring file is unreadable or not in the expected format. Check the site's keyring phase and the keyring playbooks first.
- **`invalid binlog magic`**: the pod runs a sidecar from before encrypted binlogs were archived (the pod has not rolled since the operator upgrade, or `spec.sidecar.image` pins an older image). That sidecar does not decrypt anything.
- **`PITRArchiveNotEncrypted`**: not a fault. The archive holds the plaintext of the binary logs, and the PITR profile has no `encryption`. The operator cannot see whether the store encrypts, so it reports the combination and does not refuse it.

A group that ran with encryption at rest and PITR before the release that archives encrypted binlogs archived nothing until the upgrade. After it, the sidecar archives the encrypted binlogs still on the data volume; the ones MySQL had purged (`binlog-expire-logs-seconds`, 14 days unless overridden) are a gap, and promotion records from before have no promotion set. A point-in-time restore that needs a purged binlog refuses `PITRArchiveGap`.

### Diagnostic steps
1. Confirm both features and the binary log setting:
   ```bash
   kubectl get mfg <group> -n <ns> -o jsonpath='{.spec.encryptionAtRest.enabled} {.spec.encryptionAtRest.coverage.binaryLog} {.spec.backup.pitr.enabled} {.spec.backup.pitr.profileName}{"\n"}'
   kubectl get mfg <group> -n <ns> -o jsonpath='{range .status.encryptionAtRest.sites[*]}{.name} phase={.phase} binlogEncrypted={.coverage.binlogEncrypted}{"\n"}{end}'
   ```
2. Read the archiver: `scripts/sidecar-probe.sh -n <ns> <group> archiver`. Take the exact `lastError` (writable site) or `tail.reason` (read-only site) and match it against the list above. Any other `lastError` (storage, credentials, manifest) is playbook 31.
3. For `invalid binlog magic`, compare the sidecar image of the pod with the one the operator renders:
   ```bash
   kubectl get pod -n <ns> <pod> -o jsonpath='{range .spec.containers[*]}{.name}={.image}{"\n"}{end}'
   kubectl get mfg <group> -n <ns> -o jsonpath='{.spec.sidecar.image}{"\n"}'
   ```
4. For a missing key, check whether the site is unsealed (`phase` in step 1) and whether the error is still there a minute later. Then compare the key the error names with the server's identity (the key id holds the `server_uuid` of the server that wrote the file):
   ```bash
   kubectl exec -n <ns> <pod> -c mysql -- mysql -N -e "SELECT @@server_uuid, @@GLOBAL.binlog_encryption; SHOW BINARY LOGS"
   ```
5. For the `PITRArchiveNotEncrypted` report, read the profile:
   ```bash
   kubectl get mfg <group> -n <ns> -o jsonpath='{range .spec.backup.profiles[*]}{.name} encryption={.encryption}{"\n"}{end}'
   kubectl get mfg <group> -n <ns> -o jsonpath='{.status.conditions[?(@.type=="EncryptionAtRestReady")].message}{"\n"}'
   ```

### Safe remediation
- `invalid binlog magic`: let the pod roll onto the current sidecar image (finish the ordered update; remove or raise a `spec.sidecar.image` pin). The new sidecar archives the encrypted binlogs still on the volume.
- A missing key on an unsealed site: wait. One that stays on a sealed site, or that names another server's `server_uuid`: the file does not belong to this keyring and cannot be archived. Do not delete it by hand; it stops blocking once MySQL purges it. Take a full backup on the primary so later targets do not depend on it.
- An unsupported header version or field: report it with the MySQL image tag and the error text. Until a sidecar that reads the format is released, `spec.encryptionAtRest.coverage.binaryLog: false` makes MySQL write plaintext binary and relay logs (tablespaces, redo and undo logs stay encrypted). It rolls every site and narrows the encryption claim, so it is the owner's decision; the unreadable files written before still fail until MySQL purges them.
- No keyring file: find out how `binlog_encryption` came to be on (`SELECT * FROM performance_schema.persisted_variables`). The supported way to encrypt the binary log is `spec.encryptionAtRest`.
- After any of these, and after the upgrade of a group that ran with both features before: wait for `PITRArchiveReady=True`, then take a full backup. Point-in-time restores are possible from that backup on; windows that cross binlogs that were never archived stay unrestorable.
- `PITRArchiveNotEncrypted`: if the store encrypts the archive (bucket encryption, an encrypted volume) and the owner relies on that, nothing needs to change; the message stays. To encrypt client-side, `encryption` cannot be added to a profile that already archives: admission rejects the edit, and an edit it cannot see is refused at reconcile (`PITRArchiveBindingReady=False`/`EncryptionChangeRefused`, Warning `PITRArchiveEncryptionChangeRefused`). Follow the rotation procedure of the backup encryption docs: create a passphrase Secret, add a new profile with a new prefix (or sub-path) and `encryption`, switch `spec.backup.pitr.profileName` to it (the sites roll once), wait for `PITRArchiveBindingReady=True`, then take a full backup. Keep the old prefix for as long as restores from the old history are needed; it remains unencrypted.

### What NOT to do
- Do not copy binlog files into the archive by hand or edit the manifest: restores verify every object against its manifest row, and `mysqlbinlog` cannot read an encrypted file (`Reading encrypted log files directly is not supported`).
- Do not run `SET GLOBAL binlog_encryption=OFF` or `SET PERSIST` it: the operator owns the setting, the launcher reads a persisted value on a clone's restart, and the next pod restart restores the `my.cnf` value.
- Do not run `ALTER INSTANCE ROTATE BINLOG MASTER KEY` to clear an error. The operator never issues it and a sealed site refuses it; it does not change what the sidecar can read.
- Do not edit, replace or copy the keyring file, and do not restart sidecars or change the archive prefix to clear a decryption error.
- Do not add `encryption` to the bound profile in place or edit its Secret: the change is refused and the condition stays `False` until it is reverted.
- Do not try to disable `spec.encryptionAtRest`: admission refuses turning it off.

## 50. Pods did not roll after an operator upgrade, or the paced rollout is halted

### Symptoms
- After an operator upgrade some or all groups still run the previous release's pods (no launcher `command` on the mysql container, the old sidecar image), and no ordered update started for them.
- A spec change that should restart a group's pods (`spec.image`, MySQL configuration) is accepted and nothing rolls.
- `status.rollout.phase` is `Pending`, `Stalled`, or absent on every group; alerts `BloodravenRolloutStalled`, `BloodravenRolloutPendingTooLong` or `BloodravenRolloutRecordNotStored`; Events `RolloutStalled`, `RolloutDeferred`, `RolloutRecordNotStored`.

### Cause
The operator rolls a pod change that its own release causes (a new launcher, a pod render change, a new default sidecar image) one failover group at a time. Each site Deployment and Dragonfly StatefulSet carries `bloodraven.shipstream.io/rendered-by`, the release that last wrote its pod template. A workload another release wrote, whose template the running operator would change, is written only once the group is admitted in `status.rollout`. The drift check reads the same record, so an unadmitted group starts no ordered update. Consequences that look like faults and are not:
- Waiting groups keep their old pods. That is the design: one group holds the slot at a time, in namespace and name order, with a soak (`rollout.soak`, 1m) between groups.
- While a group waits, spec changes that restart its pods wait with it, together with the site ConfigMaps.
- A group that is not stable (not `Ready`, a restore, a planned failover, an ordered update, `AutomaticFailoverHeld`) is passed over (`Pending` / `GroupNotStable`) and is never admitted on its own while it stays unstable.
- A group whose roll started and cannot finish keeps the slot (`Stalled`). Every other group reports `Pending` / `RolloutHalted`. A bad release stops at that first group.
- With a `MysqlFailoverGroup` CRD older than the operator the API server drops `status.rollout`, no admission can be stored, and no group is rolled at all.

Not held by the pacer: a change that comes from the group itself on a workload the running release already wrote, a new site's Deployment, and the restart of a clone recipient under encryption at rest (`unsealReason: Clone`).

### Diagnostic steps
1. Read every group's record:
   ```bash
   kubectl get mysqlfailovergroup -A -o jsonpath='{range .items[*]}{.metadata.namespace}/{.metadata.name}{"\t"}{.status.rollout.phase}{"\t"}{.status.rollout.reason}{"\t"}{.status.rollout.message}{"\n"}{end}'
   ```
   `scripts/triage.sh` prints the same record for one group as `Paced rollout:`, and `kubectl bloodraven status <group>` as `Rollout:` while it is not `Complete`.
2. No group has a record although pods should roll: check the CRD and the operator.
   ```bash
   kubectl get crd mysqlfailovergroups.shipstream.io -o yaml | grep -c 'rollout:'      # 0 = CRD older than the operator
   kubectl get events -A --field-selector reason=RolloutRecordNotStored
   kubectl logs -n <operator-ns> deploy/bloodraven | jq -c 'select(.msg | startswith("paced rollout"))'
   ```
   `paced rollouts are disabled (BLOODRAVEN_ROLLOUT_PACING)…` at operator start means `rollout.pacing=false`: nothing is paced and `status.rollout` is not written. No `paced rollout: pacer started` line means this pod is not the leader or the operator predates pacing.
3. One group is `Stalled`: that group is the problem, not the waiting ones. `reason: OrderedUpdateStalled` copies the `Updating` condition's message (site, pod, cause): continue with [playbook 34](#34-ordered-update-stalled-on-a-pod-that-is-not-ready-orderedupdatestalled). `reason: StallTimeout` lists what is outstanding after `rollout.stallAfter` (30m): workloads not rolled yet, rollouts not finished, or why the group is not stable (a handoff that keeps being refused: playbooks 23 and 33).
4. Groups are `Pending` and none is `Rolling` or `Stalled`: read each reason. `GroupNotStable` names the unstable reason in the message; `NotStarted` follows a `RolloutDeferred` Event whose message says what held the group after it was admitted.
5. Compare a workload's renderer with the record when in doubt:
   ```bash
   kubectl get deploy -n <ns> -l bloodraven.shipstream.io/failover-group=<group> -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.metadata.annotations.bloodraven\.shipstream\.io/rendered-by}{"\n"}{end}'
   kubectl get mysqlfailovergroup <group> -n <ns> -o jsonpath='{.status.rollout.renderer}{"\n"}'
   ```
   A Deployment whose annotation equals the record's `renderer` was written by the running release.

### Safe remediation
- **CRD older than the operator**: apply the CRDs of the release the operator runs with server-side apply. The rollout starts within seconds. Never apply v1.7.0 or later CRDs to a cluster that still runs v1.6.0.
- **Stalled, cause in the group** (a custom `spec.image` without `/bin/bash` or the entrypoint, an unschedulable pod, a divergent standby): fix the group. It is admitted, so its spec changes are not held. When its roll completes the next group is admitted after the soak.
- **Stalled, cause in the release**: roll the operator back to the release that ran before. Waiting groups then have nothing to roll; the groups already rolled are rolled back one at a time, the stalled one first. A rollback to v1.7.0, which has no pacing, rolls them back at the same time.
- **Stalled, cause in the group, no fix soon**: `kubectl annotate mysqlfailovergroup <group> -n <ns> bloodraven.shipstream.io/rollout-release-slot=true` lets the next group be admitted. The group's own roll continues and its record stays `Stalled`. Remove the annotation when it has completed.
- **One waiting group must roll now** (a spec change waits, or the group is never stable): check the group first, then `kubectl annotate mysqlfailovergroup <group> -n <ns> bloodraven.shipstream.io/rollout-admit=true`. This admits it outside the queue, past a halt and without the stability check. Remove the annotation when the roll has completed: while it is set every later release rolls this group without waiting, and the operator reads the key, not the value.
- **Every group must roll now**: `helm upgrade … --reuse-values --set rollout.pacing=false` rolls every group with a pending change at once. State the blast radius before proposing it: an ordered update with a handoff in every `OrderedUpdate` group at the same time, every site of every `Recreate` group restarted together.

Do not:
- Edit `status.rollout` or the `rendered-by` annotation by hand, or patch a site Deployment to "push" the new template: the operator reverts workloads and decides on the record.
- Delete pods to make them pick up the release: a pod is recreated from the Deployment's template, which has not changed.
- Use `rollout-release-slot` or `rollout.pacing=false` while it is unknown why the first group stalled. If the release is the cause, the next groups fail the same way.
- Switch a group to `Recreate` to get it past a stall: every site then restarts together.
