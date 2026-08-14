# Alerts, runbooks, and the 3am path

`playground` can now be backed up, verified, restored and encrypted. Every one of those can rot silently, and nothing tells you. The operator has been exporting Prometheus metrics on port `8080` this whole time and you have not written a rule against them. Write the minimum set — and decide which of them may wake you.

## Sort by 3am meaning, not by metric

A metric-shaped alert set gives you fifteen pages that all say "something about MySQL". A useful set falls into four bands, and the band sets the severity.

| Band | Alerts | What it means when it wakes you |
| --- | --- | --- |
| Nobody is deciding anything | `BloodravenOperatorDown` | The data plane is **still serving**. Primary and replica serve reads and writes with zero operator involvement, and sidecar fencing holds correctness however long the operator is gone. You have lost failover cover, not writes. |
| Writes are down now | `BloodravenNoWritableSite`, `BloodravenNoPrimary`, both off `bloodraven_site_state` | Page. `NoWritableSite` may be a failover in flight — those finish in about 12.0 s. `NoPrimary` with zero unreachable sites will *never* self-heal: the matrix refuses to auto-elect from an all-read-only topology, by design. |
| Someone's writes are being discarded | `BloodravenSplitBrainResolved` off `bloodraven_split_brain_auto_resolve_total`; `BloodravenSplitBrainDetected` off `bloodraven_site_state` | Two sides took writes. Priority resolution loses the loser's unreplicated writes and surfaces the loss loudly rather than preventing it. Page, then go count what went. |
| Your RPO is drifting | `BloodravenReplicationLagging`, `BloodravenReplicationDown`, `BloodravenDivergentTransactions`, `BloodravenPITRArchiveLagging`, `BloodravenPITRUploadFailures`, `BloodravenBackupStale`, `BloodravenBackupVerificationStale`, `BloodravenKeyringNotSealed`, `BloodravenHighPollLatency` | Nothing is broken *now*, but your recovery story is degrading while `playground` looks perfectly healthy. The silent degradation from topic 1, and the band people forget to build. |

Two of those need naming. `bloodraven_divergent_transactions{site} > 0` means a site sits `RecoveryBlocked` waiting for a human reclone decision — it will wait forever. `bloodraven_primary_reassert_total` climbing means something keeps fencing your promoted primary; check sidecar connectivity to the operator's auxiliary Service. `bloodraven_state_transitions_total{site,from,to}` is forensics, never a page.

`BloodravenFailoverOccurred` off `bloodraven_failovers_total` sits outside all four bands. It is informational, and it is the most dangerous alert in the set.

## The alert that must not page: reader lag

`playground` has three sites — `iad` and `pdx` as `primary-candidate`, `reader` as `read-only`. Chaos scenario 42 applies `SOURCE_DELAY` to `reader` and soaks the group for three times `maxLagSeconds` — 3 × 30 s = 90 s in the playground config — while writing a row a second to the primary. Both replication threads keep running against the correct source, so the reader is converged-but-slow, not stopped.

The scenario asserts on every iteration of that soak: `Ready` stays `True`, `activeSite` never changes, `lastFailover` is unchanged so no anti-flap cooldown is consumed, and `reader` never enters `SourceConvergenceState=Blocked` or `RecoveryBlocked`. The only reaction anywhere is that `reader` leaves the endpoints of `mysql-playground-replicas` once its lag passes `readOnlyMaxLagSeconds`. That is Unit 1's role model working as designed, not a fault.

So a naive lag rule pages you at 3am for an event the system already handled by shedding the reader. The fix is one label matcher, with a catch: `bloodraven_replication_lag_seconds` carries a **`site` label only**. There is no `role` label to exclude on, so the exclusion is by site name and you maintain it by hand every time you add a reader.

```widget
{"type":"anatomy","title":"A lag alert that ignores the reader on purpose","parts":[
 {"text":"bloodraven_replication_lag_seconds","label":"metric","note":"Gauge, one series per site. Reads -1, not a large number, when lag is NULL because the site is not replicating at all."},
 {"text":"{site","label":"label matcher","note":"The only dimension this gauge has. No role label and no group label — two failover groups under one operator share the series namespace here."},
 {"text":"!=\"reader\"}","label":"exclusion","note":"Scenario 42's entire 90 s soak lives inside this matcher. Without it, a deliberately-shed reader pages you."},
 {"text":" > 30","label":"comparison","note":"The number tracks the group's spec, never the CRD default. playground sets replication.maxLagSeconds: 30; a group that leaves the field alone gets 300, and its rule needs 300. Copying this file between groups without re-reading the spec is how a lag alert goes quiet."},
 {"text":"for: 5m","label":"duration","note":"One failoverCooldown default. Long enough that a promotion's own catch-up window never trips it."}]}
```

Note what the exclusion does not buy you. `> 30` never fires when the gauge reads `-1`, so a replica that has stopped replicating entirely is invisible to a positive-threshold lag rule. That is why `BloodravenReplicationDown` on `bloodraven_replication_running{site,thread}` is a separate rule, not a nicety.

## The rules

Excerpted; the bands above name every alert and the map below carries the rest.

```yaml
groups:
- name: bloodraven-playground
  rules:
  - alert: BloodravenNoWritableSite
    expr: count(bloodraven_site_state{state="writable",site!="reader"} == 1) == 0
    for: 30s                       # 5 x the 6 s detection window (2 s poll x 3 failures)

  - alert: BloodravenNoPrimary     # will not self-heal; the matrix refuses to elect
    expr: |
      count(bloodraven_site_state{state="read-only",site!="reader"} == 1) == 2
      and count(bloodraven_site_state{state="unreachable",site!="reader"} == 1) == 0
    for: 30s

  - alert: BloodravenReplicationLagging
    expr: bloodraven_replication_lag_seconds{site!="reader"} > 30     # <-- the exclusion
    for: 5m

  - alert: BloodravenPITRArchiveLagging          # backlog never drained
    expr: min_over_time(bloodraven_archiver_backlog_files[5m]) > 0

  - alert: BloodravenPITRUploadFailures          # backlog still growing
    expr: bloodraven_archiver_backlog_files - (bloodraven_archiver_backlog_files offset 5m) > 0
    for: 5m

  - alert: BloodravenBackupStale                 # 86400 = your SLO, not a default
    expr: time() - bloodraven_backup_last_success_timestamp_seconds{group="playground"} > 86400

  - alert: BloodravenKeyringNotSealed
    expr: bloodraven_keyring_phase{failover_group="playground",phase="sealed"} == 0
    for: 5m

  - alert: BloodravenHighPollLatency             # detection itself is slowing down
    expr: histogram_quantile(0.99, sum by (le, site) (rate(bloodraven_poll_latency_seconds_bucket[5m]))) > 2
    for: 5m
  # ... OperatorDown, SplitBrain*, ReplicationDown, DivergentTransactions,
  #     BackupVerificationStale, FailoverOccurred elided — see the map ...
```

`> 2` on poll latency is the `pollInterval` default: once p99 poll latency reaches the poll interval, the 6 s detection budget stops being real and the per-site 5 s probe ceiling is all that holds the loop together. This is the rule that would have caught issue #93, where a poll parked on a blackholed socket let the operator report `activeSite=iad, state=writable, Ready=True` for two minutes under a deny-all NetworkPolicy.

`86400` is the number this course uses, and it is **yours**, not Bloodraven's — a 24-hour SLO for a
nightly profile. The number Bloodraven does supply is the *ceiling* you must stay under:
`binlog-expire-logs-seconds` is 1209600 s, 14 days. Past that, PITR has no binlog material left to
bridge your last backup to now, so a backup older than the ceiling cannot be replayed forward at all.
Pick a threshold that is a small multiple of your backup cadence and is nowhere near 14 days.
Bloodraven cannot pick it for you; it can only tell you where the cliff is.

Label sets are not uniform, which bites when you templatise. Site metrics carry `{site}` and nothing else. The archiver carries `{namespace, group, site}`, backup metrics `{group, profile}`, and `bloodraven_keyring_phase` `{mysql_namespace, failover_group, site, phase}` — different words for the same two concepts. Read the label set before you copy a selector.

## Two traps

**A `Failover` condition reason does not exist.** The failover row of the decision matrix sets `Reason = "Degraded"`. The reason is one of exactly five strings: `Healthy`, `Degraded`, `SplitBrain`, `NoPrimary`, `TotalLoss`. Any rule or automation matching a reason of `Failover` sits green forever through every promotion you will ever do. Use `bloodraven_failovers_total` for "a failover happened" and the five real reasons from Units 1 and 2 for condition state.

**No shipped alert fires for "the application is still broken after a successful failover."** `BloodravenFailoverOccurred` is informational and operator-scoped: it says the operator finished, not that traffic recovered. Everything from Unit 4 lives in that gap — the pool still handing out a connection to the demoted primary, the driver that needs `rejectReadOnly`, the JVM DNS cache that ignores your 60 s TTL. Bloodraven cannot see any of it, and the docs are explicit that you must alert on application write failures, pool exhaustion and repeated read-only errors yourself. That alert is yours, and it is the only one measuring your actual outage.

One pairing helps: a failover that increments `bloodraven_failovers_total` with no matching `bloodraven_dns_flips_total` increment means the CR promoted correctly while external-dns never moved the record. The operator cannot accelerate DNS propagation, and a stuck external-dns is a write outage after the operator has "finished".

## The map: alert → runbook → first command

On-call types something in the first thirty seconds instead of opening a docs site. The default first command is `kubectl bloodraven status`; the rows below deviate, each for a reason.

| Alert | Runbook | First command |
| --- | --- | --- |
| `BloodravenOperatorDown` | Operator unavailable | `kubectl -n bloodraven get deploy,lease` — the CR is stale by definition |
| `BloodravenNoWritableSite` | Emergency manual promotion / Total site loss | default |
| `BloodravenNoPrimary` | Emergency manual promotion | default |
| `BloodravenSplitBrainDetected` / `…Resolved` | Split-brain recovery | default |
| `BloodravenReplicationLagging` / `…Down` | Replication lag high | default |
| `BloodravenDivergentTransactions` | Divergent old primary recovery | default — `status.sites[].divergentGtid` is the reclone token |
| `BloodravenPITRArchiveLagging` / `…UploadFailures` | Backup and restore | `kubectl -n <ns> logs deploy/mysql-playground-<site> -c sidecar` — archiver, not operator |
| `BloodravenBackupStale` | Failed backup | `kubectl -n <ns> get mysqlbackup` |
| `BloodravenBackupVerificationStale` | Backup verification | `kubectl -n <ns> get mysqlbackupverification` |
| `BloodravenKeyringNotSealed` | Keyring not sealed | `kubectl get mfg playground -o jsonpath='{.status.encryptionAtRest.sites}'` |
| `BloodravenHighPollLatency` | Network partition diagnosis | `kubectl -n bloodraven logs deploy/bloodraven` |
| `BloodravenFailoverOccurred` | Failover | `kubectl get dnsendpoint bloodraven-playground -o yaml` — promotion succeeded; DNS is what is left |

Give every rule a `runbook_url` annotation pointing at its row. An alert without one is a page with no next step.

`playground` now has a minimum alert set built from metrics that exist, a runbook map that fits on one screen, and a lag rule that deliberately ignores the reader scenario 42 soaks. None of it covers the whole cluster being gone rather than one site of it. That is the next topic.
