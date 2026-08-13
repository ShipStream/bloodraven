# Alerts, runbooks, and the 3am path

**Unit:** 6 — Backups, disaster recovery, and going live
**Objectives (unit-numbered):**
10. Build the minimum alert set for `playground` from real metric names and say what each one means at 3am   [obj 10]
11. Map an alert to its runbook and a first command without reading the whole docs site   [obj 11]
12. Write an alert that ignores reader lag on purpose   [obj 12]

## Topic generation prompt

Build the alerting layer for `playground`. Teach **only** metric names that exist in the shipped operator: `bloodraven_site_state`, `bloodraven_replication_lag_seconds`, `bloodraven_replication_running`, `bloodraven_failovers_total`, `bloodraven_divergent_transactions`, `bloodraven_split_brain_auto_resolve_total`, `bloodraven_primary_reassert_total`, `bloodraven_poll_latency_seconds`, `bloodraven_archiver_backlog_files`, `bloodraven_backup_last_success_timestamp_seconds`, `bloodraven_keyring_phase`, `bloodraven_dns_flips_total`, `bloodraven_state_transitions_total`. Do **not** mention `bloodraven_dr_restorable_timestamp_seconds` under any circumstances — it does not exist in the shipped operator and is unimplemented Phase 2 work; an alert written against it is a rule that can never fire.

Organise the minimum set by what it means when it wakes you: `BloodravenOperatorDown` (nobody is deciding anything; the data plane is still serving), `BloodravenNoPrimary` and `BloodravenNoWritableSite` (writes are down now), split-brain-equivalent alerts built on `bloodraven_split_brain_auto_resolve_total` (two sides took writes; someone's writes are being discarded), `BloodravenReplicationLagging` and `BloodravenReplicationDown` (your RPO is drifting), `BloodravenDivergentTransactions` (a site needs a reclone decision from a human), `BloodravenBackupStale` and `BloodravenBackupVerificationStale` (your recovery story is aging out), `BloodravenPITRArchiveLagging` and `BloodravenPITRUploadFailures` off `bloodraven_archiver_backlog_files` (the silent degradation from topic 1 — MySQL is fine, your RPO is not), `BloodravenKeyringNotSealed` off `bloodraven_keyring_phase`, `BloodravenHighPollLatency` off `bloodraven_poll_latency_seconds` (detection itself is slowing down), and `BloodravenFailoverOccurred` off `bloodraven_failovers_total` as informational.

The centrepiece is the discrimination that separates a good alert set from a noisy one: **reader lag must not page.** Chaos scenario 42 soaks a reader past three times `maxLagSeconds` and asserts that the group stays `Ready`, no failover fires, no cooldown is consumed, and only the reader endpoint sheds. That is the designed behaviour from Unit 1's role model, not a fault. So an alert on `bloodraven_replication_lag_seconds` without a site-label exclusion for `read-only` sites pages you, at 3am, for a non-event that the system already handled by removing the reader from `mysql-playground-replicas`. Make the learner write the excluded form explicitly and use an `anatomy` widget to break a PromQL expression into metric, label matcher, exclusion, comparison and `for:` duration.

Teach two traps directly. First, a `Failover` condition reason does **not** exist — the code emits `Reason="Degraded"` for the failover row of the decision matrix, so any alert or automation matching on a reason string of `Failover` will never fire. Point back at the five real reasons from Unit 1 and Unit 2. Second, the honest gap: **no shipped alert fires for "the application is still broken after a successful failover".** `BloodravenFailoverOccurred` is informational and operator-scoped — it tells you the operator finished, not that traffic recovered — and the docs explicitly ask you to alert on repeated read-only errors from your own application. Tie that straight back to Unit 4: the pool that kept a demoted primary, the driver that needs `rejectReadOnly`, the JVM DNS cache. Bloodraven cannot see any of it, so that alert has to be yours.

Then objective 11: a one-page alert → runbook → first command map. Every alert in the set gets a runbook anchor and a single first command, so the on-call engineer types something in the first thirty seconds instead of opening a docs site. Use `kubectl bloodraven status` as the default first command and name the specific ones where it differs.

Do NOT cover cross-cluster DR, `MysqlStandbyCluster`, or the go-live checklist — topic 5 owns all three.

## Requested activities

- READ: 1000-1200 words. The minimum alert set grouped by 3am meaning with the real metric names, the reader-lag discrimination with scenario 42's assertions and the excluded PromQL, the non-existent `Failover` reason trap, the application-side gap and who owns it, and the alert-to-runbook-to-first-command map. One `anatomy` widget on the reader-excluding PromQL expression; at most one other widget.
- FLASHCARDS: each real metric name and what it means at 3am, the reader-lag exclusion, `Reason="Degraded"` not `Failover`, `BloodravenFailoverOccurred` is informational, the application-side gap is yours to alert on. 12-14 cards.
- QUIZ: 5 questions. Which alert fires when the operator is gone but MySQL is fine; which single alert would page for a soaked reader if written carelessly, and how to fix the expression; why a rule matching reason `Failover` never fires; what `bloodraven_archiver_backlog_files` climbing means for the data plane; and what Bloodraven will never tell you about your application after a successful failover.

## Handoff

**Inherits:** The learner can back up, verify, restore, and encrypt `playground`.
**Leaves:** The learner has a minimum alert set built from real metrics, a runbook map, and an alert that deliberately ignores reader lag.
**Do not cover:** Cross-cluster DR, `MysqlStandbyCluster`, the kubectl plugin surface, the go-live checklist (topic 5).
