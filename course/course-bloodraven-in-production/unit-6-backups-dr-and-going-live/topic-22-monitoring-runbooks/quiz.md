# Quiz — Alerts, runbooks, and the 3am path

<!-- Rendered from course.json by course-template/tools/render-views.mjs.
     Edit course.json, then re-render. Edits here are overwritten. -->

## Question 1

**Type:** MULTIPLE_CHOICE

It is 3am and one Bloodraven alert has fired for `orders`. The counter application has not missed a write, both `iad` and `pdx` are serving, and replication lag is flat. Which alert is it?

- `BloodravenOperatorDown`
- `BloodravenNoWritableSite`
- `BloodravenNoPrimary`
- `BloodravenReplicationDown`

**Correct option index:** 0

**Explanation:**

The operator is on the failure-detection and promotion path, not the request path — a healthy primary and replica keep serving with zero operator involvement, and sidecar fencing preserves correctness however long it is gone. So the only alert compatible with 'writes are fine' is the one saying nobody is deciding anything; what you have lost is failover cover, which is why it is a ticket that escalates rather than an instant page. `BloodravenNoWritableSite` and `BloodravenNoPrimary` are both built on `bloodraven_site_state` and both mean writes are down now, which contradicts the counter app still writing. `BloodravenReplicationDown` fires off `bloodraven_replication_running == 0`, which contradicts flat lag on a running replica. (objective 10)

## Question 2

**Type:** SHORT_ANSWER

Chaos scenario 42 applies `SOURCE_DELAY` to the `read-only` site `reader` and soaks `orders` for three times `maxLagSeconds` while the primary keeps taking writes. Which single alert in a carelessly written set would page you, and exactly what do you change to fix it?

**Sample answer:**

`BloodravenReplicationLagging`, written as `bloodraven_replication_lag_seconds > 30`. Add a site-label exclusion for the reader: `bloodraven_replication_lag_seconds{site!="reader"} > 30`. Nothing else in the set moves — scenario 42 asserts the group stays Ready, activeSite never changes, no failover is recorded and no anti-flap cooldown is consumed. The only reaction is that `reader` leaves the endpoints of `mysql-orders-replicas` once its lag passes `readOnlyMaxLagSeconds`, which is the designed behaviour of the read-only role, not a fault. The exclusion has to name the site because the gauge carries a `site` label only — there is no `role` label to filter on.

**A full-credit answer shows:**

A strong answer names `BloodravenReplicationLagging` (or an equivalent rule on `bloodraven_replication_lag_seconds`), writes the corrected expression with a `site!="reader"` matcher, and justifies the exclusion by scenario 42's assertions — group stays Ready, no failover, no cooldown consumed, only the reader endpoint sheds. Credit the observation that the exclusion must be by site name because the metric has no `role` label. Do not credit answers that propose raising the threshold, lengthening `for:`, or silencing the alert in the alertmanager route: those hide a real replica falling behind as well as the reader.

**Explanation:**

The reader shedding its endpoint is the read-only role working as designed, so an alert on it is a page for a non-event the system already handled. Raising the threshold or extending `for:` would also mute a genuinely lagging `pdx`; the exclusion is surgical because it removes exactly the one site whose lag has no bearing on RPO. (objectives 10, 12)

## Question 3

**Type:** MULTIPLE_CHOICE

You wrote automation that watches the failover group's condition and acts when the reason equals `Failover`. It has never fired, through several confirmed promotions. Why?

- The condition reason is only written on state transitions, and a 12 s promotion is too fast for a scrape to observe
- The failover row of the decision matrix sets `Reason = "Degraded"`; `Failover` is not one of the five reasons the code can emit
- Condition reasons are only populated when `spec.splitBrainPolicy.sitePriorities` is set, and yours is empty
- A promotion sets the reason to `SplitBrain` until the old primary rejoins as a replica

**Correct option index:** 1

**Explanation:**

The matrix emits exactly five reasons — `Healthy`, `Degraded`, `SplitBrain`, `NoPrimary`, `TotalLoss` — and the failover row is stamped `Degraded`, so a matcher on `Failover` sits green forever. Use `bloodraven_failovers_total` for 'a failover happened'. The scrape-timing option is tempting but wrong twice over: the matrix is evaluated every poll so status always carries the current topology, and the reason would still be the wrong string at any scrape interval. `sitePriorities` governs split-brain auto-resolution only; it does not gate whether conditions carry reasons. And `SplitBrain` is a distinct matrix row requiring more than one writable core site, which a completed promotion is not. (objective 10)

## Question 4

**Type:** MULTIPLE_CHOICE

`bloodraven_archiver_backlog_files{site="iad"}` has been non-zero at every scrape for five minutes. `orders` is Ready, the counter app is writing normally and replication lag is 0. What is actually degrading?

- Replication to `pdx` is behind by that many binlog files and RPO on a failover is growing
- The primary's binlog volume is filling and MySQL will stop accepting writes when it does
- Your point-in-time recovery window — sealed binlogs are not reaching object storage, so a restore can only reach the last archived file
- The last several backup verifications failed and the artifacts are unrestorable

**Correct option index:** 2

**Explanation:**

The gauge counts sealed binlogs the sidecar archiver has not yet uploaded. The data plane is untouched — MySQL serves, replication ships, the group is Ready — but PITR reaches only the last binlog event you actually archived, so every minute of backlog is recovery window you no longer have. This is the silent degradation pattern: nothing is broken now, your recovery story is. The replication option confuses two different streams: async replication to `pdx` is a MySQL-to-MySQL binlog stream and has nothing to do with uploads to object storage, and lag is 0 anyway. Disk pressure is a plausible downstream consequence of unarchived files piling up, but the gauge measures upload backlog, not free space. And backup verification has its own metric family and its own alert; a failed verification does not move this gauge. (objective 10)

## Question 5

**Type:** TRUE_FALSE

`BloodravenFailoverOccurred` fires and then resolves. That is sufficient evidence that your application is writing again.

**Correct answer:** false

**Explanation:**

The reversal: the alert is informational and operator-scoped — it says the operator finished a promotion, not that traffic recovered. Bloodraven cannot see your connection pool still holding a session to the demoted primary, your driver without `rejectReadOnly`, or a JVM DNS cache ignoring the record's TTL. It cannot even guarantee DNS moved: a promotion that increments `bloodraven_failovers_total` with no matching `bloodraven_dns_flips_total` increment is still a write outage. No shipped alert covers 'the application is broken after a successful failover' — that rule is yours to write, on repeated read-only errors and write failures from your own application. (objectives 10, 11)
