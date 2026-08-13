# Losing a whole cluster, and the go-live gate

**Unit:** 6 — Backups, disaster recovery, and going live
**Objectives (unit-numbered):**
13. Fence a lost source on two independent signals before bootstrapping a DR group   [obj 13]
14. Bootstrap a disaster-recovery group for `playground` from object storage and cut DNS over to it   [obj 14]
15. Walk a production hardening checklist and say which items you would block a launch on   [obj 15]

## Topic generation prompt

This topic synthesises. Introduce no new mechanisms beyond the DR surface itself; everything else is assembled from what the course already established.

Start with the scenario the failover group cannot answer: the whole cluster hosting `playground` is gone or unreachable, and you have a bucket in another region. Lead with the fencing decision, not the restore, because that is where the danger is. The source-fencing checklist requires at least **two of three** independent signals before you declare the source dead — and state plainly why the bar is that high: Bloodraven v1 does not automatically detect or resolve cross-cluster split brain. There is no operator watching both clusters. The human fencing decision **is** the safety mechanism. Connect this back to Unit 5's split-brain material and to GitHub's 2018 incident: two sides each holding writes the other never saw, and a recovery measured in days.

Then set expectations on `MysqlStandbyCluster` honestly. Today it is **observability only**: it publishes `BucketReadable` and `SourceConfigKnown` conditions and a discovered dump/GTID/binlog window. No MySQL contact, no restore jobs, no activation. It tells you whether a DR bootstrap would be possible and roughly how far back it could reach. It does not perform one. So the actual DR bootstrap is the tooling the learner already has: a new failover group in the DR cluster using `spec.initFromBackup` from topic 2, pointed at the same bucket, then DNS cut over by the same `DNSEndpoint` path from Unit 4 — and the same caveat, that the operator cannot accelerate DNS propagation.

Then the day-2 surface: `kubectl bloodraven` has exactly `status`, `promote`, `reclone`, `backup`, `verify-backup`, `version` and `help`. Teach the design property that makes it safe to hand to on-call: the plugin only writes resources the operator already reads, and never talks to MySQL directly. There is no back door in it, which is why `promote` behaves identically to the annotation path and obeys every gate the learner already knows.

Then the go-live gate, assembled from the whole course. Frame it as items you would block a launch on versus items you would accept with a written owner. Draw on: `sync_binlog=1` is an overridable default written before `spec.mysqlConf`, not a guarantee — confirm what your group actually runs; `maxLagSeconds` (default 300) drives only the `ReplicationLagging` condition and is **not** a promotion gate, so a beyond-threshold replica is still promoted; the playground's overrides (`failoverCooldown: 30s` vs 5m, `maxLagSeconds: 30` vs 300, `dns.ttl: 10` vs 60) are not the shipped defaults, so no timing you measured in the playground transfers unchanged; PVC-local backups are not durable; an unverified backup is an assumption; readers can neither be promoted nor source a backup; and the application-side gap from topic 4 has no shipped alert, so somebody must own it by name. Make the learner commit to a verdict on each item rather than nodding along.

Close the course on the running example. `playground` began as three sites and a counter application. It is now a group whose failure modes the learner can enumerate, whose alerts do not lie, whose backups have been restored at least once, and which could be handed to an on-call rotation tonight — with an honest statement of what it will and will not do. That statement is the deliverable of the whole course.

Do NOT introduce new mechanisms, new metrics, or new CRD fields beyond `MysqlStandbyCluster` and the plugin surface named here.

## Requested activities

- READ: 1000-1200 words. The lost-cluster scenario, the two-of-three fencing checklist and why the human decision is the safety mechanism, `MysqlStandbyCluster` as observability only with its two conditions, the DR bootstrap via `spec.initFromBackup` plus the DNS cutover and its propagation caveat, the seven `kubectl bloodraven` subcommands and the no-back-door property, then the go-live gate as block-or-accept items. End on `playground` and what the learner can now say about it. One `terminal` widget on `kubectl bloodraven status` for a DR-bootstrapped group; at most one other widget.
- FLASHCARDS: two-of-three fencing signals, no automatic cross-cluster split-brain detection, `BucketReadable` and `SourceConfigKnown`, standby cluster does not activate, the seven subcommands, the plugin never talks to MySQL, `sync_binlog` is overridable, `maxLagSeconds` is not a promotion gate, playground overrides are not defaults. 10-12 cards.
- QUIZ: 5 questions. How many independent signals before declaring a source dead and why; what a `BucketReadable=True` standby cluster has actually done for you; which field bootstraps the DR group; which `kubectl bloodraven` subcommand does not exist among a plausible list; and which of four checklist items should block a launch.

## Handoff

**Inherits:** The learner has backups, verification, restore, encryption and a working alert set for `playground`.
**Leaves:** The course ends here. The learner can operate a Bloodraven failover group in production: run it, break it, fail it over, back it up, restore it, alert on it, recover it in another cluster, and state precisely what it guarantees and what it does not.
**Do not cover:** Nothing follows — do not hand off to a further topic or unit.
