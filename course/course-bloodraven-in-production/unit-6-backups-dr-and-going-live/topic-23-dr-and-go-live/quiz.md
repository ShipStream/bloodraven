# Quiz — Losing a whole cluster, and the go-live gate

<!-- Rendered from course.json by course-template/tools/render-views.mjs.
     Edit course.json, then re-render. Edits here are overwritten. -->

## Question 1

**Type:** MULTIPLE_CHOICE

The cluster hosting `playground` has stopped answering: `kubectl --context=source get nodes` times out. That is your only signal so far. What does the source-fencing checklist say to do next?

- Bootstrap the DR group now — an unreachable API server means the MySQL workloads are gone with it
- Get a second independent signal before touching the DR cluster, because nothing in Bloodraven will catch a cross-cluster split brain if you are wrong
- Bootstrap the DR group but declare its sites `role: dr-only`, so the operator fences the source copy automatically
- Wait until the source operator raises a `SPLIT BRAIN` alert, then bootstrap the DR group

**Correct option index:** 1

**Explanation:**

Two of three signals is the bar because Bloodraven v1 does not automatically detect or resolve cross-cluster split brain — no operator watches both clusters, so your judgement is the only safety mechanism. Option 1 confuses the control plane with the data plane: Kubernetes will not even delete pods on an unreachable node, so the containers keep running and keep writing to the PV. Option 3 misapplies site roles: role-based fencing only acts on sites inside one failover group, and the DR group has no visibility of the source at all. Option 4 expects the decision matrix to work across clusters, but `SPLIT BRAIN` is raised only when more than one core site of a single group is writable — and in any case you cannot receive an alert from a cluster you cannot reach. (objective 13)

## Question 2

**Type:** TRUE_FALSE

A `MysqlStandbyCluster` in your DR cluster reports `BucketReadable=True` and `SourceConfigKnown=True`. This proves the source archive can be restored into the DR cluster.

**Correct answer:** false

**Explanation:**

The reversal: those two conditions prove only that the DR cluster could read the bucket and parse the dump metadata. `MysqlStandbyCluster` is observability only today — no MySQL contact, no restore Jobs, no activation. It tells you a DR bootstrap would be possible and roughly how far back the dump plus archived binlog window could reach; it has not loaded a byte of that dump into a mysqld. Proving restorability is a separate act, and the only thing that proves it is a verification that actually restored the artifact. (objective 14)

## Question 3

**Type:** MULTIPLE_CHOICE

The source cluster is fenced and you are standing up a fresh `MysqlFailoverGroup` named `playground` in the DR cluster, to be populated from the S3 prefix the dead cluster was writing to. Which field does that?

- `spec.restoreInPlace`, with an RFC 3339 confirm token naming the dump
- `spec.initFromBackup`, pointed at the source bucket prefix
- `spec.template` on the `MysqlStandbyCluster`, by requesting activation
- `spec.backup.pitr.enabled`, which replays the archived binlogs into the new group

**Correct option index:** 1

**Explanation:**

`spec.initFromBackup` is the one-shot restore-on-first-boot field: it gates bootstrap until `status.restore.phase` reads `Succeeded`, then clone and replication proceed normally. Option 1 names the wrong restore entry point — `restoreInPlace` is re-runnable and operates against an already-live active primary, which a brand-new DR group does not have. Option 3 is the trap the standby CRD sets: `spec.template` does hold the shape of the group that a future activation would materialise, but activation is not implemented, so nothing consumes it. Option 4 confuses the archive with the restore — PITR governs whether binlogs were shipped at the source, and it must have been enabled there for a `pointInTime` request to be accepted at all, but it does not itself restore anything. (objective 14)

## Question 4

**Type:** MULTIPLE_CHOICE

You are writing the on-call runbook for `playground` and want every step to be a `kubectl bloodraven` invocation. Which of these is not a subcommand the plugin has?

- `promote`
- `reclone`
- `restore`
- `verify-backup`

**Correct option index:** 2

**Explanation:**

The surface is exactly seven: status, promote, reclone, backup, verify-backup, version, help. There is no `restore`, and the reason is structural rather than an oversight — restore is not a CR at all, it is two fields on the failover group's spec, and the plugin's design rule is that it only writes resources the operator already reads and never talks to MySQL directly. `promote` and `reclone` exist because both are annotation-driven on the group, so the plugin can write them and inherit every gate. `verify-backup` exists because a verification really is its own CR that the operator reconciles into a throwaway instance. A runbook step that says `kubectl bloodraven restore` will fail at 3am with `unknown command`. (objective 15)

## Question 5

**Type:** SHORT_ANSWER

You are signing off `playground` for production tonight. Four findings are open: (a) nobody can say what `sync_binlog` is on the running instances; (b) `spec.replication.maxLagSeconds` is set and nobody has said what it gates; (c) the nightly S3 backup has never been verified; (d) the runbook quotes a 30-second anti-flap cooldown, taken from a playground run. Which do you block the launch on, and what do you attach to the ones you accept?

**Sample answer:**

Block on (a) and (c). `sync_binlog=1` is only an overridable default — it is written into the base my.cnf before `spec.mysqlConf` is applied, so any override in the group's config wins silently. Until somebody reads the value off a running instance, the durability claim in the runbook is unverified, and it is the claim every RPO statement rests on. (c) is the same failure in a different place: an unverified backup is an assumption, and GitLab's 2017 outage is what an assumption looks like when it is finally tested. Accept (b) and (d) with a named owner. For (b) the owner must know that `maxLagSeconds` drives only the `ReplicationLagging` condition and is not a promotion gate — a replica beyond 300 seconds is still promoted, because no writable site is almost always worse. For (d) the owner re-measures every runbook timing against the shipped defaults, since the playground overrides `failoverCooldown` to 30s (against 5m), `maxLagSeconds` to 30 (against 300) and `dns.ttl` to 10 (against 60), so no playground timing transfers unchanged.

**A full-credit answer shows:**

A strong answer blocks on (a) and (c) and accepts (b) and (d) with a named owner, and gives the mechanism in each case: sync_binlog is an overridable default written before spec.mysqlConf so an override wins silently; an unverified backup is an assumption; maxLagSeconds drives only the ReplicationLagging condition and is explicitly not a promotion gate; the playground overrides failoverCooldown, maxLagSeconds and dns.ttl so its timings do not transfer. A different split is acceptable if the reasoning names the mechanism — for example blocking on (d) because a runbook with wrong timings misleads during an incident. An answer that merely labels the items without a mechanism, or that treats maxLagSeconds as a promotion gate, is weak.

**Explanation:**

The gate is about verdicts, not notes. The two blockers are the ones where an unexamined assumption sits underneath a durability claim: an unread `sync_binlog` and an untested backup. The two acceptable items are dangerous only through ignorance — a lagging replica really is promoted, and playground timings really are not the shipped defaults — so they are survivable when a named human holds them and fatal when nobody does. (objective 15)
