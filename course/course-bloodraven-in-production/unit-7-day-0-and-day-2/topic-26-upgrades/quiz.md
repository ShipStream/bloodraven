# Quiz — Upgrading without an incident

<!-- Rendered from course.json by course-template/tools/render-views.mjs.
     Edit course.json, then re-render. Edits here are overwritten. -->

## Question 1

**Type:** MULTIPLE_CHOICE

At 02:14 you are paged: `BloodravenFailoverOccurred` on the `ledger` group, `activeSite` has moved from `iad` to `pdx`, and the DNS record flipped. Nothing looks broken. What is the first thing to check, and why?

- `status.updatePhase` — it is non-empty for exactly the duration of an ordered update, and a routine `spec.image` or `spec.sidecarImage` bump performs a real promotion that increments the same counter
- `status.lastFailover` against `failoverCooldown` — if the promotion happened inside the cooldown window it must have been manual
- `bloodraven_divergent_transactions` — a rollout-driven promotion never produces divergence, so a zero reading proves it was planned
- The `Degraded` condition reason — an ordered update sets `reason: Updating`, which distinguishes it from an emergency failover

**Correct option index:** 0

**Explanation:**

An ordered update's `Failover` phase runs the ordinary nine-step promotion and its completion callback stamps the durable record and increments `bloodraven_failovers_total`. From the metric alone a rollout and a dead primary look identical, and `status.updatePhase` is the one field that separates them at a glance. Option 2 reasons from the wrong gate: the ordered-update handoff is not cooldown-gated at all, so an in-cooldown promotion is evidence of a rollout rather than of a human. Option 3 is not reliable — divergence depends on what the demoted site committed, not on why it was demoted. Option 4 invents a condition reason; there are exactly five, and `Updating` is not among them. (objective 7)

## Question 2

**Type:** MULTIPLE_CHOICE

You bump `spec.image` on a two-site group. Ninety seconds in, the rollout stops with `standby is writable but replication is not running; aborting ordered update`. What has happened?

- The standby's pod restarted onto the new image and came back writable with no working replication, and because cross-site recovery is suppressed during an update nothing is going to start it — so the updater gives up early rather than burning its five-minute deadline
- The image tag is wrong and MySQL failed to start, so the checker read a stale writable value from before the restart
- The primary was promoted twice, leaving both sites writable, and the updater refuses to continue into a split brain
- `recoveryThreshold` was not reached before the deadline, so the standby never debounced to `writable` and the updater timed out

**Correct option index:** 0

**Explanation:**

This is the restart hazard from Unit 5 met inside a procedure you started. A MySQL pod comes up writable for a few seconds before anything fences it, and an ordered update deliberately suppresses cross-site recovery — so a standby that stays writable with no replication is stuck, not slow. The updater counts writable observations and aborts early, and it deliberately does not reset that counter on a probe error, because alternating dial errors and 'writable, no source' reads are exactly what a stale connection pool produces. Option 2 describes a different failure with a different message. Option 3 misreads the abort as a split-brain guard. Option 4 confuses the poll loop's debounce with the updater's own wait, which reads MySQL directly. (objective 8)

## Question 3

**Type:** TRUE_FALSE

Upgrading the standby before the primary is a Bloodraven design preference; the reverse order also works, it simply costs more availability.

**Correct answer:** false

**Explanation:**

It is MySQL's requirement, not a preference. A replica may run a newer MySQL than its source; a source may not run a newer MySQL than its replica. Upgrading the standby first keeps the newer version on the replica side of replication for the entire window, which is the supported direction, and it is why the sequence ends with a failover rather than beginning with one. Doing it the other way puts an older replica behind a newer source, which is unsupported and does not always fail immediately or obviously. (objective 8)

## Question 4

**Type:** MULTIPLE_CHOICE

You run `helm upgrade bloodraven ./charts/bloodraven` to move from one release to the next. The operator pod restarts cleanly and reports healthy. A field the release notes describe as new is silently ignored on your `MysqlFailoverGroup`. What went wrong?

- Helm installs CRDs from `crds/` on first install and never upgrades them, so the API server is still validating against the old schema and pruned the unknown field without an error
- The operator caches the CRD schema at startup and needs a second restart after the chart upgrade
- The new field requires a matching `spec.sidecarImage` bump, and the sidecar rejects fields it does not recognise
- The field was set on the wrong resource; new fields land on `MysqlStandbyCluster` first and are promoted to `MysqlFailoverGroup` a release later

**Correct option index:** 0

**Explanation:**

This is the same silent-pruning failure Unit 5 met with `preferSite`, arriving by a different route: the object is admitted, the unknown field is dropped, and nothing errors anywhere — not at apply, not in an event, not in the log. The fix is procedural, not a flag: apply the CRDs explicitly with `kubectl apply -f` **before** `helm upgrade`, and treat CRD changes in release notes as an action item rather than a note. Options 2 and 3 invent mechanisms. Option 4 invents a release process. (objective 9)

## Question 5

**Type:** SHORT_ANSWER

Write the order in which you would upgrade a production Bloodraven install from one release to the next, and name what each step can disturb.

**Sample answer:**

First, read the release notes for two things only: CRD changes and the sidecar image tag — those are the only items that require action. Second, `kubectl apply -f` the CRDs from `config/crd/bases/` (or the chart's `crds/`), because Helm will not do it on upgrade or on rollback, and a new operator against an old schema means silently pruned fields. That step disturbs nothing running. Third, `helm upgrade` the operator chart: one Deployment, one replica, leader election, and the operator is not on the request path — so this costs failover cover for a few seconds and costs the data plane nothing. Fourth, bump `spec.sidecarImage` to the matching release and apply; this is an ordered update, so it restarts every site's pod, moves the primary, increments `bloodraven_failovers_total` and fires the failover alert. If the release also moves `spec.image`, do both in the same apply so you pay for one rollout instead of two. Fifth, run a backup verification against the new version, because a restore path you have not exercised since the upgrade is an assumption again.

**A full-credit answer shows:**

A strong answer has CRDs before the chart, gives the reason (Helm never upgrades CRDs, and the failure is silent pruning), places the sidecar bump last among the changes and says it is an ordered update that moves the primary and fires the failover alert, and closes with a verification run. Credit combining `spec.image` and `spec.sidecarImage` into one apply. Credit noting that the operator upgrade itself is cheap because it is not on the request path. An answer that treats `helm upgrade` as the whole procedure has missed the step that actually causes incidents.

**Explanation:**

The ordering exists because the three things you upgrade move by three different mechanisms with three different blast radii, and only one of them is automatic. Getting CRDs after the chart produces a failure with no symptom; getting the sidecar bump wrong produces a failover you did not schedule. (objective 9)
