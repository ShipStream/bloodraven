# Unit 7 test — Day 0 and day 2

<!-- Rendered from course.json by course-template/tools/render-views.mjs.
     Edit course.json, then re-render. Edits here are overwritten. -->

**Assesses:** Quick check: can you build a group from an empty namespace, design a certificate every client can verify against, and roll a MySQL upgrade without turning it into an incident?

**Passing score:** 70%

## Question 1

**Type:** MULTIPLE_CHOICE

You are writing a `MysqlFailoverGroup` for a new production group. Which of these does the operator create for you?

- The per-site Deployments, PVCs and ConfigMaps, eight Services for a three-site group, a PodDisruptionBudget, the `DNSEndpoint`, and the init-users ConfigMap that creates the MySQL users
- Everything in option 1, plus the credential Secrets — the operator generates passwords on first reconcile and writes them back
- Everything in option 1, plus the cert-manager `Certificate`, derived from `spec.tls.issuerRef`
- Only the Deployments and Services; PVCs are created by the StatefulSet controller and the DNS record by external-dns directly

**Correct option index:** 0

**Explanation:**

The line between yours and the operator's is the first thing to get right on day 0. Option 2 is a common hope and would be a security problem: you supply the Secrets, always. Option 3 is the specific trap this unit warns about — `issuerRef` records which issuer *should* produce the material, and the operator never creates the `Certificate`; forget it and the pods sit in `ContainerCreating` mounting a Secret that does not exist. Option 4 misattributes both the PVC (the operator reconciles it per site) and the `DNSEndpoint` (the operator writes the object; external-dns turns it into a record). (objective 1)

## Question 2

**Type:** MULTIPLE_CHOICE

A group is running in credentials mode. You rotate the password in the Secret named by `spec.credentials.appSecret` and apply. What happens?

- The pods roll, because credential Secret data is folded into the spec hash — but MySQL still expects the old password, because the `ALTER USER` only runs on a fresh datadir
- Nothing until the next pod restart, at which point the init script re-runs and applies the new password
- The operator reconciles the MySQL user on its next poll and the new password takes effect within seconds
- The apply is rejected: credential Secrets are immutable once a group has bootstrapped

**Correct option index:** 0

**Explanation:**

Two mechanisms are in play and only one of them does what you want. The spec hash includes credential Secret data, so the rotation genuinely rolls the pods — which makes it *look* as though the change took effect. But the `CREATE USER IF NOT EXISTS` / `ALTER USER` pair lives in the init script that the MySQL entrypoint runs only on an empty datadir, so MySQL's own view of the password is unchanged. Option 2 is the misreading the restart encourages. Option 3 invents a user-reconcile loop for this path. Option 4 invents an immutability rule. (objective 2)

## Question 3

**Type:** TRUE_FALSE

A `taintNodeSelector` that names labels no node in the cluster carries is rejected by the API server, because the operator validates it against live nodes at admission.

**Correct answer:** false

**Explanation:**

Nothing validates a manifest against the cluster it refers to. CEL rules check the *object* — uniqueness, mutual exclusion, required-unless-read-only, the interval relationships on `spec.sidecar` — and they are immediate and cheap. A `taintNodeSelector` matching nothing is admitted, and then fails completely and silently: the `NoExecute` taint is applied to no node, so the eviction half of your failover strategy does not exist and no status field, event or log line says so. It is worse than the missing StorageClass in the same class of error, because that one at least leaves you a `Pending` PVC to notice. (objective 3)

## Question 4

**Type:** MULTIPLE_CHOICE

Your cert-manager `Certificate` covers the group hostname and both shared Services. Sidecars crash-loop. Beyond the immediate outage, what have you actually lost?

- Self-fencing and the startup safety net on every site — the two mechanisms that hold correctness when the operator cannot be reached
- Only observability: the sidecar exports the archiver metrics, so alerting degrades but no safety property changes
- Replication, because the sidecar relays replication traffic between sites
- Nothing structural — the operator fences sites directly, and the sidecar is an optimisation for faster detection

**Correct option index:** 0

**Explanation:**

The sidecar verifies its own MySQL against its site's Service name because it dials loopback, which appears in no certificate. Without that SAN it cannot query MySQL, `/health` returns 503, and the liveness probe restarts the container — taking the `FencingMonitor` and the startup safety net with it. Unit 5's whole argument was that an operator which cannot reach a site cannot fence it, and the sites you most need fenced are exactly the ones you cannot reach; a certificate mistake has just removed the answer to that. Option 3 is wrong on the data path — replication is MySQL to MySQL. Option 4 inverts the division of labour. (objectives 4, 6)

## Question 5

**Type:** MULTIPLE_CHOICE

Which statement about `CHANGE REPLICATION SOURCE TO` is correct?

- With TLS it gains `SOURCE_SSL=1`; without TLS it gains `GET_SOURCE_PUBLIC_KEY=1`, and without either the IO thread exits asynchronously after a clean `START REPLICA`
- With TLS it gains `SOURCE_SSL=1`; without TLS no extra clause is needed, because `caching_sha2_password` falls back to plaintext authentication
- It always uses `SOURCE_AUTO_POSITION=0` on a TLS group, because GTID auto-positioning requires an unencrypted channel
- Neither clause is set by the operator; both are inherited from `spec.mysqlConf`

**Correct option index:** 0

**Explanation:**

The failure this prevents is the memorable part: with neither clause, `START REPLICA` returns cleanly and the IO thread exits *afterwards*, leaving a site permanently not replicating with nothing wrong at the point of the command. That is the shape you would create by running the statement by hand. Option 2 is the assumption the code comment exists to correct. Option 3 invents a conflict — `SOURCE_AUTO_POSITION=1` is always used. Option 4 misplaces the setting; `spec.mysqlConf` renders my.cnf and does not reach a replication channel. (objective 6)

## Question 6

**Type:** MULTIPLE_CHOICE

You bump `spec.image` at 09:00 on a Tuesday. Which of these should you expect, and warn your on-call about in advance?

- `activeSite` moves to the standby and does not move back, `bloodraven_failovers_total` increments, `BloodravenFailoverOccurred` fires, and the fresh `lastFailover` suppresses the next automatic failover for the whole cooldown window
- Both pods restart within a few seconds of each other and the group is briefly `TotalLoss`, which is expected under `OrderedUpdate`
- Nothing visible: ordered updates are excluded from the failover counter precisely so a rollout cannot be confused with an incident
- `activeSite` moves to the standby during the rollout and returns to the original site when it completes

**Correct option index:** 0

**Explanation:**

Every clause of option 1 is a consequence someone meets at 02:00 having not been told. The `Failover` phase is a real promotion through the ordinary sequence, and it is not cooldown-gated — but it *records* a failover, so it consumes your anti-flap budget for a genuine failure that follows. Option 2 describes `Recreate`, which is exactly the window `OrderedUpdate` exists to avoid. Option 3 is the exclusion people assume exists. Option 4 is the fail-back misconception from Unit 3: promotion is current-state-driven, and nothing remembers which site 'should' be primary. (objectives 7, 8)

## Question 7

**Type:** TRUE_FALSE

A rollout that stops with `standby is writable but replication is not running; aborting ordered update` means the updater timed out waiting for the standby to catch up.

**Correct answer:** false

**Explanation:**

It is a fail-fast abort, not a timeout — the outer deadline is five minutes and this fires long before it. A restarted MySQL pod comes up writable for a few seconds before anything fences it, and an ordered update deliberately suppresses cross-site recovery, so nothing is going to start replication for that standby. A site in that shape is stuck rather than slow, and continuing to wait would tell you nothing you do not already know. The counter behind it is deliberately not a strict streak: a probe error leaves it alone, because alternating dial errors and 'writable, no source' reads are exactly what a stale connection pool produces and a strict streak would let that mask the fault. (objective 8)

## Question 8

**Type:** MULTIPLE_CHOICE

Which upgrade order is correct, and why?

- CRDs by `kubectl apply` first, then `helm upgrade` the operator, then bump `spec.sidecarImage` — because Helm never upgrades CRDs from `crds/`, and a new operator against an old schema has its new fields silently pruned
- `helm upgrade` first, then CRDs — the chart's post-upgrade hook applies them once the new operator is running
- `helm upgrade` alone; the chart's `crds/` directory is applied on every upgrade, which is why it exists as a separate directory
- Bump `spec.sidecarImage` first so the sidecars are ready for the new operator, then CRDs, then the chart

**Correct option index:** 0

**Explanation:**

Helm installs CRDs from `crds/` on first install and never upgrades them — on upgrade or on rollback. Skip the explicit apply and you get the silent-pruning failure from Unit 5 by a different route: the object is admitted, the unknown field is dropped, and nothing errors at apply, in an event, or in the log. Option 3 is the belief that causes it. Option 2 invents a hook. Option 4 gets the sidecar in the wrong place: it is a group-level field whose bump triggers an ordered update, so it belongs last, after the control plane is on the new release. (objective 9)

## Question 9

**Type:** MULTIPLE_CHOICE

A rule you inherited alerts on the `Degraded` condition with `reason=Failover`. It has never fired. What is the correct fix?

- Delete it and alert on `bloodraven_failovers_total` instead — there is no `Failover` reason; the failover row of the matrix emits `Degraded`, and the five reasons that reach status are `Healthy`, `Degraded`, `SplitBrain`, `NoPrimary`, `TotalLoss`
- Change it to `reason=Degraded`, which is the same condition under a different name and will now fire correctly
- Add a `for: 0s` so it catches the transient window in which `Failover` is set before the promotion completes
- Nothing is wrong with it; it has not fired because no failover has occurred since it was written

**Correct option index:** 0

**Explanation:**

A rule matching a reason string the operator never emits does not fire rarely. It sits green forever, through every promotion. Option 2 is a real trap rather than an obvious wrong answer: `reason=Degraded` *is* what the failover row emits, but it is also what a live primary with an unreachable peer emits and what a writable non-promotable site awaiting fencing emits, so as a failover alert it is far too broad. The counter is the right signal for 'a failover happened'; the reason strings are for topology shape. (objectives 11, 12)

## Question 10

**Type:** SHORT_ANSWER

Six units ago you could not read a status dump. Name the three things you would now put in front of a new team member on their first on-call shift, and say what each one is for.

**Sample answer:**

One: the reference card, because the first thirty seconds of an incident should be spent reading rather than recalling — it holds the shipped defaults beside the playground overrides, the five condition reasons, the nine promotion steps with which four are fatal, the four Service kinds and their selectors, and the metric label sets that are not uniform. Two: the version appendix, because everything with a date on it — issue states, upstream pins, licence status, 'the published page says X' — is exactly what a course cannot keep true, and it carries the command that re-checks each one, so a stale memory costs a lookup rather than a wrong answer in a meeting. Three: the alert-to-runbook-to-first-command map from Unit 6, because an alert without a next step is a page with nowhere to go, and it is where the one alert nobody ships — application write failures after a successful failover — is owned by a named human. If I could add a fourth it would be the honest statement about the group itself: it promotes unattended in about 12 seconds, switches over on purpose at RPO 0 by construction, reports its exact lost-transaction count, and will not save them from a DNS record nobody flipped.

**A full-credit answer shows:**

A strong answer names the reference card (and at least two of: defaults-versus-overrides, the five reasons, fatality per step, Service selectors, label sets), the version appendix (and *why* — dated facts rot, and it carries re-check commands), and the alert-to-runbook map (and that the application-side write alert is unowned by default). Credit any third choice that is defensible and explained. Do not credit a list of three artefacts with no statement of what each is for.

**Explanation:**

The question is a check on whether the course produced judgement rather than recall. All three artefacts exist because a fact is only useful at the moment it is needed, in the form it is needed in — one screen, dated, with a next command attached. (objectives 10, 12)
