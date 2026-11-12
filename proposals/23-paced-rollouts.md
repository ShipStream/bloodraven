# 23. Paced rollouts of release-driven pod changes

Status: implemented in v1.8.0 (issue #244; Dragonfly part of #217). Design decisions marked **pending Colin's
sign-off** were settled without a stakeholder.

## 1. Problem

An operator release can change the pod template of every failover group at once: a render-version bump
(`deploymentPodRenderVersion`, `configMapRenderVersion`, `pitrPodRenderVersion`, `encryptionPodRenderVersion`), a
new launcher or `mysqld` flag list, or a new default sidecar image. Before this change every group acted on that
alone and at the same moment: `checkSpecDrift` started an ordered update in each `OrderedUpdate` group, and the
group reconciler restarted both sites of each `Recreate` group. A bad build, or a custom image the new launcher
cannot run in, hit every group together.

v1.8.0 is such a release (`deployment-pod-render-v8-mysql-launcher`, #280).

The requirements are R-1 to R-7 of finding A-52 in `proposals/20-api-v1beta1.md`. That audit designed a
cross-group queue three times and each review found a Major problem in it, so it kept only the requirements.

## 2. Design

### 2.1 What is paced: the renderer annotation

Every workload the operator renders (site Deployment, Dragonfly StatefulSet) carries
`bloodraven.shipstream.io/rendered-by`: the **renderer identity** of the operator that last wrote its pod
template. The identity is a digest of the operator image, the default sidecar image, the default MySQL image,
the four render versions and the launcher text (`rendererIdentity`).

A change to a workload is **release-driven** when another renderer wrote the workload (the annotation differs or
is missing, as on everything v1.7.0 rendered) and the running operator would change its template. Only those
changes are paced. A workload the running renderer already wrote is not: a change to it comes from the group
itself (its spec, its Secrets, its keyring or PITR state) and rolls as before.

"Would change" is:

| Workload | Test |
|---|---|
| Site Deployment, any strategy | The spec hash or the PITR render annotation differs. |
| Site Deployment, `Recreate` | Also any difference in the pod template that a server-side dry run of the write shows. The `Recreate` reconciler writes the whole template on every pass, so an unhashed template change would otherwise restart those groups at once. |
| Dragonfly StatefulSet | The template comparison the serial rollout already uses (`dragonflyStatefulSetTemplateEqual`). |

A workload another renderer wrote that would not change is **restamped**: a patch of the one annotation, never
a template write. A release that changes nothing a group renders therefore costs that group one metadata patch.

The operator image is part of the identity on purpose. Classifying a change by enumerating "release inputs" of
the hash would miss the next input someone adds; classifying by *who wrote the template* cannot. The cost is that
a spec edit made in the seconds between an operator upgrade and the restamp is paced too, which is the safe
direction.

### 2.2 The gate: one record, read by both sides (R-2)

`status.rollout` on the MysqlFailoverGroup is the admission record. Two places read it and nothing else:

- **The write path.** `reconcileDeployment` (used by the `Recreate` reconciler and by the ordered updater's
  `ReconcileSiteDeployment`) and the Dragonfly serial rollout call `admitRolloutWrite` before a release-driven
  template write. It reads the group uncached and refuses (`errRolloutHeld`) unless the record admits the running
  renderer. The ordered updater gets the error, so it never goes on to a handoff for a write that did not happen.
- **The drift check.** `specDriftView` reports a differing site as *held*, not as drift, by the same record
  (`rolloutHoldsSite`). No ordered update starts for a group that is not admitted. Held sites still count as
  "awaiting rollout" for the PITR archive conditions.

A held site keeps its per-site ConfigMap too. The unencrypted ConfigMap is mutable and mounted by the running
pod, so writing it early would hand a held pod the new configuration the next time it restarts for another
reason. The ConfigMap is written with the Deployment, after admission.

A group that is not admitted therefore keeps its rendered pod spec through unrelated spec edits: the edit changes
the hash of a workload another renderer wrote, which is still a held change. This is the cost of R-2 and is
documented: **while a group waits for its slot, spec changes that restart pods wait with it.**

Two exceptions, both deliberate:

- **A new site.** A Deployment that does not exist is created on the running renderer. A new pod is not a restart
  of a running one.
- **A clone recipient under encryption at rest.** A site whose keyring is unsealed for a clone
  (`UnsealReason=Clone`) must restart with a writable keyring before `CLONE INSTANCE` can run. Its roll is not
  held: holding it would park a group's recovery behind every other group's upgrade, and behind a halt
  indefinitely. The pod it restarts is one whose data is about to be replaced. *Pending Colin's sign-off.*

### 2.3 Admission: the pacer (R-1, R-3, R-6)

`RolloutPacer` runs on the leader. Every pass (5s) it lists the groups uncached, classifies each group's
workloads (`RolloutGroupState`: pending, rollout incomplete) and runs one pure function, `planRollout`, whose
output is status writes. Each write is conditional on the record the decision was taken on.

- **One slot.** A group with an unfinished admission, for any renderer, holds the slot. Nothing else is admitted.
- **Queue.** With the slot free and the soak elapsed (default 1m after the last completed roll), the first
  *stable* group with something pending is admitted: by namespace and name, deferred groups last. Stable is the
  deployment API's `unstableReason` being empty, which covers a restore, a group that is not `Ready`, a planned
  failover and the `AutomaticFailoverHeld` hold. An unstable group is passed over (`Pending/GroupNotStable`) and
  waits for the slot like any other once it is stable: it is never rolled outside the slot.
- **Completion** is the same for every `updateStrategy`: nothing pending, every workload's rollout finished
  (Deployment and StatefulSet status), and the group stable. `status.updatePhase` is not consulted.
- **`Recreate` groups are paced**, not excluded: the gate is on the write path they share with the ordered
  updater, so they get the same admission and the same completion signal. *Pending Colin's sign-off* (the issue
  allowed excluding them instead).

### 2.4 The latch and the halt (R-4)

`status.rollout.startedTime` is written, with a conditional update, **before** the first release-driven workload
write of an admitted group. It stays set until the roll completes.

- An admission that has **not** started can be withdrawn: when the group has nothing left to roll, or after
  `startGrace` (default 5m) when something else keeps it from starting (a migration lease, a hold that arrived
  after admission). The group goes to the back of the queue (`RolloutDeferred`).
- An admission that **has** started is never withdrawn. The group keeps the slot until it completes. The
  withdrawal and the latch are both conditional on the resource version, so one of them loses.
- A started roll is recorded as **stalled** (`stalledTime`, `RolloutStalled` Event) when the ordered updater
  reports a pod that is not Ready, or after `stallAfter` (default 30m). The stall stays recorded until the roll
  completes; the updater's own stall report resets between its retries and is not what the halt rests on.

So a bad build stops at the first group: that group started, cannot complete, and holds the only slot. Every
other group reports `Pending/RolloutHalted` and keeps its pods.

### 2.5 Restarts and a second upgrade (R-5)

The pacer keeps nothing in memory that decides anything. After a restart it reads the same records. An
unfinished admission written for another renderer (a second `helm upgrade` mid-rollout) is **retargeted**: same
group, same latch, same stall, new renderer. The fleet does not start over with another group while one is
half-rolled, and a halt survives the upgrade.

If the installed CRD is older than the operator, the API server prunes `status.rollout`. Nothing is then
admitted, so nothing rolls; the operator emits `RolloutRecordNotStored` once per group and sets
`bloodraven_rollout_record_not_stored`. This fails closed on purpose.

### 2.6 Dragonfly (#217)

Dragonfly StatefulSets are workloads of the group: a release-driven template change waits for the same
admission, latches the same start, and counts toward the same completion. The gate comes first: a StatefulSet
the group is not admitted to roll is not part of the group's serial rollout, and no takeover is sent for it.

The serial rollout inside a group is made strict in the same change (`dragonfly_rollout.go`):

- Nothing is written until the operator knows the Dragonfly master: status names a site, that site answers as a
  master, and no other site does.
- One site at a time. The template write marks the StatefulSet
  (`bloodraven.shipstream.io/dragonfly-rollout-in-flight`) in the same update; the mark is removed when the pod
  is Ready on the new template and the site is back in sync with the master (per-flow positions, `DFLY
  REPLICAOFFSET` against `DEBUG REPLICA OFFSET`). No other site is written meanwhile. The mark lives on the
  StatefulSet, so it survives an operator restart. The pacer's completion (2.3) does not read it: the group's roll
  counts as complete when every StatefulSet's own rollout status is, which can be before the last restarted site
  has finished its full sync.
- The master's site is last. A replica proven in sync takes the master over with `REPLTAKEOVER`; the takeover
  writes the former master's template before that pod is deleted. A takeover that fails is retried with a
  backoff, indefinitely. The rollout sends no `REPLICAOF NO ONE` and never restarts the master where it is.
- `bloodraven.shipstream.io/dragonfly-restart-in-place: <site>` is the administrator's way past a wait: one
  site's template is applied without those proofs, and the operator removes the annotation. *Pending Colin's
  sign-off.*

A Dragonfly rollout that waits keeps the group's roll unfinished, so it holds the slot (2.4): after `stallAfter`
the roll is recorded as stalled and the fleet halts there. That is the intended direction for a release whose
Dragonfly template does not come up. A group whose replica was already broken before the release halts the
fleet too; `rollout-release-slot` lets the queue continue past it. *Pending Colin's sign-off.*

### 2.7 Operator controls

| Control | Effect |
|---|---|
| Chart `rollout.pacing` (`BLOODRAVEN_ROLLOUT_PACING`) | `false` turns pacing off: every group rolls at once, as before v1.8.0. |
| Chart `rollout.soak`, `rollout.stallAfter`, `rollout.startGrace` | The three durations. |
| Annotation `bloodraven.shipstream.io/rollout-admit: "true"` | Admits this group now, outside the queue and past a halt. Declarative: remove it afterwards. |
| Annotation `bloodraven.shipstream.io/rollout-release-slot: "true"` | This group's unfinished or stalled roll no longer holds the slot. |
| Rolling the operator back | Workloads the old release rendered match again and are no longer held; the groups already rolled are paced back. (Rolling back to v1.7.0, which has no pacing, rolls them back at once.) |

Metrics: `bloodraven_rollout_groups{phase}`, `bloodraven_rollout_group_phase{namespace,group,phase}`,
`bloodraven_rollout_{admissions,deferrals,stalls,completions}_total`, `bloodraven_rollout_record_not_stored`.

## 3. Costs

- The gate sits in the Deployment write path and next to the spec hash. A slip there restarts or wedges every
  group. The hash itself is unchanged by this work (`TestMySQLLauncherRendering` and `TestMySQLServerArgs` did
  not move), and pacing can be switched off.
- A waiting group does not apply spec changes that restart pods (2.2). With a long queue or a halt that can be a
  long time; `rollout-admit` is the way out for one group.
- A group that is never stable is never admitted automatically and stays on the old pods until an administrator
  admits it. The `BloodravenRolloutPendingTooLong` alert covers it.
- New stored state: `status.rollout`, which needs the release's CRDs (2.5).
- One slot means a fleet of N groups takes N ordered updates plus N soaks. There is no parallelism setting yet.

## 4. Rejected alternatives

- **A queue object** (a ConfigMap or Lease in the operator namespace holding the order and the holder). It makes
  "one holder" a single-object write, but the gate would then read an object that is not the group, and the
  per-group evidence a restart needs (did *this* group start?) would live away from the group. The pacer is the
  only writer of admissions and runs under leader election, like everything else that must not run twice here.
- **Classifying by hash inputs** (2.1).
- **Deriving "started" from the workloads** (a Deployment already on the new template). A site added to a
  waiting group is created on the new template without any roll having started, so the evidence is ambiguous.
- **Excluding `Recreate`** from automatic rolls. It would leave those groups on old pods until someone edited
  them, and the gate that paces them is the one the ordered path needs anyway.
- **Pacing every roll**, group-driven ones included. A keyring unseal or a Secret rotation in one group would
  then wait for the fleet.

## 5. Tests

- `planRollout` unit tests: one slot, the stall latch across passes, a second upgrade, an unstable group passed
  over and not rolled outside its slot, deferral, soak, completion, both annotations.
- Gate tests against the real reconciler: an unadmitted group keeps Deployments and ConfigMaps through a spec
  edit while the drift check reports no drift; restamp; `Recreate` admission and the dry-run case; a sidecar
  default and a render-version change together with a pinned group; a pruned record.
- `TestPacedRollout_BadBuildStopsAtTheFirstGroup`: two groups, the real ordered updater and the real Deployment
  write path, a replica that never becomes Ready, an operator restart and a second upgrade mid-rollout.
- DST, a release-profile scenario and the upgrade E2E (v1.7.0 to the branch build): see the PR.
