# Quiz — Verify, and restore

<!-- Rendered from course.json by course-template/tools/render-views.mjs.
     Edit course.json, then re-render. Edits here are overwritten. -->

## Question 1

**Type:** MULTIPLE_CHOICE

A scheduled verification of the `playground` nightly profile reports `Succeeded`, with `status.sanityCheck.resultRow: "1"` against a `minRows: 1` floor. What has it proved?

- The artifact loads into a real mysqld, and your scalar assertion held. Nothing about the live primary.
- The artifact is logically equivalent to the live primary as it stood when the dump was taken.
- The group can be restored inside your RTO, because the verification timed a full load end to end.
- Your application would come up cleanly on the restored data, since the load raised no errors.

**Correct option index:** 0

**Explanation:**

A verification restores the artifact into an ephemeral throwaway mysqld and runs whatever scalar you asserted. That is the entire contract, and it says nothing about the live primary. Logical equivalence with the primary is explicitly out of scope — it needs a different tool, and the verification never contacts the primary to compare. An RTO claim is wrong twice over: the verification loads into a cold, isolated instance with its own PVC and no Service, not into your production path, so its wall clock is not your recovery time. And application-level rehearsal of writes or traffic cutover is the thing verification deliberately does not do — that is a DR drill. A Succeeded is the cheap half of the problem, and the half that otherwise fails silently. (objective 4)

## Question 2

**Type:** MULTIPLE_CHOICE

`playground` is live and healthy, but a bad migration last night corrupted one schema. You want last night's dump loaded back into the running group. Which field do you set?

- `spec.initFromBackup`, pointed at the MysqlBackup, since that is the restore entry point.
- `spec.restoreInPlace`, with `confirm` set to the current RFC 3339 timestamp.
- A `MysqlRestore` CR in the same namespace, referencing the MysqlBackup by name.
- `spec.restoreInPlace`, but only after deleting and recreating the failover group first.

**Correct option index:** 1

**Explanation:**

`spec.restoreInPlace` is the re-runnable entry point that operates against a live cluster: it fences, loads into the active primary, and resumes. `spec.initFromBackup` is one-shot and gates *bootstrap* — on an existing group that already bootstrapped, it is skipped, so setting it does nothing. There is no `MysqlRestore` CR; restore is two fields on the failover group, and reaching for a CR that does not exist is the most common wrong first move here. Deleting and recreating the group is exactly the teardown-and-rename cycle `restoreInPlace` was built to avoid. (objective 5)

## Question 3

**Type:** TRUE_FALSE

Your `MysqlFailoverGroup` manifest is reconciled continuously by a GitOps controller. Leaving a `spec.restoreInPlace` block in that manifest means the destructive restore re-runs on every sync.

**Correct answer:** false

**Explanation:**

The reversal: a re-applied manifest re-runs nothing, and that is the whole point of the token design. `confirm` must be an RFC 3339 timestamp strictly greater than `status.restoreInPlace.confirmTokenUsed`. After a run, the executed token is recorded, so the identical manifest now carries a token that is not greater — the reconciler sees a terminal status that already reflects it and returns. The same protection covers the older failure mode of applying last month's manifest by accident. To restore again you must deliberately bump `confirm` forward, which is a decision no sync loop makes for you. (objective 5)

## Question 4

**Type:** MULTIPLE_CHOICE

You apply a group with `spec.restoreInPlace.pointInTime` set while `spec.backup.pitr.enabled=false`. Where does the rejection reach you?

- As a `kubectl apply` failure from an admission webhook, before the object is persisted.
- As a CEL validation message from the API server, which refuses to store the object.
- In the CR status and the operator log, seconds after a `kubectl apply` that succeeded.
- Only in the restore Job's container logs, once the Job pod starts and fails to replay.

**Correct option index:** 2

**Explanation:**

The check lives in the reconciler's PITR fragment builder, not in the admission chain, so the write is accepted and the error surfaces afterwards on the object and in the operator log — watch there, not at the exit code of your apply. It is not an admission webhook rejection and not a CEL rule: neither runs this check, which is why the apply comes back clean. Nor does it wait for the restore Job — the builder returns the error while assembling the Job's init containers, so no Job is ever created. The same rejection covers `spec.initFromBackup.pointInTime`, because both entry points share one builder. (objective 6)

## Question 5

**Type:** SHORT_ANSWER

Chaos scenario 31 failed with `ERROR 1062 Duplicate entry` because the ephemeral verify `mysqld` was started with `gtid_mode=OFF`, defeating server-side GTID dedup on binlog replay. What does that bug argue for, and why?

**Sample answer:**

It argues for running verification, not for distrusting it. The bug was in the verifier itself: the throwaway mysqld ran gtid_mode=OFF, so replayed archived binlogs re-applied transactions the dump already contained and the run died on a duplicate key. That defect was invisible in configuration review — the backup profile, the archiver and the CR schema were all correct — and it could only be found by actually restoring an artifact and watching it fail. The general lesson is that the mechanism which proves your backups are loadable is itself a piece of software that can be wrong, and the only way to discover that is to exercise it on a schedule against real artifacts, and to treat a Failed verification as information about the whole chain rather than assuming the backup is at fault.

**A full-credit answer shows:**

A strong answer covers: (1) the conclusion is 'run verification more, not less'; (2) the bug was in the verify path, not in the backup artifact; (3) it was undetectable by config review and only surfaced by executing a real restore; (4) ideally, that a Failed verification implicates the whole chain — artifact, replay path, and verifier — so triage should not stop at the dump. Reject answers concluding that verification is unreliable and should be skipped, or that the backups themselves were corrupt.

**Explanation:**

The verifier shipping broken is the strongest available argument for verification, because it is a defect in exactly the class of thing verification exists to catch: something that looks correct in YAML and only misbehaves when data actually moves. Scenario 31 caught it (issue #101, fixed in PR #105). Nothing else would have. (objective 4)
