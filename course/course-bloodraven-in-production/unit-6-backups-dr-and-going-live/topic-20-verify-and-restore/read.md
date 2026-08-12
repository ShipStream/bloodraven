# Verify, and restore

`orders` takes a nightly dump, ships sealed binlogs, and has a retention policy, and the counter app has been writing to it throughout. You have written a great deal of backup configuration and read back none of it. So you cannot answer the only question that matters at 3am: does last night's artifact load?

## Schrödinger backups

The API type says it plainly. A verification "restores a MysqlBackup artifact into an ephemeral, throwaway MySQL instance to prove the backup can actually be loaded. Unverified backups are schrödinger backups." Until something reads it, last night's object in S3 is not a backup. It is a file that is the right size.

`MysqlBackupVerification` is the CR that reads it, and the mechanism is deliberately unglamorous. The run gets an **ephemeral PVC** as its datadir — dedicated per run, deleted on cleanup, lifecycle never shared with the backup PVC — and an in-pod `mysqld` that binds `127.0.0.1` with **no Service**, so the instance is unreachable from outside its own network namespace. Nothing can point at it by accident, and nothing it does can touch `orders`. The PVC is auto-sized to `max(10Gi, ceil(1.5 × backup.status.sizeBytes / 10Gi) × 10Gi)` — a **10 GiB floor**, because a fresh datadir plus a small dump is already a few hundred megabytes. On failure, `keepOnFailure` (default `true`) leaves the Pod and PVC in place so you can `kubectl exec` in and look at the wreckage.

## The sanity check has exact semantics

Loading is the default contract. `spec.sanityCheck` extends it from "it loaded" to "it loaded something". The query is a single statement, run through the `mysql` CLI as `mysql -B -N -e` wrapped in `timeout`, and it must return **one row and one column**. Multi-statement input is rejected to keep the timeout budget predictable.

The detail that earns its keep: **an empty result set is treated as scalar 0**. That is precisely the shape a silently-empty restore produces — the query runs, nothing errors, no rows come back. Without that rule, "returned nothing" would look like success. With it, a `minRows: 1` floor catches it.

Failures split into two reasons, not one. `SanityCheckFailed` means the scalar came back below `expect.minRows`, or the query errored — the data is wrong. `SanityCheckTimeout` means the query exceeded `expect.maxDurationSeconds` (default 60) — the instance is wedged. Different problems, different pagers.

Pick an assertion whose expected value is structural rather than a moving row count. `SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = 'orders'` returns 1 when the schema landed and 0 when it did not, and `minRows: 1` turns that into a pass/fail.

```widget
{
  "type": "terminal",
  "title": "Verify last night's nightly backup for orders",
  "lines": [
    {
      "cmd": "kubectl bloodraven verify-backup orders --profile nightly",
      "out": "Created MysqlBackupVerification bloodraven-playground/orders-nightly-verify-9k2rt (profile=nightly, triggeredBy=manual)\nWatch with: kubectl get mysqlbackupverification orders-nightly-verify-9k2rt -n bloodraven-playground -w"
    },
    {
      "cmd": "kubectl get mysqlbackupverification orders-nightly-verify-9k2rt -o jsonpath='{.status.phase} {.status.sanityCheck.ran} {.status.sanityCheck.resultRow}'",
      "out": "Succeeded true 1"
    }
  ]
}
```

## What a `Succeeded` proves — and what it does not

Be exact here, because this is the sentence people quote back at you in an incident review. A `Succeeded` proves two things and no more: **the artifact loads into a real mysqld**, and **your chosen scalar assertion held**.

It does not prove logical equivalence with the live primary — that is a different tool's job. It does not rehearse your application's writes or a traffic cutover. Verification is not a DR drill. It is the cheap, automatable half of the problem, and the half that fails silently.

The strongest argument for running it is this project's own history: the verifier shipped broken. Chaos scenario 31 failed with `ERROR 1062 Duplicate entry` because the ephemeral verify `mysqld` was started with `gtid_mode=OFF`, which defeats server-side GTID dedup on replay, so cross-site archived binlogs double-applied (issue #101, fixed by PR #105). The mechanism that proves your backups are good was itself wrong — an argument for running verification, not against it. A broken verifier is discovered by running it, and by nothing else.

## Restore is two fields, not a CR

Operators reach for a `MysqlRestore` CR. **There is no restore CR.** Restore is two fields on the failover group, and choosing between them is the whole decision:

| Field | Shape | Use it to |
| --- | --- | --- |
| `spec.initFromBackup` | one-shot; gates normal bootstrap, skipped after success even if left in place | seed a brand-new group from an artifact |
| `spec.restoreInPlace` | re-runnable; no teardown-and-rename cycle | repair the live `orders` you already have |

An in-place restore runs against a live cluster, so it walks discrete phases one step per reconcile, and an operator restart always lands on a well-defined observable state.

```widget
{
  "type": "flow",
  "title": "spec.restoreInPlace phases",
  "steps": [
    {
      "label": "Preflight",
      "detail": "Confirm token accepted; preconditions validated — active site writable, deployment rolled out, source artifact resolvable"
    },
    {
      "label": "Fencing",
      "detail": "Topology manager frozen; for a full-instance restore the primary role label is stripped, so the -primary Service sheds endpoints for the duration"
    },
    {
      "label": "Restoring",
      "detail": "The restore Job runs loadDump, plus optional PITR replay, against the active primary"
    },
    {
      "label": "Resuming",
      "detail": "Role label restored, replica reclone scheduled (full-instance only), topology manager unfrozen"
    },
    {
      "label": "Succeeded / Failed",
      "detail": "Terminal. Failed is never retried automatically — bump confirm to re-arm"
    }
  ]
}
```

`Fencing` is the phase you already understand. Fencing stamps `role = "fenced"`, which matches neither the `-primary` selector nor the `-replicas` selector, so the site sheds its Service endpoints rather than serving a half-loaded database. Cross-site mutation is suppressed wholesale while a restore is in flight — no failover is going to fire underneath your load.

## The anti-fat-finger token

`spec.restoreInPlace.confirm` is required, must parse as an **RFC 3339 timestamp**, and must be **strictly greater** than the timestamp in `status.restoreInPlace.confirmTokenUsed`. Programmatic callers just send `now()`.

Look at what that buys. Re-applying last week's manifest does nothing: its token is no longer greater than the recorded one, so the reconciler sees a terminal status that already reflects that confirm value and returns. GitOps cannot replay a destructive restore. An invalid token on a re-arm emits a `RestoreInPlaceRejected` event and leaves the previous terminal status visible.

## Why `pointInTime` gets rejected

Ask for `pointInTime` while `spec.backup.pitr.enabled=false` and you get:

> `pointInTime is set but spec.backup.pitr.enabled=false; PITR restore requires the failover group to have continuous binlog archival configured on the source`

Both entry points share one builder, so the rejection is identical for `spec.initFromBackup.pointInTime` and `spec.restoreInPlace.pointInTime`. It is not a style preference: replay material only exists if something archived it, and with PITR off nothing did.

Where it surfaces matters more than the wording. This is a **reconciler error, not an admission rejection**. Your `kubectl apply` succeeds. The failure arrives seconds later, in the CR's status and the operator log. Look there, not at the exit code of the apply.

## Handoff

GitLab, January 2017: five backup mechanisms, none usable. `pg_dump` silently failing on a version mismatch, empty S3 uploads, misconfigured alert emails. They recovered from an incidental six-hour-old staging snapshot. Every one of those five would have passed a config review — the YAML was fine, the cron fired, the bucket existed. None would have passed a verification, because not one had ever been read back.

You can now verify a backup for `orders`, restore it in place through the confirm token, and state precisely what your verification did and did not prove. Which leaves the artifact itself: a dump of your customers' orders, sitting in an object store, in plaintext. What happens when you encrypt the data at rest instead?
