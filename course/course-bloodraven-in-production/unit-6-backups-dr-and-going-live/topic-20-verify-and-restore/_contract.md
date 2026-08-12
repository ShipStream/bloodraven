# Verify, and restore

**Unit:** 6 — Backups, disaster recovery, and going live
**Objectives (unit-numbered):**
4. Run a backup verification and say precisely what a `Succeeded` result proved   [obj 4]
5. Restore `orders` in place using the RFC 3339 confirmation token   [obj 5]
6. Explain why a `pointInTime` request is rejected when PITR is disabled   [obj 6]

## Topic generation prompt

Open with the framing straight from the API type: a verification restores a `MysqlBackup` artifact into an ephemeral, throwaway MySQL instance to prove the backup can actually be loaded, because unverified backups are schrödinger backups. You do not know whether last night's dump is a backup or a file until something reads it.

Teach the mechanism concretely: an ephemeral PVC plus an in-pod `mysqld` bound to `127.0.0.1` with no Service, so the verification instance is unreachable by anything else, and a PVC floor of 10 GiB. Then the sanity check's exact semantics — it runs via `mysql -B -N -e` wrapped in `timeout`, expects one row and one column, treats an empty result as scalar 0, and fails as `SanityCheckFailed` or `SanityCheckTimeout`. Be precise about what a `Succeeded` therefore proves: the artifact loads and your chosen scalar assertion held. Be equally precise about what it does not prove — logical equivalence with the live primary, and any application-level rehearsal of writes or traffic cutover. Verification is not a DR drill.

Then give the strongest possible argument for verification, from this project's own history: the verifier itself shipped broken. Chaos scenario 31 failed with `ERROR 1062 Duplicate entry` because the verify `mysqld` ran `gtid_mode=OFF`, which defeats GTID dedup on replay. The mechanism that proves your backups were broken can itself be broken, which is an argument for running it, not against.

Then restore, and lead with the shape, because operators reach for a CR that does not exist. There is **no restore CR**. Restore is two fields on the failover group: `spec.initFromBackup` is one-shot and gates bootstrap, and `spec.restoreInPlace` is re-runnable with no teardown-and-rename cycle. Walk the in-place phases — `Preflight`, `Fencing`, `Restoring`, `Resuming`, then `Succeeded` or `Failed` — and connect `Fencing` back to what the learner already knows about fenced sites shedding Service endpoints. Then the anti-fat-finger token: `confirm` is required, must be an RFC 3339 timestamp, and must be **strictly greater** than the timestamp recorded in `status.restoreInPlace.confirmTokenUsed`. Show why that design works — copying the old manifest cannot re-trigger a restore, because the token it carries is no longer greater. Finally, `pointInTime` is rejected when `spec.backup.pitr.enabled=false`, for **both** entry points, and the rejection arrives as a reconciler error rather than at admission — so the learner should expect it in the CR status and the operator log, not in a `kubectl apply` failure.

Close on GitLab's January 2017 outage: five backup mechanisms, none of them usable — `pg_dump` silently failing on a version mismatch, empty S3 uploads, misconfigured alert emails — and recovery from an incidental six-hour-old staging snapshot. Every one of those five would have passed a config review. None would have passed a verification. Do NOT cover encryption at rest or the keyring — topic 3 owns them.

## Requested activities

- READ: 900-1100 words. The schrödinger framing, the ephemeral-PVC/127.0.0.1/10 GiB mechanism, the sanity check's exact semantics and its two failure reasons, what `Succeeded` does and does not prove, the scenario 31 `gtid_mode=OFF` story, the two restore fields, the in-place phases, the RFC 3339 strictly-greater token, the `pointInTime` rejection at both entry points as a reconciler error, and GitLab 2017. One `terminal` widget showing a verification's status transitioning to `Succeeded` alongside the sanity-check result; optionally one `compare` widget on what verification proves versus what only a DR drill proves.
- FLASHCARDS: schrödinger backups, ephemeral PVC + 127.0.0.1 + no Service, 10 GiB floor, `SanityCheckFailed` vs `SanityCheckTimeout`, empty result treated as 0, `spec.initFromBackup` vs `spec.restoreInPlace`, the five in-place phases, RFC 3339 strictly-greater `confirm`, `pointInTime` rejected when `pitr.enabled=false`. 10-12 cards.
- QUIZ: 5 questions. What a `Succeeded` verification proves about the live primary (nothing); which field to use to bootstrap a brand-new group from a backup versus repair an existing one; why re-applying an unchanged manifest does not re-run a restore; where the `pointInTime` rejection surfaces; and what scenario 31's `gtid_mode=OFF` bug argues for.

## Handoff

**Inherits:** The learner can configure backups and PITR for `orders` and knows what is not recoverable.
**Leaves:** The learner can verify a backup, restore `orders` in place through the confirm token, and say exactly what their verification did and did not prove.
**Do not cover:** Encryption at rest, the keyring lifecycle (topic 3), metrics and alerting (topic 4), cross-cluster DR (topic 5).
