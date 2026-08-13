# Quiz — Encryption at rest and the keyring lifecycle

<!-- Rendered from course.json by course-template/tools/render-views.mjs.
     Edit course.json, then re-render. Edits here are overwritten. -->

## Question 1

**Type:** MULTIPLE_CHOICE

You enable `spec.encryptionAtRest` on `playground` and watch `status.encryptionAtRest.sites[]`. Which phase tells you a site is finished and protected?

- `Escrowed` — the keyring is safely captured in a Secret and the digest is verified
- `Sealed` — the keyring is projected read-only and mysqld cannot add keys
- `Unsealed` — the keyring is live and MySQL is using it
- `Pending` — the site is waiting for nothing further, the operator has no more work

**Correct option index:** 1

**Explanation:**

`Sealed` is the steady state: the keyring data file is projected read-only from the escrow Secret, so mysqld physically cannot add a key. `Escrowed` is genuinely reassuring — the bytes are captured and the digest verified — but the Deployment is still rolling onto the sealed rendering, so the pod is still running a writable keyring. `Unsealed` is the trap: MySQL is running and encrypting, which looks finished, but the keyring is on a memory-backed volume MySQL can write, and the site is not considered protected. `Pending` is the very start — no escrowed keyring exists at all. (objective 7)

## Question 2

**Type:** MULTIPLE_CHOICE

`pdx` was `Sealed`. You annotate `playground` with `rotate-keyring=pdx` and the site now reads `phase: Unsealed`. What is happening?

- The rotation failed and the site fell back to its bootstrap state, so its escrowed keyring is gone
- The site is running a writable memory-backed keyring so it can mint a new master key, and is not protected until it re-seals
- The site is waiting for a `CLONE INSTANCE` from the primary before the new key can be created
- The phase is stale; a rotation never leaves `Sealed`, so you should re-apply the annotation

**Correct option index:** 1

**Explanation:**

Rotation is the one transition that moves backwards out of `Sealed`, re-entering `Unsealed` deliberately — a rotation needs a writable keyring, so the site is genuinely unprotected until it escrows the new keyring and re-seals. It is not a failure state: a failed lifecycle lands in `Failed`, and the escrow Secret versions are immutable and retained, so the old keyring has not gone anywhere. No clone is involved — `unsealReason` would read `Clone` rather than `Rotation` if it were. And the phase is not stale: this is exactly why you read `phase` beside `unsealReason` and the rotation target rather than alone, since bootstrap and rotation produce the identical phase string. (objective 7)

## Question 3

**Type:** MULTIPLE_CHOICE

Your cluster has no API-server encryption at rest for Secrets. You enable `spec.encryptionAtRest` on `playground` anyway and every site reaches `Sealed`. What have you actually achieved?

- Full protection: the keyring never touches a persistent disk, so where etcd stores Secrets is irrelevant
- Nothing at all — the operator refuses to seal sites in a cluster without API-server encryption
- You have moved the keys from the MySQL data disk to the control-plane disk, and etcd is now part of your key custody
- Protection against a stolen PVC only after you also rotate every site's keyring at least once

**Correct option index:** 2

**Explanation:**

The live keyring is projected from a Kubernetes Secret, and Kubernetes stores Secrets unencrypted in etcd by default — so without API-server encryption the keys have relocated from the MySQL data disk to the control-plane disk rather than been protected. The first option confuses the pod's view with the cluster's: it is true that the keyring only ever lands on tmpfs inside the pod, but the Secret backing it is at rest in etcd. The second inverts the load-bearing sentence: 'None of these are optional. Bloodraven cannot verify them for you' — nothing in the operator checks, so sites seal happily. The fourth invents a dependency; rotation mints a new master key but changes nothing about where the escrow is stored. (objective 8)

## Question 4

**Type:** TRUE_FALSE

`spec.tls` is a strong recommendation for an encrypted group: if you accept plaintext traffic inside the cluster, you can enable `spec.encryptionAtRest.enabled` without it.

**Correct answer:** false

**Explanation:**

The reversal: it is a hard CEL rejection at admission, not a recommendation — `spec.encryptionAtRest.enabled requires spec.tls: MySQL requires a secure connection to clone encrypted data`. The reason is upstream, not defensive: MySQL requires a secure connection when cloning encrypted data regardless of any `REQUIRE SSL` clause, and `CLONE INSTANCE` is exactly how Bloodraven reseeds a diverged site. Enabling the pair without TLS would produce a group that cannot be reclone-recovered, so the CRD refuses the combination up front instead of failing you during a recovery. (objective 8)

## Question 5

**Type:** SHORT_ANSWER

`playground` has three sites: `iad` (active primary), `pdx` (replica) and `reader` (`role: read-only`). You need every site's master key rotated. Which site cannot be rotated right now, why, and what do you do about it?

**Sample answer:**

`iad`, because it is the active primary and the operator refuses to rotate it — rotation runs with a writable keyring, and that is the only window in which a keyring can be lost, so on the primary a loss would cost data rather than a re-clone. Rotate `pdx` and `reader` first, one at a time, waiting for each to return to `Sealed`. Then run a planned failover onto `pdx`. `iad` is now a replica, so the refusal no longer applies and you rotate it last.

**A full-credit answer shows:**

A strong answer names `iad` and ties the refusal to its role as active primary rather than to its name; explains that rotation needs a writable keyring and that a keyring lost on a replica is recoverable by re-cloning while one lost on the primary is not; and gives the workaround as rotate the other two sites, planned-failover away from `iad`, then rotate it. Answers that say the `reader` cannot be rotated (it can — it is never a primary) or that treat `iad` as permanently unrotatable have missed the point.

**Explanation:**

The unrotatable site is defined by current role, not identity: whichever site is active is the one the operator refuses, so the answer changes the moment you fail over. `reader` is a tempting wrong answer because it is excluded from other things — it can never be promoted and is never a backup source — but nothing stops it rotating. The fix is the planned failover from Unit 4: make `iad` a replica and the refusal evaporates. (objective 9)
