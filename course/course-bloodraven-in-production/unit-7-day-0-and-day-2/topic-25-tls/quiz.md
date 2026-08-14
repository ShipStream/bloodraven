# Quiz — TLS, and what it unlocks

<!-- Rendered from course.json by course-template/tools/render-views.mjs.
     Edit course.json, then re-render. Edits here are overwritten. -->

## Question 1

**Type:** MULTIPLE_CHOICE

You enable `spec.tls` with a cert-manager `Certificate` whose `dnsNames` cover the group hostname, `mysql-ledger-primary` and `mysql-ledger-replicas`. The MySQL containers start. Within a minute every sidecar container is in `CrashLoopBackOff`. What happened, and what have you lost?

- The per-site Service SANs are missing, so each sidecar cannot verify its own MySQL over loopback; `/health` returns 503, the liveness probe restarts it, and every site is now without self-fencing and without the startup safety net
- `--require-secure-transport=ON` blocks the sidecar because sidecars connect over the unix socket, so TLS has to be disabled for the sidecar container specifically
- The sidecars are waiting for the operator to distribute client certificates, which it does on its next poll; the crash loop resolves itself
- The `Certificate` is missing `ca.crt`, which the operator reports as `TLS secret missing required ca.crt` and the sidecars inherit

**Correct option index:** 0

**Explanation:**

The sidecar connects to its MySQL over loopback, and `127.0.0.1` appears in no certificate, so it verifies against its own site's Service name instead — which is why `mysql-<group>-<site>` must be in the SAN list even though no human ever dials it. The consequence is the part to carry: a crash-looping sidecar is a site with no self-fencing rule and no startup safety net, so a certificate mistake has quietly removed the layer that protects correctness when the operator cannot be reached. Option 2 is wrong on the mechanism — the sidecar uses TCP to loopback, not the socket — and there is no per-container TLS opt-out. Option 3 invents a distribution step; the Secret is mounted, nothing is distributed later. Option 4 is a real failure mode with a different symptom: a missing `ca.crt` fails the operator's own client path with that exact message. (objectives 4, 6)

## Question 2

**Type:** MULTIPLE_CHOICE

Your MySQL certificate has `duration: 2160h` and `renewBefore: 360h`. A colleague proposes shortening `renewBefore` to `720h` so renewals are never close to expiry. What is the operational consequence you should raise?

- Each renewal rewrites the TLS Secret, which changes every site's spec hash and triggers an ordered update — so a shorter window means more frequent primary moves, each one incrementing `bloodraven_failovers_total` and firing the failover alert
- None for Bloodraven: mysqld reloads certificate files in place, so renewal is invisible to the operator
- A shorter window risks the operator reading a half-written Secret, so the safe minimum is one full poll interval
- Renewals are only picked up on the next planned failover, so a shorter window has no effect until you move the primary yourself

**Correct option index:** 0

**Explanation:**

The spec hash deliberately folds in a SHA-256 of every TLS Secret key, precisely so that a rotated certificate reaches the running mysqld — which cannot pick up new files by itself. The price is that renewal *is* a rollout: standby first, a real failover, then the old active. Option 2 is the assumption this question exists to break. Option 3 invents a race that the hash mechanism does not have. Option 4 inverts cause and effect — the renewal triggers the update, not the other way round. The practical conclusion is that `renewBefore` belongs in the same conversation as your change freezes. (objective 5)

## Question 3

**Type:** TRUE_FALSE

With `spec.tls` unset, `CHANGE REPLICATION SOURCE TO` is issued with no TLS-related clause at all.

**Correct answer:** false

**Explanation:**

It gains `GET_SOURCE_PUBLIC_KEY=1` instead. MySQL's default `caching_sha2_password` authentication needs the source's RSA public key for a non-TLS replication channel, and without it `START REPLICA` returns cleanly while the IO thread exits asynchronously — leaving the site permanently not replicating with nothing wrong at the point of the command. Carry it as a diagnostic: a clean `START REPLICA` followed by silent non-replication, on a group without TLS, is an authentication-material problem rather than a network one, and it is exactly what you would create by running the statement by hand without either clause. (objective 6)

## Question 4

**Type:** MULTIPLE_CHOICE

You are bringing up a new encrypted group. In which order do you apply things, and why does the order matter?

- Issuer, then `Certificate` (waiting for the Secret), then the group with `spec.tls`, then confirm all sites Ready with one writable, and only then `encryptionAtRest.enabled: true` — so a TLS mistake surfaces as a crash-looping sidecar before encryption is in the picture
- The group with both `spec.tls` and `encryptionAtRest.enabled` in one apply, then the Issuer and `Certificate` — the operator waits for the Secret and rolls the pods when it appears
- `encryptionAtRest.enabled` first, so the keyring is sealed before any certificate exists to be stolen, then TLS
- Issuer and `Certificate` only; `spec.tls` is inferred from the presence of a cert-manager `Certificate` in the namespace

**Correct option index:** 0

**Explanation:**

Step four is the one people skip and the reason the order is worth stating. Turning on TLS and encryption in the same apply means a crash-looping sidecar and a keyring stuck in `Unsealed` arrive together, and you get to guess which caused which. Option 2 gets the dependency backwards and leaves pods in `ContainerCreating` mounting a Secret that does not exist. Option 3 is impossible — the CEL rule refuses `encryptionAtRest.enabled` without `spec.tls`. Option 4 invents inference the operator does not do: `spec.tls.secretName` is read literally. (objectives 4, 6)

## Question 5

**Type:** SHORT_ANSWER

Your security review accepts TLS in transit and asks the obvious follow-up: 'so the backups are covered too?' Answer precisely — what TLS does and does not reach on the backup and restore path.

**Sample answer:**

Backup and restore Jobs are covered: they get the same Secret mounted at `/etc/mysql/tls` and connect with `ssl-mode=VERIFY_CA` against its `ca.crt`, for both the `mysqlsh` dump session and the `mysqlbinlog | mysql` PITR replay. A TLS-enabled Job fails before connecting if the CA path is empty, missing, unreadable or unusable — it never downgrades to unverified TLS, so there is no silent-plaintext case to worry about. Backup *verification* is the deliberate exception: it loads the dump into an ephemeral mysqld that listens on loopback with no certificate and no Service, so that connection stays plaintext. That is not a gap in transit security, because the instance is unreachable from outside its own network namespace — nothing can dial it. And TLS says nothing about the artefact itself: the dump sitting in the bucket is protected by object-store encryption and `spec.backup` encryption, not by `spec.tls`.

**A full-credit answer shows:**

A full-credit answer covers: (1) dump and PITR-replay Jobs use the same Secret with `ssl-mode=VERIFY_CA`; (2) a TLS Job fails closed rather than downgrading; (3) verification is the exception, and *why* it is safe — loopback, no certificate, no Service, so nothing can reach it; (4) the distinction between transit and at-rest — TLS does not protect the object in the bucket. An answer that only says 'yes, backups use TLS' has missed both the exception and the boundary.

**Explanation:**

The question is really about knowing where a guarantee stops. Two of the three paths are covered and fail closed; the third is deliberately outside the guarantee and is safe for a structural reason rather than a cryptographic one. The at-rest half belongs to Unit 6's encryption topic, and conflating the two is the commonest way this answer goes wrong in a review. (objective 6)
