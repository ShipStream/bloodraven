# Flashcards — TLS, and what it unlocks

<!-- Rendered from course.json by course-template/tools/render-views.mjs.
     Edit course.json, then re-render. Edits here are overwritten. -->

**Front:** `spec.tls` — the two fields, and what the operator does with them

**Back:** `secretName` (a Secret with `ca.crt`, `tls.crt`, `tls.key`) and `issuerRef` (`name` plus `kind`, enum `Issuer` or `ClusterIssuer`). The operator records the issuer and reads the Secret. It does **not** create the `Certificate` — that is cert-manager's job and yours to declare.

---

**Front:** Which names the certificate's SAN list must cover

**Back:** The DNS hostname, `mysql-<group>-primary`, `mysql-<group>-replicas`, every `mysql-<group>-<site>`, and every `mysql-<group>-<site>-internal`. Two sites plus a reader is nine names, and adding a site means editing the list.

---

**Front:** What a missing per-site SAN costs you

**Back:** The sidecar connects over loopback and verifies against its own site's Service name, so without that SAN it cannot query MySQL at all: `/health` returns 503, the liveness probe restarts the container, and the site loses self-fencing *and* the startup safety net.

---

**Front:** What `spec.tls` does to mysqld's command line

**Back:** Adds `--ssl-ca`, `--ssl-cert`, `--ssl-key` from `/etc/mysql/tls`, plus `--require-secure-transport=ON`. Plaintext is refused server-side, and the fallback to mysqld's auto-generated `server-cert.pem` is deliberately prevented.

---

**Front:** `SOURCE_SSL=1` versus `GET_SOURCE_PUBLIC_KEY=1`

**Back:** `CHANGE REPLICATION SOURCE TO` gains `SOURCE_SSL=1` with TLS on and `GET_SOURCE_PUBLIC_KEY=1` with it off. Without either, `caching_sha2_password` leaves `START REPLICA` succeeding while the IO thread exits asynchronously — a site that is permanently not-replicating with no error at the point of the command.

---

**Front:** What TLS does to backup and restore Jobs

**Back:** They get the same Secret at `/etc/mysql/tls` and connect with `ssl-mode=VERIFY_CA` — both the `mysqlsh` dump and the `mysqlbinlog | mysql` replay. A TLS-enabled Job fails before connecting rather than downgrading to unverified TLS.

---

**Front:** The one place TLS deliberately does not apply

**Back:** Backup verification. The ephemeral verify instance listens on loopback with no certificate and no Service, so that connection stays plaintext and never touches the group's TLS material.

---

**Front:** Why a certificate renewal restarts your MySQL pods

**Back:** The per-site spec hash includes a SHA-256 of every key in the TLS Secret. cert-manager rewrites the Secret on renewal, the hash changes on every site, and the operator rolls the group — because a running mysqld does not pick up new certificate files by itself.

---

**Front:** What a certificate renewal costs you operationally

**Back:** An ordered update: standby first, a real failover, then the old active. Your primary moves, `bloodraven_failovers_total` increments, and `BloodravenFailoverOccurred` fires. `renewBefore` is therefore an operational setting as much as a security one.

---

**Front:** Why `encryptionAtRest` requires `spec.tls`

**Back:** A CEL rule, not advice: MySQL requires a secure connection to clone encrypted data regardless of any clause, and `CLONE INSTANCE` is how Bloodraven reseeds a diverged site. Without TLS, reclone on an encrypted group would be impossible, so the CRD refuses the combination up front.

---

**Front:** Legacy `secretName` mode with TLS on

**Back:** The DSN is used verbatim, so you add `?tls=true` to it yourself. The sidecar is the exception: it gets verified TLS automatically unless your DSN already sets `tls=`, in which case your choice wins.
