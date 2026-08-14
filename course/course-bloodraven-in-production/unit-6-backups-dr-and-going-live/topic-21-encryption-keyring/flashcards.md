# Flashcards — Encryption at rest and the keyring lifecycle

<!-- Rendered from course.json by course-template/tools/render-views.mjs.
     Edit course.json, then re-render. Edits here are overwritten. -->

**Front:** The keyring phases a site of `playground` can be in

**Back:** Five: `Pending`, `Unsealed`, `Escrowed`, `Sealed`, `Failed` — the CRD enum is `"";Pending;Unsealed;Escrowed;Sealed;Failed`.

---

**Front:** What `Sealed` means physically, not just as a status string

**Back:** The keyring data file is projected read-only from the site's escrow Secret, so mysqld cannot add a key.

---

**Front:** Oracle's own caveat on file-based keyring components

**Back:** `component_keyring_file` and `component_keyring_encrypted_file` are not intended as a regulatory compliance solution.

---

**Front:** The one edge in the keyring lifecycle that runs backwards

**Back:** Rotation: a site at `Sealed` whose name matches the rotation target re-enters `Unsealed`.

---

**Front:** A site of `playground` reads `phase: Unsealed`. What do you read next before concluding anything?

**Back:** `unsealReason` — `Bootstrap`, `Clone` or `Rotation` — plus the rotation target, because the phase string alone cannot tell you which.

---

**Front:** Where the escrowed keyring is stored

**Back:** In per-site versioned, immutable Kubernetes Secrets, owner-ref'd to the failover group.

---

**Front:** The digest annotation on an escrow Secret is authoritative — true or false, and why?

**Back:** False: it is informational. The operator always recomputes the digest from the Secret's contents rather than trusting the annotation.

---

**Front:** What you must turn on outside Bloodraven before enabling `spec.encryptionAtRest`

**Back:** API-server encryption at rest for Secrets (ideally KMS-backed, not `aescbc` with a local key file), plus restricted RBAC on Secrets in the group's namespace.

---

**Front:** The admission error you get from `encryptionAtRest.enabled: true` with no `spec.tls`

**Back:** The CEL rejection `spec.encryptionAtRest.enabled requires spec.tls: MySQL requires a secure connection to clone encrypted data`.

---

**Front:** Why a clone of encrypted data genuinely needs a secure connection

**Back:** MySQL upstream requires it when cloning encrypted data regardless of whether the `REQUIRE SSL` clause is specified — and `CLONE INSTANCE` is how Bloodraven reseeds a diverged site.

---

**Front:** Why rotation is refused on the active primary specifically

**Back:** Rotation runs with a writable keyring, the only window in which a keyring can be lost; on a replica that loss is recoverable by re-cloning from a healthy peer, on the primary it is not.

---

**Front:** `keyring_file` versus `component_keyring_file`

**Back:** The `keyring_file` plugin was removed in MySQL 8.4.0 along with `keyring_file_data`; `component_keyring_file` is the current component and what Bloodraven uses.
