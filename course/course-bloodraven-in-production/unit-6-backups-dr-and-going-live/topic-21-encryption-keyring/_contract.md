# Encryption at rest and the keyring lifecycle

**Unit:** 6 — Backups, disaster recovery, and going live
**Objectives (unit-numbered):**
7. Walk a site through the keyring phases and say which phase is the steady state   [obj 7]
8. Explain why enabling encryption at rest makes etcd part of your key custody   [obj 8]
9. Rotate a keyring without rotating the one site that must not be rotated   [obj 9]

## Topic generation prompt

Teach `spec.encryptionAtRest` on `orders` as a lifecycle, not a boolean. There are **five** phases, and the fifth is the one people forget: `Pending`, `Unsealed`, `Escrowed`, `Sealed`, `Failed`. Walk a site through them in order and name `Sealed` as the steady state — a site sitting in `Unsealed` is mid-flight, not done. Then the detail that makes rotation legible: rotation re-enters `Unsealed` **from** `Sealed`, so the same phase string means two different things depending on where the site came from, and the learner should read phase alongside the rotation target rather than alone.

Teach the escrow precisely, because it is where the security argument turns. The escrow lives in per-site **versioned** Kubernetes Secrets, owner-ref'd to the group, with the digest recomputed rather than trusted. Draw the conclusion the docs draw, and quote its bluntness: the live keyring is projected from a Kubernetes Secret, and Kubernetes stores Secrets unencrypted in etcd by default — so without API-server encryption at rest, enabling this feature does not protect your keys, it just moves them from the MySQL data disk to the control-plane disk. Then the sentence that should end the argument in any review: "None of these are optional. Bloodraven cannot verify them for you." This is a prerequisite you satisfy outside Bloodraven, and it is on you.

Then the hard constraint: `spec.encryptionAtRest.enabled` requires `spec.tls`, and it is a **CEL rejection** at admission, not advice in a doc. Show the message. Then explain why it is genuinely necessary rather than defensive — MySQL upstream states that a secure connection is required when cloning encrypted data regardless of any clause, and `CLONE INSTANCE` is exactly how Bloodraven reseeds a diverged site. Without TLS, reclone on an encrypted group would be impossible, so the CRD refuses the combination up front.

Then rotation, and the refusal that objective 9 turns on: the operator **refuses to rotate the active primary**. Rotation necessarily runs with a writable keyring, and that is the only window in which a keyring can be lost — so the site that must not be rotated is whichever one is currently active. Have the learner reason through the practical consequence for `orders`: rotate the replicas and the reader, then make the last remaining site rotatable by no longer being primary, via the planned failover they already know how to run from Unit 5.

Finish with upstream currency, because keyring advice ages badly. The current component is `component_keyring_file`; the `keyring_file` **plugin** was removed in MySQL 8.4.0 along with `keyring_file_data`, so any runbook or blog post naming the plugin is describing something that no longer exists. And close with Oracle's own caveat: file-based keyring components are not intended as a regulatory-compliance solution. If someone is enabling this to satisfy an auditor, that sentence belongs in the conversation.

Use a `flow` widget for the five-phase lifecycle including the rotation edge back from `Sealed` to `Unsealed` and the `Failed` exit. Do NOT cover metrics, alert rules, or `bloodraven_keyring_phase` as an alerting signal — topic 4 owns alerting.

## Requested activities

- READ: 800-1000 words. The five phases with `Sealed` named as steady state, the rotation re-entry edge, versioned owner-ref'd Secrets with recomputed digests, the etcd custody argument with the two quoted sentences, the TLS CEL rejection plus the upstream clone-encrypted-data requirement, the active-primary rotation refusal and how to work around it on `orders`, then `component_keyring_file` versus the removed 8.4.0 plugin and Oracle's compliance caveat. One `flow` widget on the phase lifecycle; at most one other widget.
- FLASHCARDS: the five phases, which is steady state, the rotation re-entry, per-site versioned Secrets, digest recomputed not trusted, etcd custody, the TLS CEL rule, why clone needs TLS, the active-primary rotation refusal, `component_keyring_file` vs removed `keyring_file`. 10-12 cards.
- QUIZ: 5 questions. Which phase means done; what a site in `Unsealed` after a rotation request is doing; what enabling encryption at rest without API-server encryption actually achieves; why `spec.tls` is mandatory rather than recommended; and which site in `orders` cannot be rotated right now and what to do about it.

## Handoff

**Inherits:** The learner can back up, verify, and restore `orders`.
**Leaves:** The learner can enable encryption at rest with its real prerequisites, read a site's keyring phase, and rotate every site in `orders` in a safe order.
**Do not cover:** Metric names, alert rules or runbooks (topic 4), cross-cluster DR and the go-live gate (topic 5).
