# Encryption at rest and the keyring lifecycle

Restoring `playground` proved you can get the data back. Your security review asks the other question: what happens when someone else gets it? Today the `playground` PVCs hold InnoDB tablespaces, redo, undo and binlogs in the clear on three k3d worker nodes. So you add `spec.encryptionAtRest.enabled: true` and apply — and the API server rejects the object before the operator ever sees it. That rejection is the first honest thing this feature tells you.

## A lifecycle, not a flag

`spec.encryptionAtRest.enabled` is a boolean. What it starts is not. Each site in `playground` — `iad`, `pdx` and the `reader` — walks its own keyring through a phase machine, and the CRD enum names **five** values (objective 7):

```go
// +kubebuilder:validation:Enum="";Pending;Unsealed;Escrowed;Sealed;Failed
type SiteKeyringPhase string
```

```figure
{
  "src": "assets/img/g10-keyring-phases.svg",
  "alt": "Keyring phases in a line: Pending, Unsealed, Escrowed, Sealed marked as the steady state. Failed drops off. A back-arrow from Sealed to Unsealed is labelled Rotation. A callout says never rotate the current primary.",
  "caption": "Sealed is done. Unsealed is mid-flight — including a rotation that opened a sealed site on purpose.",
  "width": 960,
  "height": 320
}
```

```widget
{
  "type": "flow",
  "title": "Keyring phases for one site of playground",
  "steps": [
    {
      "label": "Pending",
      "detail": "No escrowed keyring yet. The Deployment renders unsealed with an empty memory keyring so MySQL can create its master keys."
    },
    {
      "label": "Unsealed",
      "detail": "Deliberately running a writable memory-backed keyring: bootstrap, a CLONE INSTANCE into this site, or a rotation. Not protected yet."
    },
    {
      "label": "Escrowed",
      "detail": "The live keyring is captured into an immutable per-site Secret and the operator has verified the digest. The Deployment is rolling onto the sealed rendering."
    },
    {
      "label": "Sealed — steady state",
      "detail": "Keyring projected read-only from the escrow Secret; mysqld cannot add keys. A rotation request on a Sealed site is the one edge that runs backwards: Sealed -> Unsealed with unsealReason=Rotation."
    },
    {
      "label": "Failed (exit, any phase)",
      "detail": "Escrow timed out, the escrow Secret is missing, or the live digest does not match escrow. The operator refuses to declare the site sealed."
    }
  ]
}
```

`Sealed` is the steady state and the only phase that means done. It means the keyring data file is projected read-only from that site's escrow Secret, so mysqld physically cannot add a key. `Unsealed` means the opposite — the keyring lives on a memory-backed volume MySQL can write, because something has to create a key. A site sitting in `Unsealed` is mid-flight. Reading the group as "encryption is on" because the flag says `true` is exactly the mistake.

Rotation is what makes the phase unreadable on its own. Rotation re-enters `Unsealed` **from** `Sealed`:

```go
if site.Phase == v1alpha1.KeyringPhaseSealed && rotateTarget == site.Name {
```

One string, two meanings: a site that has never been protected, and a protected site opened deliberately to mint a new master key. Read `phase` beside `unsealReason` and the rotation target — never alone.

## The escrow, and who else now holds your keys

The escrow lives in per-site **versioned** Kubernetes Secrets, owner-ref'd to the group, immutable once written, with the digest **recomputed from the Secret's contents rather than trusted** from its annotation. That is careful engineering. It is also where the security argument turns, and the docs do not soften it (objective 8):

> The live keyring is projected from a Kubernetes Secret. **Kubernetes stores Secrets unencrypted in etcd by default.** Without API-server encryption at rest, enabling this feature does not protect your keys — it just moves them from the MySQL data disk to the control-plane disk.

So before you enable this you turn on API-server encryption at rest for Secrets (KMS-backed, not `aescbc` with a local key file), restrict RBAC on Secrets in the namespace, and deal with swap on the worker nodes. Then the sentence that should end the argument in any review:

> None of these are optional. Bloodraven cannot verify them for you.

These are prerequisites you satisfy outside Bloodraven. Nothing in the operator checks them, and no status field will ever tell you that you skipped them.

## Why the API server refused you

One prerequisite *is* enforced, as a CEL rule on the CRD — a hard admission rejection, not advice in a doc:

```
spec.encryptionAtRest.enabled requires spec.tls: MySQL requires a secure
connection to clone encrypted data
```

That is not defensive coding. Oracle's clone documentation states plainly that "a secure connection is required when cloning encrypted data regardless of whether this clause is specified" — the `REQUIRE SSL` clause does not enter into it. And `CLONE INSTANCE` is precisely how Bloodraven reseeds a diverged site after a reclone. On an encrypted group without TLS, reclone would simply be impossible, so the CRD refuses the combination up front rather than letting you discover it at 3am with a `RecoveryBlocked` site. Add `spec.tls` and the object is admitted.

## Rotating every site, including the one you cannot rotate

Now the constraint objective 9 turns on: **the operator refuses to rotate the active primary.** The comment says why.

> The operator refuses to rotate the active primary. Rotation necessarily runs with a writable keyring, and the only window in which a keyring can be lost is that one.

Lose a replica's keyring in that window and you re-clone the site from a healthy peer. Lose the primary's and you have lost data. So the site you cannot rotate is not a fixed name — it is whichever site is currently active. On `playground` right now that is `iad`.

Work through the consequence for the three sites. `pdx` is a replica and rotatable. The `reader` is `role: read-only` and never a primary, so it is rotatable too. `iad` becomes rotatable by no longer being primary — via the planned failover you already run from Unit 4, at RPO 0 by construction.

```widget
{
  "type": "order",
  "title": "Rotating all three sites of playground",
  "items": [
    "1. Rotate pdx — kubectl annotate mysqlfailovergroup playground bloodraven.shipstream.io/rotate-keyring=pdx --overwrite — wait for Sealed.",
    "2. Rotate reader — Same annotation, target reader. One site at a time; each rotation mints a new immutable escrow Secret version.",
    "3. Planned failover to pdx — kubectl annotate mysqlfailovergroup playground bloodraven.shipstream.io/planned-failover=pdx — iad is now a replica.",
    "4. Rotate iad — Now permitted. Rotation is also refused while an ordered update or a planned failover is in flight, and while no active primary is known."
  ]
}
```

Rotation is a per-instance physical operation — each site holds its own keyring — so 1.0.0 issues
`ALTER INSTANCE ROTATE INNODB MASTER KEY` with `sql_log_bin = 0`. If that statement replicated, the
replica would be one transaction ahead of its source and the next promotion would brick the
ex-primary. Recloning an encrypted site unseals the recipient, holds it unsealed until
`CLONE INSTANCE` and the post-clone restart finish, then reseals.

## What you can now read

Where you previously read `.status.sites[]` for replication state, you now read one more block:

```bash
kubectl get mysqlfailovergroup playground \
  -o jsonpath='{range .status.encryptionAtRest.sites[*]}{.name}{"\t"}{.phase}{"\t"}{.message}{"\n"}{end}'
```

Three sites at `Sealed`, each `sealed against mysql-playground-<site>-keyring-v1`, with `status.encryptionAtRest.sealed: true` — that is the finished state. During step 1 above, `pdx` reads `Unsealed` with `unsealReason: Rotation`, and comes back as `v2`. The phase string is identical to the one it showed at bootstrap; only the reason and the version distinguish them.

One last currency check, because keyring advice ages badly. The current component is `component_keyring_file`, verified against the default `mysql:9.7` image. The `keyring_file` **plugin** was removed in MySQL 8.4.0 along with the `keyring_file_data` system variable — any runbook or blog post naming the plugin is describing something that no longer exists. And Oracle's own caveat belongs in the conversation if someone is enabling this for an auditor: `component_keyring_file` and `component_keyring_encrypted_file` "are not intended as a regulatory compliance solution".

## Handoff

You can enable encryption at rest on `playground` with its real prerequisites — TLS, because the CRD makes you; API-server encryption and Secret RBAC, because nothing will make you — read any site's keyring phase and know whether it means protected or mid-flight, and rotate all three sites in an order that never opens the primary's keyring. What none of this tells you is when a site quietly stops being sealed at 3am. That is a signal, and signals need somewhere to go.
