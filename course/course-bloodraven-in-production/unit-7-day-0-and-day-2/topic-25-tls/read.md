# TLS, and what it unlocks

Unit 6 walked you into a wall and then walked around it. You set
`spec.encryptionAtRest.enabled: true`, the API server refused the object, and the rejection named a
field the course had never taught:

```
spec.encryptionAtRest.enabled requires spec.tls: MySQL requires a secure
connection to clone encrypted data
```

This topic is that field. It is two lines of YAML with a surprising amount hanging off them: a
certificate you must design, a rolling restart you did not ask for, and a change to how every
component in the group talks to MySQL.

## Two lines, and what has to exist behind them

```yaml
spec:
  tls:
    secretName: ledger-mysql-tls        # a Secret with ca.crt, tls.crt, tls.key
    issuerRef:
      name: mysql-ca                    # a cert-manager Issuer or ClusterIssuer
      kind: ClusterIssuer               # enum: Issuer | ClusterIssuer
```

Note what the operator does and does not do with `issuerRef`. It records which issuer *should* be
producing the material. **It does not create the `Certificate` for you.** Bloodraven's non-goals list
said it does not replace cert-manager, and this is the exact place that bites: set `spec.tls`, forget
the `Certificate`, and the Secret never appears. The operator logs
`TLS secret not found yet, skipping TLS hash` and carries on rendering the Deployment with a volume
that mounts a Secret that does not exist — so the pods stay `ContainerCreating` and nothing tells you
why in the CR.

The Secret needs exactly three keys: `ca.crt`, `tls.crt`, `tls.key`. A missing `ca.crt` is a hard
error on the operator's own client path — `TLS secret … missing required ca.crt` — and supplying one
of `tls.crt`/`tls.key` without the other is refused rather than half-applied.

## The certificate is a design decision, not a formality

This is where teams get it wrong, and the failure is severe enough to be worth doing carefully. **The
SAN list is the whole exercise**, because at least four different clients dial MySQL by four different
names and every one of them verifies.

```widget
{
  "type": "anatomy",
  "title": "Who dials MySQL, and by which name",
  "parts": [
    { "text": "mysql-<group>-primary", "label": "your application, writes", "note": "The group write endpoint. Your own clients verify against this name." },
    { "text": "mysql-<group>-replicas", "label": "your application, reads", "note": "The group read endpoint. Same reasoning." },
    { "text": "mysql-<group>-<site>", "label": "the sidecar", "note": "The one people forget. The sidecar connects over loopback, and 127.0.0.1 appears in no certificate, so it verifies against its own site's Service name instead." },
    { "text": "mysql-<group>-<site>-internal", "label": "replication and the operator", "note": "The canonical replication source host from Unit 1, and what the operator's own probes and the backup/restore Jobs dial." }
  ]
}
```

A `Certificate` that covers the group endpoints and misses the per-site names looks fine until you
watch the pods:

> the sidecar cannot query MySQL at all — `/health` returns 503 and the liveness probe restarts the
> container, which also stops the self-fencing monitor and the `super_read_only` safety net.

Read that consequence against Unit 5. A missing SAN does not merely break a health check: it puts the
sidecar into a crash loop, and a crash-looping sidecar is a site with **no self-fencing and no startup
safety net**. You have removed the layer that protects correctness when the operator cannot be
reached, by getting a DNS name wrong in a certificate.

So enumerate the names, all of them, per site:

```yaml
apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: ledger-mysql-tls
  namespace: ledger-db
spec:
  secretName: ledger-mysql-tls          # must equal spec.tls.secretName
  duration: 2160h                       # 90 days
  renewBefore: 360h                     # 15 days — see the next section before you shorten this
  issuerRef:
    name: mysql-ca
    kind: ClusterIssuer
  commonName: ledger-db.example.com
  dnsNames:
    - ledger-db.example.com                                       # the DNSEndpoint hostname
    - mysql-ledger-primary.ledger-db.svc.cluster.local            # write endpoint
    - mysql-ledger-replicas.ledger-db.svc.cluster.local           # read endpoint
    - mysql-ledger-iad.ledger-db.svc.cluster.local                # per-site: the sidecar verifies this
    - mysql-ledger-pdx.ledger-db.svc.cluster.local
    - mysql-ledger-reader.ledger-db.svc.cluster.local
    - mysql-ledger-iad-internal.ledger-db.svc.cluster.local       # replication source host
    - mysql-ledger-pdx-internal.ledger-db.svc.cluster.local
    - mysql-ledger-reader-internal.ledger-db.svc.cluster.local
```

Two sites plus a reader is nine names. Adding a site means editing this list, and nothing will remind
you. That is a genuine operational cost of TLS on this operator, and it belongs in your runbook.

## What turning it on actually changes

Four things, and the third is the one that surprises people.

**One: mysqld is started with a TLS contract on the command line.** The operator mounts the Secret at
`/etc/mysql/tls` and appends `--ssl-ca`, `--ssl-cert`, `--ssl-key` and — this is the important one —
`--require-secure-transport=ON`. Plaintext connections are refused server-side. There is no partial
mode and no fallback to mysqld's auto-generated `server-cert.pem`; the fallback is deliberately
prevented, because a sidecar verifying a self-signed auto-cert would fail its first health check and
crash-loop.

**Two: the sidecar is handed the client half.** The same Secret is mounted into the sidecar container,
and four environment variables tell it where to look and what name to verify:
`BLOODRAVEN_MYSQL_TLS_CA_FILE`, `…_CERT_FILE`, `…_KEY_FILE`, and `…_SERVER_NAME` — the last set to
that site's own Service DNS name, for the loopback reason above.

**Three: replication changes shape.** `CHANGE REPLICATION SOURCE TO` gains `SOURCE_SSL=1` when TLS is
on. When it is off, it gains something else instead — `GET_SOURCE_PUBLIC_KEY=1` — and the comment in
the source explains why that is not optional either:

> MySQL 8's default `caching_sha2_password` authentication needs the source's RSA public key for
> non-TLS replication channels. Without this, `START REPLICA` succeeds but the IO thread exits
> asynchronously, leaving the site permanently not-replicating.

Carry that as a diagnostic. A `START REPLICA` that returns cleanly and then quietly stops replicating,
on a group with no TLS, is an authentication-material problem, not a network one — and it is the
shape of failure you would create by hand-running `CHANGE REPLICATION SOURCE TO` without either
clause.

**Four: backup and restore Jobs inherit it.** They get the same Secret at `/etc/mysql/tls` and connect
with `ssl-mode=VERIFY_CA` — both the `mysqlsh` dump session and the `mysqlbinlog | mysql` PITR replay.
A TLS-enabled Job **fails before connecting** if the CA path is empty, missing, unreadable or not
usable CA material. It never downgrades to unverified TLS. Backup verification is the deliberate
exception: the ephemeral verify instance from Unit 6 listens on loopback with no certificate and no
Service, so that connection stays plaintext and never touches the group's TLS material.

If you are still on the legacy `spec.secretName` mode, one manual step falls to you: the DSN in that
Secret is used verbatim, so add `?tls=true` to it yourself. The sidecar is the exception — it gets
verified TLS automatically unless your DSN already sets `tls=`, in which case your choice wins.

## Certificate renewal is a rolling restart

Here is the day-2 fact worth knowing before your first renewal rather than during it.

The operator computes a spec hash per site and stores it on the Deployment; a difference is the drift
signal that triggers an ordered update. **That hash includes a SHA-256 of every key in the TLS
Secret.** The comment is one line: *include TLS certificate data so cert rotation triggers a rolling
update.*

So when cert-manager renews your certificate, it rewrites the Secret, the hash changes on every site,
and the operator rolls the group. That is correct — a running mysqld does not pick up new certificate
files by itself — but draw the consequences out:

- **A renewal is a planned outage-shaped event you did not plan.** Under `OrderedUpdate` it is the
  next topic's sequence: standby first, then a failover, then the old active. Your primary moves.
- **It records a failover.** The ordered-update handoff calls `recordFailover` and increments
  `bloodraven_failovers_total`, exactly as Unit 2 warned. A `BloodravenFailoverOccurred` alert will
  fire on the day your certificate renews, and your on-call will go looking for a dead site that never
  existed.
- **So `renewBefore` is an operational setting, not a security one.** A short `renewBefore` on a short
  `duration` means frequent primary moves. Pick a renewal cadence you would be happy to be paged for,
  and put the expected renewal window in the same calendar as your change freezes.

The same mechanism covers credential Secrets, for the same reason and with the same consequence: the
hash includes them too, so rotating a password rolls the pods.

## And now the thing it unlocks

With `spec.tls` present, the object the API server refused in Unit 6 is admitted, and
`spec.encryptionAtRest.enabled: true` starts the keyring lifecycle you already know how to read. The
CEL rule is not defensive coding — Oracle's own clone documentation states that a secure connection is
required when cloning encrypted data regardless of any clause, and `CLONE INSTANCE` is exactly how
Bloodraven reseeds a diverged site. Without TLS, reclone on an encrypted group would be impossible, so
the CRD refuses the combination up front rather than letting you find out at 3am with a
`RecoveryBlocked` site and no way to fix it.

Which makes the ordering for a new group explicit, and it is not the order most people try:

```widget
{
  "type": "order",
  "title": "Bringing up an encrypted group, in the only order that works",
  "items": [
    "1. cert-manager Issuer or ClusterIssuer — exists before anything references it.",
    "2. Certificate with every SAN — group endpoints, per-site Services, per-site internal Services. Wait for the Secret to appear.",
    "3. MysqlFailoverGroup with spec.tls — admitted now; the pods come up with require_secure_transport=ON and the sidecars verify against their own site Service names.",
    "4. Confirm all three sites Ready and one writable — a TLS mistake shows up here as a crash-looping sidecar, and you want to meet it before encryption is in the picture.",
    "5. spec.encryptionAtRest.enabled: true — the CEL rule now passes, and each site walks Pending -> Unsealed -> Escrowed -> Sealed."
  ]
}
```

Step 4 is the one people skip. Turning on TLS and encryption in the same `kubectl apply` means a
crash-looping sidecar and a keyring stuck in `Unsealed` arrive together, and you get to guess which
caused which.

## Where this leaves you

You can wire `spec.tls` to a cert-manager issuer, design a SAN list that keeps every client — including
the sidecar on loopback — able to verify, and say precisely what a missing per-site SAN costs you.
You can explain why replication gains `SOURCE_SSL=1` with TLS and `GET_SOURCE_PUBLIC_KEY=1` without
it, and recognise the silent IO-thread exit that follows from having neither. And you know that your
next certificate renewal will move your primary and fire a failover alert.

That renewal is a rolling update, and rolling updates are the last piece of day 2 this course has not
taken apart.
