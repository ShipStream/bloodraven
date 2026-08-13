# The old primary comes back

`playground` is serving from `pdx`. The counter application is reading and writing again, and `iad` has sat at zero
replicas since you took it down. Bring it back:

```bash
kubectl -n bloodraven-playground scale deployment/mysql-playground-iad --replicas=1
```

Everything that follows is decided by exactly one test. You should be able to call the outcome before
the pod is Ready.

## One test, and its direction

On every poll the operator looks for a site that is read-only with no active replication — the
signature of a former primary — while another site is the directly confirmed writable primary. When
it finds one it asks a single question: **does the new primary's GTID set contain the old primary's?**

If it does, there is nothing on `iad` that `pdx` has not already got. The operator logs
`no GTID divergence, auto-recovering old primary as replica` and rejoins the site itself. If it does
not, it computes `divergent := oldGtid.Subtract(newGtid)`, counts the transactions in that difference,
writes both to `status.sites[].divergentGtid` and `status.sites[].divergentTransactionCount`, sets the
`bloodraven_divergent_transactions` gauge, and stops.

Get the direction right or you will predict every outcome backwards. It is **new ⊇ old**. The reverse
test goes false the moment `pdx` accepts its first counter write after promotion, so it would block
every rejoin that ought to be automatic — and it passes precisely when the old primary is *ahead*, the
one case that must never proceed.

## Fence first, then re-read

The set being compared is not the one read on arrival. The operator applies a defensive
`SET GLOBAL super_read_only = ON` first, and only then re-reads `@@global.gtid_executed`. The code says
why: *"Re-read after the fence: this is the authoritative set the divergence comparison runs against
(the fence guarantees it can no longer grow)."*

A returning mysqld is not quiet. Compare two GTID sets while one is still advancing and your answer
describes a moment already gone. The fence turns a moving target into a fact.

## What the gate is not

Recovery is deliberately **not** gated on `lastFailoverTarget`, because a primary can change hands with
no failover recorded at all: a replica may respawn writable and be adopted as the de-facto primary while
the old primary comes back fenced, or the record may be lost to a status-write outage plus an operator
restart. The orphaned ex-primary still needs to rejoin, and history cannot tell you so.

What gates it instead is a *unique, directly confirmed, promotable writable primary observed right now* —
the stronger condition, because it is present-tense. A recorded target is a claim about the past that may
have been superseded, lost or hand-edited; a confirmed writable primary is a fact just proved over a
connection.

## The rejoin, statement by statement

When containment holds, the operator runs five statements against `iad`. You will read them in this
order in the logs, and each is there for a reason.

```widget
{
  "type": "order",
  "title": "Auto-rejoin: the five statements against the returning site",
  "items": [
    "SET GLOBAL super_read_only = ON — Defensive. The sidecar may already have fenced the site; this makes it certain before anything is mutated.",
    "STOP REPLICA — MySQL requires both the receiver and the applier thread to be stopped before a CHANGE REPLICATION SOURCE TO that uses SOURCE_AUTO_POSITION = 1.",
    "RESET REPLICA ALL — Clears stale applier metadata from the site's life as a primary — and removes all connection parameters with it.",
    "CHANGE REPLICATION SOURCE TO ... SOURCE_AUTO_POSITION=1 — Must follow RESET REPLICA ALL: the instance cannot be a replica again until the connection parameters are re-supplied. GET_SOURCE_PUBLIC_KEY=1 is added when TLS is off.",
    "START REPLICA — The site attaches to pdx and asks for every GTID it is missing."
  ]
}
```

`SOURCE_AUTO_POSITION=1` is always used — the replica hands over its executed set and the source sends
whatever is missing. Steps 2 and 3 are not decoration: the stopped-threads rule forces `STOP REPLICA` in
front, and `RESET REPLICA ALL` removing all connection parameters forces `CHANGE REPLICATION SOURCE TO`
behind it.

`RecoveryInProgress` is written to status **before** the sequence runs — the durable handoff for an
operator restart landing in the middle of STOP/RESET/CHANGE/START, so a restarted operator finds a
marker rather than a half-configured site. While it runs, the `RecoveryPending` condition is `True`
with reason `RecoveryInProgress`. When divergence blocks instead, the same condition is `True` with
reason `DivergentTransactions`, and its message names the count and the exact annotation to apply.
Blocked reports are re-verified every 30 s, so a site that diverges further gets a refreshed set rather
than a stale under-count, and divergence you resolve externally is noticed on the next pass.

## The empty-datadir trap

A genuinely fresh datadir must be cloned, not recovered. But "empty" is decided from **shared GTID
UUIDs** — whether the site's executed set shares any UUID with the new primary's history — not from the
absence of user schemas. That distinction is a shipped bug fix. A cluster legitimately has no user
schemas before its first application write, so the schema-only test sent a returning old primary
carrying every cluster UUID down the auto-clone path: a *diverged* site cloned over instead of reported
as `RecoveryBlocked`. A returning member always shares the cluster's UUIDs; a fresh datadir never does.
Emptiness tests in database tooling must be about **history**, not content.

## The reclone interlock

Here is the measurement, from the reference run. Roles ran the other way there — `pdx` died and
returned — so read `site` as `iad`. `fg` and `time` elided:

```json
{"level":"WARN","msg":"divergence detected","site":"pdx","divergentTransactions":3,
 "divergentGtid":"f52a03db-45e5-11f1-944b-a6b4a989ea09:1-3",
 "oldPrimaryGtid":"f4d07a53-45e5-11f1-8706-dabe2399558e:1-15, f52a03db-45e5-11f1-944b-a6b4a989ea09:1-3",
 "newPrimaryGtid":"f4d07a53-45e5-11f1-8706-dabe2399558e:1-15"}
```

Three transactions under the returning site's own UUID that `pdx` never saw. Read the set off the CR and
copy a prefix:

```bash
NS=bloodraven-playground; FG=playground; SITE=iad
DG=$(kubectl -n $NS get mfg $FG -o jsonpath="{.status.sites[?(@.name==\"$SITE\")].divergentGtid}")
kubectl -n $NS annotate mfg $FG bloodraven.shipstream.io/reclone-site="$SITE:${DG:0:8}" --overwrite
```

```widget
{"type":"anatomy","title":"bloodraven.shipstream.io/reclone-site=iad:3E11FA47","parts":[
 {"text":"bloodraven.shipstream.io/reclone-site","label":"annotation key","note":"Applied to the MysqlFailoverGroup, not to a site object. Rejected values are deleted from the CR."},
 {"text":"=","label":"","note":"kubectl annotate syntax."},
 {"text":"iad","label":"site name","note":"Must match an entry in spec.sites[].name. Bare, with no ':' suffix, this is the cold form — accepted only when no divergentGtid is recorded."},
 {"text":":","label":"separator","note":"Everything after the first colon is the confirmation token."},
 {"text":"3E11FA47","label":"divergentGtid prefix","note":"Hot reclone: at least 8 characters, and a true prefix of the observed status.sites[].divergentGtid. The cold form puts confirm=playground here instead."}]}
```

Four teeth. A hot reclone — the site has a recorded `divergentGtid` — needs a prefix of at least 8
characters matching that set; a mismatch is rejected. A cold reclone, where nothing is recorded, still
wipes the datadir, so it demands a literal confirm token equal to the failover-group name:
`iad:confirm=playground`. The interlock keys on **`divergentGtid` presence only**, never on `RecoveryState` —
`RecoveryBlocked` is a downstream UX field that can be transiently unset during a reconcile, so clearing
it by hand changes nothing. And a rejected annotation emits a `RecloneRejected` warning event and is
then **deleted**, so it cannot spam the reconciler. Two real rejections, verbatim:

```text
reclone of "pdx" rejected: site has divergent transactions (gtid "589f4b67-4d00-11f1-9df5-7e14c29a8121:104-106");
  annotation value must include the divergent-GTID prefix to confirm intent — set annotation
  bloodraven.shipstream.io/reclone-site=pdx:589f4b67

reclone of "pdx" rejected: divergent-GTID prefix "deadbeef" does not match the observed divergentGtid
  "589f4b67-4d00-11f1-9df5-7e14c29a8121:104-106" — double-check the site name and re-read
  status.sites[].divergentGtid
```

The message tells you exactly what to type. Reclone also refuses the active primary as a recipient and
requires a confirmed writable donor, so a fat-fingered site name cannot destroy the site you are
running on.

## Two exits, and only two

| Exit | Mechanism | Cost | Right when |
| --- | --- | --- | --- |
| Reclone `iad` from `pdx` | `CLONE INSTANCE` wipes and rebuilds the datadir; divergent transactions are discarded | Time and bytes — a full copy | The divergent set is expendable, or already extracted |
| Replay the set onto `pdx` | You apply the missing transactions, so `pdx`'s set comes to contain `iad`'s | Cheap — but you must have actually extracted them | Those transactions matter enough to introduce into the surviving timeline |
| Hand-repoint `iad` | Not sanctioned | — | Never |

Replay is the elegant exit: once `pdx` ⊇ `iad`, containment passes on the next 30 s re-verification and
the normal rejoin proceeds with no annotation at all. Reclone is the blunt one, and usually the right
one.

Hand-repointing is not a third option. `SOURCE_AUTO_POSITION=1` will ask `pdx` for transactions it never
had, and the position-based workarounds that appear to fix that — pinning a binlog file and offset,
resetting the errant set — work by discarding the correctness the GTID model is providing. If
replication "starts working" after that, you have not fixed divergence; you have stopped being able to
detect it.

One warning generalises the whole topic: **even a clean pod kill can produce divergence.** The capture
above is from a clean primary kill. MySQL commits internal transactions — system-table updates,
counters — when it respawns, under the returning site's own UUID, before the operator can fence it.
Whether it happens is timing-dependent. A populated `divergentGtid` is not evidence that anybody did
anything wrong.

## Where this leaves you

`playground` is whole: `pdx` primary, `iad` back as a replica, nothing divergent reported anywhere. And
nothing in this topic told the counter application any of it. Its reads went on succeeding against
whatever socket it happened to hold; its writes failed for as long as that socket pointed at a fenced
site, and started working again for reasons it never learned. The promotion was correct, fast and
complete, and the one party that mattered was never in the conversation. Unit 4 takes that up:
Services, DNS, connection pools, and what an application has to do to follow a primary that moved.
