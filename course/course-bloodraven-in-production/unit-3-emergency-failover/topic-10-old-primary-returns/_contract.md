# The old primary comes back

**Unit:** 3 — Emergency failover, end to end
**Objectives (unit-numbered):**
7. Predict whether a returning primary rejoins automatically or lands in `RecoveryBlocked`   [obj 7]
8. Run the reclone interlock with the `divergentGtid` confirmation token   [obj 8]
9. Choose between recloning and replaying the divergent set onto the new primary   [obj 9]

## Topic generation prompt

Scale `iad` back up. What happens next is decided by exactly one test, and the learner should be able to predict the outcome before the pod is Ready.

**The containment test.** There is no divergence if and only if the **new** primary's GTID set **contains** the old primary's. If it does, the operator logs that there is no GTID divergence and auto-recovers the old primary as a replica. If it does not, the operator computes the set difference — old minus new — and its transaction count, writes them to `status.sites[].divergentGtid`, sets the `bloodraven_divergent_transactions` gauge, and stops.

**Teach the ordering that makes the test sound.** The comparison does not run against a GTID read taken on arrival. The operator applies a defensive fence first, then **re-reads** the GTID set, and that post-fence read is the authoritative set the comparison runs against — because the fence guarantees the set can no longer grow underneath the comparison. A learner who has ever tried to compare two moving GTID sets by hand will recognise why this matters.

**And teach what the gate is not.** Recovery is deliberately **not** gated on `lastFailoverTarget`. Safety comes from a directly confirmed unique writable primary, observed now, rather than from history. Say why that is the stronger condition.

**The auto-rejoin path**, when containment holds — give the SQL in order, because the learner will read these statements in the logs: `SET GLOBAL super_read_only = ON`, `STOP REPLICA`, `RESET REPLICA ALL`, `CHANGE REPLICATION SOURCE TO` (always with `SOURCE_AUTO_POSITION=1`, plus `GET_SOURCE_PUBLIC_KEY=1` when TLS is off), `START REPLICA`. Two upstream facts make the sequence legible rather than arbitrary: both replication threads must be stopped before a `CHANGE REPLICATION SOURCE TO` that uses `SOURCE_AUTO_POSITION = 1`, and `RESET REPLICA ALL` removes all connection parameters, which is precisely why a `CHANGE REPLICATION SOURCE TO` must follow it. Then the restart-safety detail: `RecoveryInProgress` is persisted **before** the mutation runs, as the durable handoff for an operator restart landing in the middle of the STOP/RESET/CHANGE/START sequence. Re-verification runs on a 30 s cadence. Status conditions: `RecoveryInProgress` carries reason `RecoveryInProgress`; the blocked state carries reason `DivergentTransactions`, with a message naming the divergent count and the exact reclone annotation to apply.

**The empty-datadir trap**, which is a real shipped bug worth the space. Whether a site is treated as empty is decided from **shared GTID UUIDs**, not from the absence of user schemas. The reason is concrete: a cluster legitimately has no user schemas before its first application write, so a schema-only emptiness test would have cloned straight over a diverged old primary instead of reporting `RecoveryBlocked`. Use it to make the general point that emptiness tests in database tooling must be about history, not about content.

**The reclone interlock**, in full, because objective 8 is an execution objective. The annotation key is `bloodraven.shipstream.io/reclone-site`. Two accepted value forms: `<siteName>` and `<siteName>:<divergentGtidPrefix>`. A hot reclone requires a prefix of at least 8 characters that matches the observed `status.sites[].divergentGtid`; a mismatch is rejected. A cold reclone wipes the datadir and requires a literal confirm token equal to the failover-group name — `<siteName>:confirm=<groupName>` — and the rejection message tells you exactly what to type. The interlock keys on **`divergentGtid` presence only**, not on `RecoveryState`, because `RecoveryBlocked` is a downstream UX field that can be transiently unset during a reconcile. A rejected annotation emits a `RecloneRejected` warning event and is **deleted**, so a bad annotation cannot spam the reconciler. Reclone also refuses the active primary as a recipient and requires a confirmed writable donor. Have the learner read the `divergentGtid` off `orders`, copy a prefix, and run the hot reclone.

**The two sanctioned exits, and only two.** Either reclone the old primary from the new one, or replay the divergent set onto the new primary so that the new primary's GTID set comes to contain the old one's — at which point the containment test passes on the next 30 s re-verification and rejoin proceeds normally. Never hand-repoint replication around a divergence: `SOURCE_AUTO_POSITION=1` will ask the new primary for transactions it never had, and the position-based workarounds that appear to fix it destroy the correctness the GTID model is providing. Frame the choice as an operational one: reclone is simple, safe and expensive in time and bytes; replay is fast and cheap but requires you to have actually extracted the divergent transactions and to be willing to introduce them into the surviving timeline.

**One closing warning that generalises the whole topic:** even a clean pod kill can produce divergence, because MySQL commits internal transactions when it respawns, before the operator has any chance to fence it. Divergence is not evidence that someone did something wrong.

Do NOT cover split brain (Unit 5) — divergence here is one-sided and historical, not two live writable sites.

## Requested activities

- READ: 1000-1200 words. The containment test and the fence-then-re-read ordering; the not-gated-on-`lastFailoverTarget` design; the rejoin SQL in order with the two upstream constraints; `RecoveryInProgress` as restart handoff and the 30 s cadence; conditions and reasons; the empty-datadir trap; the full reclone interlock; the two sanctioned exits and the never-hand-repoint rule; the clean-kill-can-diverge warning. Use one `anatomy` widget on the annotation string `bloodraven.shipstream.io/reclone-site=iad:3E11FA47` breaking it into key, site name and GTID prefix, with the cold form shown alongside. Optionally one `order` widget on the five rejoin statements.
- FLASHCARDS: The containment direction (new contains old); fence-then-re-read; the five rejoin statements in order; `SOURCE_AUTO_POSITION=1`; `RecoveryInProgress` versus `RecoveryBlocked` and their condition reasons; 30 s re-verification; the annotation key and both value forms; the 8-character prefix floor; the cold `confirm=<group>` token; the interlock keying on `divergentGtid` not `RecoveryState`; shared GTID UUIDs as the emptiness test. 10-12 cards.
- QUIZ: 5 questions discriminating: given two GTID sets, whether the site auto-rejoins or blocks (test the containment direction — the inverted comparison is the distractor); what a 6-character prefix in the annotation does; what a cold reclone requires beyond the site name; whether clearing `RecoveryState` by hand unblocks a reclone (no — the interlock reads `divergentGtid`); and which of reclone, replay, and hand-repointing replication are sanctioned exits.

## Handoff

**Inherits:** The learner has run an emergency failover and measured its transaction cost.
**Leaves:** `orders` is whole again — `pdx` primary, `iad` rejoined as a replica — and the learner can predict, unblock and audit any old-primary return. The unit's open problem stands: the promotion was correct, fast and complete, and the application never noticed, because it is still reading through a socket it opened to `iad` before the failover. Unit 4 takes that up as its subject — Services, DNS, connection pools, and what an application has to do to follow a primary that moved.
**Do not cover:** Split brain and two live writable sites (Unit 5); Service selectors and DNS steering mechanics (Unit 4); backup, PITR or restore (Unit 6).
