# Quiz — The old primary comes back

<!-- Rendered from course.json by course-template/tools/render-views.mjs.
     Edit course.json, then re-render. Edits here are overwritten. -->

## Question 1

**Type:** MULTIPLE_CHOICE

`pdx` is primary on `playground` with `GTID_EXECUTED` = `A:1-15`. You scale `iad` back up and it comes back read-only with `A:1-15,B:1-3`. What does the operator do?

- Auto-rejoins `iad` as a replica: `iad`'s set contains everything `pdx` has, so nothing is missing.
- Blocks `iad` with `RecoveryBlocked`, `divergentGtid = B:1-3`, and a divergent transaction count of 3.
- Auto-rejoins `iad` after the next 30 s re-verification, because `B:1-3` are internal transactions from the respawn and the containment test ignores those.
- Treats `iad` as a fresh datadir and clones it from `pdx`, because `B` is a UUID `pdx` has never seen.

**Correct option index:** 1

**Explanation:**

The test is one-directional: does the NEW primary's set contain the OLD primary's? `pdx` holds `A:1-15`, which does not contain `B:1-3`, so the operator computes old minus new, writes `B:1-3` and the count 3 to `status.sites[].divergentGtid` and `divergentTransactionCount`, sets `bloodraven_divergent_transactions`, and stops. The “`iad` contains everything `pdx` has” answer runs the comparison backwards — that inversion is false after almost any failover, because the new primary keeps taking writes, and it passes precisely when the old primary is ahead, the one case that must block. The “internal transactions are ignored” answer invents an exemption that does not exist: the operator compares GTID sets and cannot tell which transactions were internal, which is exactly why a clean pod kill can leave a site blocked. The “fresh datadir, clone it” answer confuses the emptiness test with the containment test — emptiness is decided from shared GTID UUIDs, and `iad` shares `A` with `pdx`, so it is a returning member and runs the divergence comparison rather than being cloned over (objective 7).

## Question 2

**Type:** MULTIPLE_CHOICE

`iad` is `RecoveryBlocked` and its `divergentGtid` begins `589f4b67`. You run `kubectl annotate mfg playground bloodraven.shipstream.io/reclone-site=iad:confirm=playground:uid=$UID:gtidPrefix=589f4b`, with `$UID` the group's `metadata.uid`. What happens?

- The reclone starts — `589f4b` is a correct prefix of the observed set, and the 8-character figure is only a hint printed in the error message.
- Nothing happens until the next 30 s re-verification, which re-reads the annotation and accepts it.
- Rejected: the prefix is shorter than the required 8 characters. A `RecloneRejected` warning event is emitted and the annotation is deleted from the CR.
- The value is parsed as a cold reclone, so `iad`'s datadir is wiped and rebuilt from `pdx` immediately, without any further confirmation being required.

**Correct option index:** 2

**Explanation:**

The interlock checks length before it checks match: a prefix under 8 characters is rejected outright, even though it happens to be a genuine prefix of the divergent set. The rejection emits a `RecloneRejected` warning event and the annotation is then deleted, so you must re-annotate rather than wait. The “a correct prefix is enough” answer is the tempting one — but the floor exists to make the token specific to one incident, not merely plausible. The “wait for the next re-verification” answer misreads the 30 s cadence, which re-verifies the divergence report, not the annotation; the annotation is already gone by then. The “parsed as a cold reclone” answer inverts the two forms — a value with `gtidPrefix=` is the divergent form, the cold form is the same `iad:confirm=playground:uid=…` value without it, and nothing about a short prefix converts one into the other (objective 8).

## Question 3

**Type:** SHORT_ANSWER

`iad` has no `divergentGtid` in status, but you want to rebuild it from `pdx` anyway. You run `kubectl annotate mfg playground bloodraven.shipstream.io/reclone-site=iad` and it is rejected. What does the operator require instead, and why is the bare site name not enough here?

**Sample answer:**

It wants the cold form with the confirmation pairs: `bloodraven.shipstream.io/reclone-site=iad:confirm=playground:uid=<playground's metadata.uid>`. `CLONE INSTANCE` wipes iad's datadir, so every reclone names the group (`confirm=` equal to the failover group's name) and its incarnation (`uid=`, so a request left over for an earlier group of the same name cannot apply) — you cannot destroy a site by typing one site name. With no divergentGtid recorded there is no GTID prefix to add, so no `gtidPrefix=`. The rejection message prints the exact value to set, and the rejected annotation is deleted, so re-annotate with the full form, or run `kubectl bloodraven reclone playground iad --cold`, which builds it.

**A full-credit answer shows:**

A strong answer gives the form `<siteName>:confirm=<groupName>:uid=<groupUID>` and uses the group name (`playground`) and its uid, not the site name or the GTID; and explains that a cold reclone is still destructive, so the interlock demands the confirmation even with nothing divergent recorded. Credit for noting the rejection message names the exact string, that the rejected annotation is deleted from the CR, or that `kubectl bloodraven reclone --cold` builds the value. Deduct if the answer supplies a GTID prefix (there is none to supply) or claims the bare form works when no divergence is recorded.

**Explanation:**

Both forms wipe the datadir, so the interlock always demands `confirm=<group name>` and `uid=<group uid>`. It branches on whether `divergentGtid` is populated only for the extra token: populated means the divergent form, which adds `gtidPrefix=` with an 8-character matching prefix; empty means a cold reclone, where the operator has no observed value to check you against, so the name and uid are the whole confirmation (objective 8).

## Question 4

**Type:** TRUE_FALSE

`iad` is blocked. You edit the CR status and clear `recoveryState` by hand, then re-apply the cold form `reclone-site=iad:confirm=playground:uid=<uid>` (no `gtidPrefix=`). It is now accepted, because `RecoveryBlocked` is what the interlock was checking.

**Correct answer:** false

**Explanation:**

The reversal: the interlock never reads `RecoveryState`. It keys on the presence of `divergentGtid` alone, precisely because `RecoveryBlocked` is a downstream UX field that can be transiently unset during a reconcile — gating on it would let a routine reconcile blip open the destructive path. With `divergentGtid` still populated, a value without `gtidPrefix=` is rejected exactly as before. And there is no hand-edit that helps: clearing `divergentGtid` too would let the cold form through only by deleting the operator's record of what the clone throws away, and the next re-verification records it again. Either way the rejected annotation is deleted and a `RecloneRejected` event is emitted (objective 8).

## Question 5

**Type:** MULTIPLE_CHOICE

`iad` is blocked with 3 divergent transactions that you have already dumped and want to keep. Which pair are the sanctioned ways out?

- Reclone `iad` from `pdx`, or replay the divergent transactions onto `pdx` so its GTID set comes to contain `iad`'s.
- Reclone `iad` from `pdx`, or run `CHANGE REPLICATION SOURCE TO ... SOURCE_AUTO_POSITION=1` on `iad` and let the GTID handshake reconcile the two sets.
- Replay the divergent transactions onto `pdx`, or repoint `iad` with an explicit binlog file and position so the auto-position handshake is skipped.
- Reclone `iad` from `pdx`, or drop the divergent GTIDs on `iad` so the two sets match and replication can start.

**Correct option index:** 0

**Explanation:**

There are exactly two exits. Reclone discards the divergent transactions and rebuilds `iad` from the current primary — simple, safe, expensive in time and bytes. Replay puts the transactions you extracted onto `pdx`, so `pdx` comes to contain `iad`'s set and the containment test passes on the next 30 s re-verification, rejoining `iad` with no annotation at all; it is cheap, but it requires you to have actually got the transactions and to accept them into the surviving timeline. The `CHANGE REPLICATION SOURCE TO` answer is hand-repointing: `SOURCE_AUTO_POSITION=1` asks `pdx` for transactions it never had, and the channel breaks. The binlog file-and-position answer is the workaround people reach for when that fails — it makes replication appear to run by discarding the GTID model's ability to detect the divergence at all. The drop-the-divergent-GTIDs answer is the same trade dressed up as tidying: it destroys the record of what was lost without recovering any of it (objective 9).
