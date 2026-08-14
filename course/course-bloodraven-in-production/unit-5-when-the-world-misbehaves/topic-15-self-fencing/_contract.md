# Self-fencing: the sidecar's two rules

**Unit:** 5 — When the world misbehaves
**Objectives (unit-numbered):**
1. Name the two rules the fencing monitor evaluates each tick and the order it evaluates them in   [obj 1]
2. Explain why one reachable peer keeps a primary writable, and why that is not a quorum   [obj 2]
3. Say what the startup safety net does before MySQL is allowed to accept writes   [obj 3]

## Topic generation prompt

Open on `playground` with the operator deliberately scaled to zero. The counter app keeps writing to the active site and nothing bad happens — that is the point. What stops that site writing when it can no longer confirm it is the active site is the sidecar, not the operator.

Teach the `FencingMonitor` in `internal/sidecar/fencing.go` exactly as implemented: **two** rules, not three. Rule #1 is topology mismatch. Rule #2 is lease expiry. Rule #1 fires first and returns, so a site that has learned the active site is somebody else fences without ever consulting the lease. The published documentation describes a three-rule monitor; it is wrong, and this course teaches the code. Before either rule runs the monitor checks `@@read_only` and returns early: a read-only instance never self-fences, because there is nothing left to fence.

Then teach the thing that is not a rule at all. `Server.RunSafetyNet` is a separate one-shot in `internal/sidecar/server.go`, wired in `cmd/sidecar/main.go`, and it completes **before** the monitor is constructed. It fails closed by *staying* fenced rather than by actively fencing, and it has three distinct log strings — quote all three verbatim: `safety net: could not query active site, staying fenced`, `safety net: no active site reported by operator, staying fenced`, `safety net: confirmed standby site, staying fenced`. Make the learner able to tell a safety-net line from a monitor line in a log bundle, because they mean different things: one is a pod that has never been allowed to write, the other is a pod that was writing and lost the argument.

Teach where the sidecar's belief about topology comes from. Peer snapshots are adopted only when **strictly newer** — `topology.Adopt` returns false unless `observedAt` is after the cached value. Operator reads use `Set` unconditionally, because the operator is authoritative. That asymmetry is the entire trust model, expressed in two method names.

Teach the lease numbers and their invariants: `leaseTimeout` default 20 s, `peerCheckInterval` default 5 s, with CEL invariants `peerCheckInterval >= 1s`, `leaseTimeout >= 3s` and `leaseTimeout >= 3 × peerCheckInterval`. The shipped 20 s / 5 s pair sits at that 3× floor, so tuning one without the other gets the object rejected at admission. Then land the hard part: the lease fence requires the operator **and every peer** to be silent for the full window. One reachable peer keeps the primary writable. That is explicitly retained compatibility behaviour, not a quorum guarantee — and a `read-only` reader counts as a peer, so **adding a reader makes the lease fence less likely to fire**. State that plainly and dwell on it; it is counter-intuitive and it changes how you size a group.

Close on what fencing actually does at the MySQL layer, because the mechanism has edges the Bloodraven docs skip. `SET GLOBAL super_read_only = ON` does not cut writers off. It blocks while other clients have an ongoing statement, an active `LOCK TABLES WRITE`, or an ongoing commit, and it fails outright if the issuing session holds explicit locks or a pending transaction. `super_read_only` is the real barrier because it prohibits client updates even from users holding `CONNECTION_ADMIN` or the deprecated `SUPER`, which `read_only` alone does not. Setting `super_read_only=ON` implicitly forces `read_only=ON`; setting `read_only=OFF` implicitly forces `super_read_only=OFF`. Replication threads keep writing under it, which is precisely why a fenced site can still be a working replica. And fencing does not close sockets: a surviving session can serve stale reads until the site is next promoted or demoted.

Do NOT cover split-brain detection or resolution, `sitePriorities`, the partition scenarios, or operator downtime — topics 2, 3 and 4 own those.

## Requested activities

- READ: 1000-1200 words. Open on the operator-at-zero scene, then the two rules in evaluation order, then the read-only early return, then the startup safety net as a separate one-shot with its three log strings, then `Adopt` versus `Set`, then the lease numbers and the one-reachable-peer consequence, then the MySQL-layer mechanics of `super_read_only`. Use one `order` widget stepping a single monitor tick: read `@@read_only` and return if read-only, evaluate rule #1 topology mismatch and return if it fires, evaluate rule #2 lease expiry, fence or do nothing. No second widget.
- FLASHCARDS: The fencing vocabulary and its numbers — rule #1, rule #2, `leaseTimeout` 20 s, `peerCheckInterval` 5 s, the three CEL invariants, `Adopt` vs `Set`, `read_only` vs `super_read_only`, the three safety-net log strings, what a fenced replica may still do. 8-10 cards.
- QUIZ: 5 questions on discriminations that matter in a log bundle: rule #1 versus rule #2 given a fence with a live operator; a safety-net line versus a monitor line; `read_only` versus `super_read_only` when a `SUPER` user can still write; whether a single reachable reader suppresses the lease fence; why a fence call can hang for seconds instead of returning immediately.

## Handoff

**Inherits:** The application now survives a promotion — the learner has pinned pool and DNS behaviour and has a measured write-gap for `playground` across a failover.
**Leaves:** The learner can attribute an unexpectedly read-only site to rule #1, rule #2, or the startup safety net, from logs alone, and can say why fencing sometimes blocks or fails.
**Do not cover:** Split-brain tiers and `sitePriorities` (topic 2), partition shapes (topic 3), operator downtime and the cooldown (topic 4), backups and DR (Unit 6).
