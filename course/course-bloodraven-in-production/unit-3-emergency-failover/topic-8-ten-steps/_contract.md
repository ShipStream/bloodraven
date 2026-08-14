# The nine steps of a promotion

**Unit:** 3 — Emergency failover, end to end
**Objectives (unit-numbered):**
1. Put a site down hard enough to trigger a failover, and say why a container restart may not be enough   [obj 1]
2. Name the steps of the failover sequence in order and say which ones are fatal on error   [obj 2]
3. Explain the 30-second relay-log drain and when it costs the full budget   [obj 3]

## Topic generation prompt

The learner can already predict the operator's decision from a status dump. Now they make the decision happen and watch the consequence execute. Open by taking `iad` down on the running `playground` group — `kubectl scale deployment mysql-playground-iad --replicas=0` — with the counter application still writing, and watch `status.activeSite` flip to `pdx`.

**Injection first.** Be precise and correct a widespread superstition: pod force-delete *does* trigger failover. Scenario 09b hard-waits for `activeSite` to flip after a `--grace-period=0 --force` delete and passes. The reason the sub-five-second Deployment respawn does not save the primary is that the debounce does not watch pod objects — it watches whether mysqld answers `CheckReadOnly`, and a cold container start plus InnoDB recovery comfortably exceeds the six-second detection window. Scenario 01 uses scale-to-0 for **determinism**, not because delete fails: a pod-delete races the respawn and can end up restoring the original topology through split-brain recovery instead of completing the failover. The genuine no-failover cases are different: a container restart in place (scenario 16 issues SQL `SHUTDOWN`; pod, PVC and IP all survive, and the scenario explicitly accepts either outcome depending on how fast the kubelet restarts the container against the ~6 s window), and a pod crash with the PVC intact, which is RPO 0 and no failover at all because the operator sees the primary come back writable and keeps it.

**Then the sequence — teach the code, not the docs.** The published ten-step list in the documentation is wrong in five ways, and this course teaches `internal/controller/failover.go`. Give the steps in order, each with its fatality:

1. Fence the old primary — `SET GLOBAL super_read_only = ON`. Error only **warns**; the old primary is usually unreachable, which is the whole reason we are here.
2. **Kill application connections on the old primary** — `SELECT id FROM information_schema.processlist WHERE id != CONNECTION_ID() AND command NOT IN ('Binlog Dump', 'Binlog Dump GTID')`, then `KILL <id>` per row. Note what the WHERE clause protects: replication dump threads survive.
3. Relay-log drain on the candidate, 30 s budget, **non-fatal** — on timeout the operator logs a warning and promotes anyway.
4. `STOP REPLICA` — **fatal**.
5. `RESET REPLICA ALL` — **fatal**.
6. Record the promotion GTID, `SELECT @@global.gtid_executed` — **non-fatal**, a warning only.
7. `SET GLOBAL super_read_only = OFF` — **fatal**.
8. `SET GLOBAL read_only = OFF` — **fatal**.
9. Writable confirmation, run **synchronously in the same call stack**, not deferred to the next poll. If it fails the operator logs that promotion succeeded but DNS was not flipped, and returns.

Make step 7-8 a teaching point in its own right: promotion is **two** statements. Anyone who learned `read_only=0` as the promotion command has a wedged-primary gap, because `super_read_only` is what the sidecar fences with, and a site left at `super_read_only=ON` with `read_only=OFF` is not a primary. Then the ordering fact that shows the design's paranoia: the durable failover record and the metrics counter are stamped **before** the DNS flip, deliberately, so that a DNS-provider outage cannot erase the fact that a promotion happened.

**Correct two more doc errors explicitly.** Node taints are **not** a step of this sequence — they are a pure function of per-site state transitions, applied earlier in the same poll by a different code path. Source convergence is **not** a step either — it is an independent poll stage with its own 20 s budget. The docs present both as ordered steps of the promotion; they are neighbours in the poll, not links in the chain.

**Drain internals**, since objective 3 lives here: the drain exits early the moment the SQL thread is running and `Seconds_Behind_Source` reads 0; internally it waits 500 ms, doubling to a 4 s ceiling, with one SQL-thread restart and retry.

**Timing, from measurements not documentation.** A clean primary kill flips `activeSite` in **12.0 s**, reproducible across nine-plus independent runs. Scenario 14 — which pauses the replica SQL applier, seeds five seconds of writes, then kills the primary — measures **36.0 s**. Explain the difference rather than just stating it: the 12 s case is 6 s of detection plus a drain that returns almost immediately because the candidate is already caught up; the 36 s case is the same detection plus a drain that spends its entire 30 s budget on relay logs that have to be applied. The documentation's 30-45 s and ~37 s figures are cut from this course as ungrounded — teach 12 s typical and ~37 s worst case, and say why they differ. The playground's `failoverCooldown: 30s` is an override of the shipped 5m default; timings observed here are real, cooldowns are not.

Do NOT cover RPO accounting or transaction-loss counting (topic 2), and do NOT cover what happens to the old primary when it comes back (topic 3).

## Requested activities

- READ: 1000-1200 words. Injection technique and the three no-failover cases; the nine steps with fatality per step; the two-statement promotion; the record-before-DNS ordering; taints and source convergence as non-steps; drain internals; then the measured 12.0 s / 36.0 s pair with the explanation. Use one `order` widget on the nine steps — the learner sequences them before revealing, and the reveal carries the fatal/non-fatal annotation. Optionally one `terminal` widget on the `failover complete` log line with `promotedSite` and `promotionGtid`.
- **THE FAILURE MOMENT — required, and it belongs in this READ as a blockquote callout placed immediately after the timing section.** Write it as the learner's own experience: they did everything right, `iad` is held down, the operator promoted `pdx` in about twelve seconds, the DNS record flipped, the `-primary` Service selector moved. Then split the failure in half, because the counter application has two code paths and they fail differently. Its page polls `GET /api/counter` every two seconds — a **read**, on the pooled socket it opened to `iad` — and that keeps succeeding, returning `iad`'s stale value with `dbHost` still naming `iad`; the number simply stops moving. Press **+ Increment** and the **write** fails immediately with `ERROR 1290`. Writes break loudly on the first attempt; reads succeed and lie. Name the surprise plainly, validate that this is genuinely confusing rather than a mistake the learner made, give the one-line mental model, and point at Unit 4. **Do not fix it here and do not explain the mechanism in detail.** For accuracy while writing the callout: `super_read_only` blocks writes but closes no sockets, so an already-open session keeps serving stale reads; the operator's connection drain needs a **reachable** old primary, so in this exact scenario — the site is held down — it cannot run at all; the reason nobody is paged is that no shipped alert watches application writes, not that the failure is subtle; and note honestly that the counter app sets `SetConnMaxLifetime(30 * time.Second)`, so the stale window closes on its own inside about thirty seconds. Point at the version appendix rather than dating the gap. Nothing in Bloodraven's alert map fires for it. The mental model to land, in the writer's own phrasing: the operator's job ends at a label selector and a DNS record, and a socket that was already open is outside its reach.
- FLASHCARDS: The nine steps and their fatality; the two-statement promotion; `Binlog Dump` exclusion in the kill query; drain 30 s / 500 ms / 4 s / early-exit condition; 12.0 s and 36.0 s; taints and source convergence as non-steps; scale-to-0 versus force-delete versus in-place restart. 10-12 cards.
- QUIZ: 5 questions discriminating: which steps abort the failover on error and which only log a warning (targets the belief that fencing failure blocks promotion); whether a `--grace-period=0 --force` pod delete triggers failover (it does — the distractor is the five-second respawn); whether `SET GLOBAL read_only = OFF` alone promotes a site; which of taints, source convergence and writable confirmation are part of the sequence; and given a failover measured at 36 s, what consumed the extra 24 s.

## Handoff

**Inherits:** From Unit 2 — the learner can predict the operator's decision from a status dump: which matrix row fires, which site is the promotion candidate, and how long detection takes.
**Leaves:** The learner has driven a real emergency failover on `playground`, can recite the sequence with fatality per step, can explain a 12 s run against a 36 s run, and has been shown the reading-from-the-demoted-site surprise without any explanation of the mechanism.
**Do not cover:** Transaction-loss accounting and `promotionGtidExecuted` (topic 2), old-primary recovery, divergence or reclone (topic 3), split brain (Unit 5), any part of the client-side fix (Unit 4).
