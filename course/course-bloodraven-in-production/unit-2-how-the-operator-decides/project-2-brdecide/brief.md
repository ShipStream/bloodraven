# brdecide — the failover predictor

**Unit 2 — How the operator decides · project · `code-notebook` · Python**

## Goal

Build a tool that takes a failover-group status plus a clock and prints the decision the operator
would take — the action, the alert, the `Reason` string, and whether cooldown will let it run. Check
your own mental model against the real table now, on your terms, instead of at 3am while an incident
checks it for you.

**Learning goals**

- Implement the cross-site evaluation table in evaluation order, fence-first return included.
- Separate the decision from its execution gate by applying `failoverCooldown` only where the
  operator does.

## How this works

You have run the cross-site table in your head. Now write it down, where a machine can check it.

`brdecide` reads a `MysqlFailoverGroup` object — the same JSON `kubectl get mysqlfailovergroup orders
-o json` gives you — plus a clock reading, and prints what the operator would do with it. Nothing
talks to a cluster. Every input is a fixture in `tests/fixtures/`, so the answers are reproducible and
you can check yours against a known one.

```
python starter/brdecide.py --status tests/fixtures/orders-iad-down.json --now 2026-08-12T12:04:16Z
python starter/brdecide.py --status tests/fixtures/orders-iad-down-cooldown.json --now 2026-08-12T12:04:16Z --json
```

The starter runs as given. It prints `Reason (unset)` and `coreCount 0`, because the four functions
that matter are empty.

The running example is `orders`: `iad` and `pdx` as `primary-candidate`, `reader` as
`role: read-only`. Some fixtures add a fourth site to make a point.

## Your tasks

**TODO A — the pre-pass.** In `tally()`, walk the observations once. Increment `coreCount` for every
site whose role is not `read-only` — a `dr-only` site counts, an `unknown` state counts. Route any
site that is `writable` while its role is not `primary-candidate` into `fenceSites` and skip it, so it
never reaches a tally. Skip `role: read-only` sites entirely. Everything left lands in the tally for
its state; `unknown` lands in none of them.

**TODO B — the rows.** In `evaluate_cross_site()`, evaluate the rows in the order the code does and
return at the first one that fires: fence-first, TotalLoss, SplitBrain, Failover, NoPrimary, Degraded,
Healthy. Exact conditions, alert strings and `Reason` values are in the docstring. Two of them are
easy to get wrong: the failover row needs three conjuncts at once — zero writable **and** at least one
unreachable **and** at least one read-only — and it is the only acting row that sets no alert.

**TODO C — the history.** In `rehydrate_last_failover()`, choose between the two durable copies of
`lastFailover`: the status subresource and the `bloodraven.shipstream.io/last-failover` annotation.
Discard either one stamped more than 5 minutes ahead of `now`, install the later of what survives, and
give ties to status.

**TODO D — the gate.** In `apply_gate()`, build `willRun`. Fencing a writable non-promotable site runs
every poll. A promotion is the one thing `failoverCooldown` gates. Report `cooldownRemaining` whenever
there is a record to measure against, even when nothing is waiting on it.

Then run `python3 tests/harness.py` until it prints `PASS`.

## What the scaffolding is for

Everything peripheral is wired: argument parsing, loading the object, the join of `spec.sites[].role`
to `status.sites[].state` (a site absent from status is `unknown`, not missing), Go duration parsing,
reading both history copies, `rank_promotion_candidates()`, and both output formats. The `--json`
output is what the tests read, so keep its keys as `new_action()` defines them.

`status.conditions` is in the fixtures because it is in a real object. It is the **previous** poll's
verdict — sometimes stale by design. Do not read your answer out of it.

## Expected output

```
brdecide — bloodraven-playground/orders at 2026-08-12T12:04:16Z

  sites         iad=unreachable(primary-candidate)  pdx=read-only(primary-candidate)  reader=read-only(read-only)
  coreCount     2
  tallies       writable=[] readOnly=['pdx'] unreachable=['iad']
  fenceSites    -

  Reason        Degraded
  Alert         (none)
  SplitBrain    no
  Candidates    pdx  (tiebreak order — GTID freshness picks the winner)

  lastFailover  2026-08-12T12:02:46Z → pdx  (from status)
  cooldown      5m0s configured, 3m30s remaining
  BLOCKED       promotion blocked by cooldown
  willRun       (nothing)
```

## Rules

- `evaluate_cross_site()` stays pure. No clock, no history, no cooldown — the real `EvalCrossSite`
  never consults any of them, and a test asserts yours does not either.
- Use only the five reason strings the operator actually emits: `Healthy`, `Degraded`, `SplitBrain`,
  `NoPrimary`, `TotalLoss`. The docs name a sixth, `Failover`. It does not exist.
- Alert strings are verbatim, including the comma-space joins.
- Standard library only. Do not edit `tests/`.

## Steps

- [ ] **Count the sites the way the matrix counts them** — fill in TODO A. Done when
      `orders-healthy.json` prints `"coreCount": 2`, `"writable": ["iad"]`, `"readOnly": ["pdx"]`;
      `orders-reader-writable.json` prints `"fenceSites": ["reader"]` with `reader` in no tally; and
      `orders-dr-only.json` prints `"coreCount": 3` with `"readOnly": ["lhr"]`.
- [ ] **Evaluate the rows in order, fence-first at the top** — done when `orders-healthy.json`,
      `orders-peer-down.json`, `orders-split-brain.json`, `orders-total-loss.json` and
      `orders-reader-writable-split-brain.json` print `Healthy`, `Degraded`, `SplitBrain`, `TotalLoss`
      and `Degraded`, and the last prints `"splitBrain": false`.
- [ ] **Make the failover row demand all three conjuncts** — done when `orders-iad-down.json` prints
      `"promotionCandidates": ["pdx"]` with `"alert": null`, `orders-all-read-only.json` prints
      `NoPrimary` with `NO PRIMARY: both sites are read-only`, and `orders-dr-only.json` prints
      `"promotionCandidates": []`.
- [ ] **Rehydrate the history from whichever copy survived** — done when `orders-history-conflict.json`
      prints `"lastFailoverSource": "annotation"`, and `orders-history-skewed.json` and
      `orders-history-tie.json` both print `"lastFailoverSource": "status"`.
- [ ] **Gate the promotion, and nothing else** — done when `orders-iad-down-cooldown.json` prints
      `"promotionBlockedBy": "cooldown"`, `"cooldownRemaining": 210.0`, `"willRun": []` and still
      `"promotionCandidates": ["pdx"]`; and `orders-reader-writable-cooldown.json` prints
      `"willRun": ["fence:reader"]` with `"promotionBlockedBy": null`.
- [ ] **Run the whole fixture set** — `python3 tests/harness.py` exits 0 and prints `PASS`.
- [ ] **Record the reason string the docs get wrong** — add a `# NOTE:` comment to `brdecide.py`
      naming both the `Failover` the docs promise and the `Degraded` the code emits.

## Grading

See [`rubric.md`](rubric.md) for the four weighted criteria and what full marks look like on each.
The automated checks are the four `testCases` in `project.json`; they run the same functions
`tests/harness.py` runs.
