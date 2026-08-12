# Rubric — brdecide

Human grading, 100 points. The automated `testCases` in `project.json` are scored separately and
carry their own 100.

| Weight | Criterion |
| ---: | --- |
| 35 | The rows are evaluated in the operator's order, with the fence-first return above everything |
| 25 | The cooldown gates the promotion and nothing else, and the history it measures against is rehydrated correctly |
| 20 | The pre-pass tallies sites by role the way the matrix does |
| 20 | Craft: the decision is separated from its gate, and the output is readable under pressure |
| **100** | |

## 35 — The rows are evaluated in the operator's order, with the fence-first return above everything

`evaluate_cross_site()` is a sequence of guarded early returns in the order fence-first, TotalLoss,
SplitBrain, Failover, NoPrimary, Degraded, Healthy — not an unordered set of cases and not a lookup
keyed on a state tuple.

Full marks require all of:

- The fence-first branch returns before the TotalLoss and SplitBrain rows can be reached. A writable
  reader alongside two writable candidates is a fencing job, not a split brain.
- The failover row demands zero writable **and** at least one unreachable **and** at least one
  read-only, simultaneously.
- The failover row sets `promotionCandidates` and `Reason` but no alert — it is the only acting row
  without one.
- Both `NoPrimary` messages are present, with the two-site variant conditioned on exactly two
  read-only sites and zero unreachable.
- Every alert string is verbatim, comma-space joins included.

Deduct for any invented reason string outside the five the operator emits (`Healthy`, `Degraded`,
`SplitBrain`, `NoPrimary`, `TotalLoss`) — a `Failover` reason in particular, which the docs promise
and the code never emits.

## 25 — The cooldown gates the promotion and nothing else, and the history it measures against is rehydrated correctly

- `apply_gate()` blocks only `promote`, and only when a promotion was selected, a record exists, and
  the elapsed time is under the cooldown.
- `fence:<site>` entries are emitted regardless of the cooldown state.
- `cooldownRemaining` is reported whenever a record exists, including when nothing is waiting on it,
  and negative elapsed time counts as inside the window.
- `rehydrate_last_failover()` discards a copy stamped more than five minutes ahead of `now`, installs
  the later of what survives, and gives ties to status.
- The candidate list and `Reason` survive a blocked promotion unchanged — the table still ran.

## 20 — The pre-pass tallies sites by role the way the matrix does

- `tally()` increments `coreCount` for every non-`read-only` role, so a `dr-only` site counts toward
  `coreCount` and lands in a tally while a `read-only` site does neither.
- A writable non-`primary-candidate` site goes to `fenceSites` and is skipped before any tally sees
  it.
- `unknown` sites count toward `coreCount` and appear in no tally.

Deduct if `dr-only` is treated as a reader, if a fenced site is double-counted in the writable tally,
or if the promotion candidate list can contain a non-`primary-candidate` site.

## 20 — Craft: the decision is separated from its gate, and the output is readable under pressure

- `evaluate_cross_site()` takes only observations and priorities and touches no clock, history or
  cooldown, so the table can be reasoned about and tested on its own.
- The default text report is scannable at 3am: the reason, the alert and what will actually run are
  each on their own labelled line, and a blocked promotion is visibly distinct from a decision with
  nothing to do.
- A malformed or unreadable `--status` file produces a one-line diagnostic on stderr and a non-zero
  exit, not a traceback.
- Comments explain the non-obvious rules — why the failover row sets no alert, why the tie goes to
  status — rather than restating the code.
