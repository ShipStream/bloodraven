# The six-row table that decides everything

`playground` has three sites: `iad` and `pdx` as `primary-candidate`, and `reader` as `role: read-only`. The counter application is reading through the `-primary` Service every two seconds and writing on every button press. You can now take a poll result and name each site's state — `writable`, `read-only`, `unreachable` or `unknown` — and you know a site takes `pollInterval × failureThreshold` = 2 s × 3 = **6 s** to reach `unreachable` (objective 2). What you cannot yet do is turn three of those labels into one decision. That is one function: `EvalCrossSite` in `internal/state/matrix.go`. It takes the per-site observations and the `sitePriorities` list, and returns a single `CrossSiteAction`. Everything visible about `playground` — the condition reason, the alert text, whether a promotion happens at all — falls out of it.

## The idea: order is the behaviour

The function is a sequence of guarded early returns. A row can only fire if every row above it declined. Reading the table as an unordered list of cases is the commonest way to predict the operator wrongly.

Before any row is evaluated, one loop walks the observations and does three things. It counts `coreCount`, incrementing for every site whose role is **not** `read-only`. It routes any site that is `writable` while its role is not `primary-candidate` into `action.FenceSites` and `continue`s — so that site never reaches a tally. And it drops `role: read-only` sites entirely. The consequence people get wrong: a `dr-only` site is **not** excluded. `dr-only` is non-promotable, but it still counts toward `coreCount` and it still lands in the `readOnly` or `unreachable` tally. Only `role: read-only` is invisible to the matrix (objective 4).

```widget
{
  "type": "order",
  "title": "EvalCrossSite — put the rows in the order the code evaluates them",
  "items": [
    "Fence-first early return — len(FenceSites) > 0 → Alert \"writable non-promotable site requires fencing (…)\", Reason \"Degraded\", return",
    "TotalLoss — len(unreachable) == coreCount → \"TOTAL LOSS: all sites are unreachable\", Reason \"TotalLoss\"",
    "SplitBrain — len(writable) > 1 → \"SPLIT BRAIN: N sites are writable (…)\", Reason \"SplitBrain\"",
    "Failover — len(writable)==0 AND len(unreachable)>0 AND len(readOnly)>0 → PromotionCandidates set, Reason \"Degraded\", no Alert",
    "NoPrimary — len(writable)==0 and the three conjuncts did not hold → Reason \"NoPrimary\", alert only",
    "Degraded (primary up, peer down) — exactly one writable AND len(unreachable) > 0 → \"<site> unreachable while <site> is primary\", Reason \"Degraded\"",
    "Healthy — exactly one writable, zero unreachable → Reason \"Healthy\""
  ]
}
```

Three details inside that sequence earn their own sentence.

**The failover row needs three conjuncts at once**: zero writable **and** at least one unreachable **and** at least one read-only. Drop the unreachable peer and the operator refuses to act. The comment in the source says why: *"Without any unreachable peer we refuse to auto-elect a primary (all-read-only is a startup or recovery state that needs human input)."* All-read-only is indistinguishable from a cluster that has not finished starting, so the operator alerts and stops. All-unreachable gets the same treatment for the opposite reason — nothing is left to promote (objective 5).

**`NoPrimary` has two messages.** Exactly two read-only sites and zero unreachable gives `NO PRIMARY: both sites are read-only`. Everything else gives `NO PRIMARY: no writable site available`.

**The one row that acts is the only row that sets no alert.** The failover branch fills `PromotionCandidates` and sets `Reason = "Degraded"` — it never assigns `action.Alert`. Every other non-healthy row sets one.

## There is no `Failover` reason

The single most expensive misreading of this function has nothing to do with which row fires. It is what the winning row *writes down*.

The failover row sets `Reason = "Degraded"`. That string is written verbatim onto the `Degraded` condition in `status.conditions`, with `snap.Alert` as the message. There is **no `Failover` reason**, and there never was one — the topology that is one promotion away from recovery reports the same reason as every other unhealthy shape. So a Prometheus or `kubectl` rule matching a condition reason of `Failover` does not fire rarely. It fires **never**, silently, through every promotion you will ever do.

Exactly five reason strings reach `status.conditions`:

`Healthy` · `Degraded` · `SplitBrain` · `NoPrimary` · `TotalLoss`

Write your rules against those, and read "a failover happened" off `bloodraven_failovers_total` — a counter, not a condition.

## What a summary table cannot carry

The published documentation renders this function as a handful of two-column rows. Summaries lose things, and it is worth naming what this one loses, because both omissions describe states `playground` really sits in.

**There is no row for "the primary is fine and a peer is down."** That is the shape of the entire recovery window after a site failure: one writable, at least one unreachable, `Reason = "Degraded"`, with an alert naming the pair. It is a real branch of `EvalCrossSite`, and it belongs on any table you build for your own on-call.

**There is no row for the fence-first early return.** A summary that lists site states in columns structurally cannot express that one branch *preempts every branch below it*. Fence-first is not a case among cases; it is a `return`.

Take the habit rather than the grievance. The CRD is the contract, the code is the behaviour, and a rendered table — including the widget above — is a picture of the code rather than the code. When the two disagree, `EvalCrossSite` wins. A dated record of where the published page has and has not caught up is in the [version appendix](../sources.html#version-appendix), section B, so this reading does not have to carry it.

## Worked: two states of `playground`

**A — `iad` dies.** After 6 s of failed polls `iad` is `unreachable`, `pdx` is `read-only`, `reader` is `read-only`. The loop: `reader` is excluded, so `coreCount = 2`. No writable non-candidate, so `FenceSites` is empty. Tallies: `writable = []`, `readOnly = [pdx]`, `unreachable = [iad]`. Row by row — fence-first declines (empty); `TotalLoss` declines (1 ≠ 2); `SplitBrain` declines (0 is not > 1); the failover row's three conjuncts all hold, so `PromotionCandidates = [pdx]` and `Reason = "Degraded"`, with no alert. The caller then runs `pickFreshestCandidate` over that list.

**B — `reader` comes up writable.** A freshly restarted MySQL pod is writable for a few seconds before anything fences it. Now `iad` is `writable`, `pdx` is `read-only`, `reader` is `writable`. The loop: `reader` is not counted in `coreCount` (still 2), and because it is writable while non-candidate it goes to `FenceSites` and is skipped — it never joins the `writable` tally. So `writable = [iad]`. Fence-first fires: alert `writable non-promotable site requires fencing (reader)`, `Reason = "Degraded"`, return. Note what did *not* happen: two sites in the group were genuinely accepting writes, and the operator did **not** report split brain, because `len(writable) > 1` counts core sites only and the reader was already removed (objective 6).

```widget
{
  "type": "match",
  "title": "Site states of playground → the Reason string that reaches status.conditions",
  "pairs": [
    {
      "term": "iad writable, pdx read-only, reader read-only",
      "match": "Healthy"
    },
    {
      "term": "iad writable, pdx unreachable",
      "match": "Degraded (\"pdx unreachable while iad is primary\")"
    },
    {
      "term": "iad unreachable, pdx read-only",
      "match": "Degraded (promotion candidates set, no alert)"
    },
    {
      "term": "iad read-only, pdx read-only, none unreachable",
      "match": "NoPrimary (\"NO PRIMARY: both sites are read-only\")"
    },
    {
      "term": "iad writable, pdx writable",
      "match": "SplitBrain"
    },
    {
      "term": "iad unreachable, pdx unreachable",
      "match": "TotalLoss"
    },
    {
      "term": "iad writable, reader writable",
      "match": "Degraded (fence-first, reader fenced)"
    }
  ]
}
```

## What the function does not do

`EvalCrossSite` is **pure**. The source comment is explicit: *"The function is pure: it never considers history or policy beyond the supplied priorities."* It has no clock, no memory of prior promotions, and no access to MySQL. That is why split-brain auto-resolution, which needs history, is layered on top by the caller rather than living in the matrix.

The matrix is evaluated on **every** poll, not only on a state transition — *"Evaluate every poll so all status snapshots carry the current topology condition. Mutating cross-site actions remain transition-driven."* So `status.conditions` always reflects the current topology, while the actions that change MySQL fire on transitions.

And when the table does select a failover, the matrix does not choose the winner. It hands `PromotionCandidates` to the caller, ordered by `sitePriorities` then declared order, and `pickFreshestCandidate` reads `GTID_EXECUTED` from each one. **GTID freshness is the primary selector — it minimises data loss on promotion.** `sitePriorities` only breaks ties or incomparable sets. A site listed first in `sitePriorities` will lose to a fresher peer.

## Handoff

You can now read any set of site states for `playground` and name three things without running anything: the action, the alert string, and the `Reason` that lands in `status.conditions`. Example A said `Reason = "Degraded"` with `PromotionCandidates = [pdx]` — the table selected a failover. That is not the same as the operator performing one. Something between the table's verdict and the promotion can still refuse, and it keeps its own record of what it did last.
