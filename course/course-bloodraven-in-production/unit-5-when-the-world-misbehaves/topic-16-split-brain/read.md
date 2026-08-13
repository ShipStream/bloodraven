# Split brain, and what `sitePriorities` really buys you

`playground` is telling you something new. The counter application's writes are still landing, both `iad` and `pdx` are answering, and the group's condition reads `SPLIT BRAIN: 2 sites are writable (iad, pdx)` with reason `SplitBrain`. The matrix emits that the moment more than one core site reports `read_only=0` — strictly `len(writable) > 1`, with the `read-only` `reader` site excluded from the tally. Two writable primary-candidates, then. What does the operator do about it?

That depends entirely on one field, and there is a habit to install before you go looking for it.

## Ask the cluster, not the page

The real field is `spec.splitBrainPolicy.sitePriorities`: an ordered list of site names. The operator promotes the first entry that is currently writable.

The reason to be emphatic about the *name* is a failure mode with no error message anywhere in it. A `MysqlFailoverGroup` that names a field the CRD schema does not define is not rejected — **the API server prunes the unknown field and admits the object**. No admission error, no Kubernetes event, no line in the operator log, and `kubectl get -o yaml` comes back without it because it was never stored. Your group silently keeps whatever behaviour it had, and you find out at 3am when a split brain sits there alerting and nothing fences.

That is not a hypothetical: for part of Bloodraven's history the published failover page described a field called `preferSite` — in prose, and in a copy-pasteable YAML block — that no shipped CRD has ever defined. The page has since been corrected; the dated record is in the [version appendix](../sources.html#version-appendix), row B1. The habit outlives the instance.

Two commands make it impossible to be caught by this class of error. `kubectl explain mysqlfailovergroup.spec.splitBrainPolicy` renders the schema the API server actually validates against, which is the only schema that matters. And `grep -rn preferSite config/crd/bases/ charts/bloodraven/crds/` returns nothing, which is the whole story in one line. Documentation drifts from CRDs in both directions and at unpredictable speed. The CRD is the contract.

```widget
{"type":"anatomy","title":"The field that actually ships","parts":[{"text":"splitBrainPolicy:","label":"optional block","note":"Omit it entirely and you are on tier 3 — alert only."},{"text":"  sitePriorities:","label":"the real field","note":"Not preferSite. An ordered list, not a single name. MaxItems 16, matching spec.sites."},{"text":"    - iad","label":"first choice","note":"Wins if it is currently writable AND role: primary-candidate."},{"text":"    - pdx","label":"fallback","note":"Wins only if iad is not currently writable. Order is the whole policy."}]}
```

## The three tiers, in evaluation order

The response is tiered, evaluated in this order every time the operator sees `SplitBrain`.

| Tier | Trigger | What the operator does |
| --- | --- | --- |
| 1 — history | `lastFailoverTarget` names a site that is **live, writable and promotable** | Fences every other site immediately, regardless of policy |
| 2 — policy | No usable history **and** `sitePriorities` is non-empty | `ResolveSplitBrain` picks the winner, losers are fenced, winner is re-promoted through the standard path |
| 3 — neither | No usable history **and** no priorities | Alert only. Manual resolution, by design |

Tier 1's three conditions are a conjunction and all three matter: `keepSite != nil && keepSite.state == StateWritable && keepSite.isPromotable()`. A recorded target that has since gone unreachable, been demoted to read-only, or had its role changed away from `primary-candidate` does **not** win by memory alone — the operator falls through to tier 2. Authority has to be currently true, not merely once recorded.

Tier 2 runs `state.ResolveSplitBrain(writable, sitePriorities)`. It walks your list in order and takes the earliest entry that is both currently writable and `primary-candidate`; every other writable site becomes a loser to fence. Two refusals are worth memorising. An empty list returns `("", nil)` — it will not guess. So does a list whose entries name nothing currently writable and promotable. It **never falls back to the order sites were declared in** under `spec.sites`. No policy means no automated resolution.

## The selector that is deliberately missing

In Unit 2 you met normal promotion, where GTID freshness is the *primary* selector and the priority list is only a tiebreaker — `pickFreshestCandidate` takes the most caught-up replica precisely to minimise data loss.

Split-brain winner selection does not consult GTID at all. The code says why: *"GTID freshness is intentionally not consulted here — split-brain winner selection is policy-driven because every writable side may carry unique writes."* Normal promotion compares replicas against a dead primary, where "freshest" genuinely means "loses least". In a split brain both sides have been *accepting original writes*, and neither GTID set contains the other. There is no freshest that is safe, so the operator stops pretending and asks you instead. Same system, opposite selector, for a good reason.

## What shows up in logs and manifests

CEL rejects priority entries that do not name a `primary-candidate` site: *"splitBrainPolicy.sitePriorities entries must match the names of sites with role 'primary-candidate'"*. Naming `reader` fails at `kubectl apply`, not silently at 3am. The resolution then emits a `Warn` naming the correct field and carrying two keys you can alert on — `winner` and `fencedSite`.

```widget
{
  "type": "terminal",
  "title": "What a tier-2 resolution looks like in the operator log",
  "lines": [
    {
      "cmd": "kubectl -n bloodraven-playground logs deploy/bloodraven | grep -E 'ALERT|split-brain'",
      "out": "{\"level\":\"WARN\",\"msg\":\"ALERT\",\"message\":\"SPLIT BRAIN: 2 sites are writable (iad, pdx)\"}\n{\"level\":\"WARN\",\"msg\":\"split-brain auto-resolve: fencing non-preferred site per spec.splitBrainPolicy.sitePriorities\",\"winner\":\"iad\",\"fencedSite\":\"pdx\"}"
    }
  ],
  "caption": "Recorded output. **Run** reveals what is already on the page — nothing executes, and no cluster is contacted."
}
```

Fencing is also retried on **non**-transition polls, and the retry gate counts writable promotable sites directly rather than trusting `action.SplitBrain` — the matrix reports `SplitBrain=false` whenever a writable non-promotable site needs fencing first. A stable split brain produces no further transitions, so a fence that failed once would otherwise never get a second attempt. Counting the actual hazard means it does.

## The honest paragraph

The project's own docs flag this with a danger admonition. Carry the sentence out of this topic: priority-based resolution *"makes split-brain resolution fast and deterministic at the cost of silently losing the loser's unreplicated writes. The loss is surfaced loudly but not prevented."*

`sitePriorities` does not prevent split brain, and merges nothing. It is a standing decision about **which writes you are willing to discard**, made in advance so the operator does not have to wake you. Policy, not safety.

## Where your split brains will actually come from

Not from exotic partitions. From restarting a pod. A freshly created or freshly cloned MySQL pod comes up **writable** for several seconds before anything fences it. A recorded run has the new `pdx` pod Running and writable at **T+22s** and `ALERT: SPLIT BRAIN` at **T+33s** — eleven seconds with two sites taking writes. Restart a pod on `playground` and you reproduce that today.

The wider world says the same in bigger numbers. GitHub's October 2018 incident: a **43-second** partition left East and West each holding writes the other never saw, and recovery took over **24 hours**. The partition was seconds; the cleanup was a day. Orchestrator issue #854 shows a *graceful* takeover split-brained anyway, because the new master was made writable before the old one was set read-only, leaving the demoted master holding transactions the cluster never got. And Pacemaker on quorum: its loss *"can take an unbounded amount of time to detect and react to… The ultimate cure is to use fencing and lock the other side out."* Bloodraven agrees. The fencing layer is that agreement made executable.

## Recovering `playground` from a split brain

Four steps, in order:

1. **Pick a winner.** By policy if `sitePriorities` is set, by hand otherwise. There is no freshest — decide whose writes are authoritative.
2. **Fence the loser.** `SET GLOBAL super_read_only = ON`. It blocks rather than cuts off; surviving sessions can still serve stale reads until the site is next demoted.
3. **Audit the divergence.** `status.sites[].divergentGtid` holds the exact set the loser has and the winner never saw; the `bloodraven_divergent_transactions` gauge holds the count. The condition reason is `DivergentTransactions` and its message names the annotation you need.
4. **Reclone.** The `bloodraven.shipstream.io/reclone-site` annotation you already met — `<siteName>:<divergentGtidPrefix>`, at least 8 characters, matched against the observed `divergentGtid` so you cannot fat-finger it.

You can now state which tier `playground` is on, justify or change its `sitePriorities`, and run pick-fence-audit-reclone without guessing. What you cannot yet say is what happens to any of it when the operator itself is not there to run it.
