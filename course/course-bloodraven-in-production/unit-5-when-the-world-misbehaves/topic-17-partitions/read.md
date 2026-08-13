# Five partitions, five answers

`playground` is healthy on your three-site k3d playground: `iad` writable, `pdx` read-only and
replicating, the counter app reading and writing without complaint. Then the page: "the network is partitioned."
That is not a diagnosis. Five different failures wear it, and they do not share a response — one
promotes, three deliberately do nothing, and one has never been injected in a test.

## Five shapes, not one

Bloodraven documents the five in `docs/docs/network-partitions.mdx`, labelled A to E. What separates
them is not how bad the network is. It is *which* link broke, and so what each of two independent
deciders can still see. The operator decides from what it can poll — one `SELECT @@read_only` per
site, per tick. The sidecar decides from what it can reach — Bloodraven, or any peer. A partition
rarely blinds both. The widget below is the taxonomy; the list is *why* each answer is right.

- **A — operator cannot reach site A, site B reachable.** `iad` goes `unreachable` after 6 s (2 s
  `pollInterval` × 3 `failureThreshold`); `pdx` is promoted. An isolated-but-alive `iad` self-fences
  at roughly T+20 s, the shipped `leaseTimeout` the playground does not override.
- **B — replica isolated, primary reachable.** Promoting away from a healthy primary costs
  availability and risks data loss for nothing. And a read-only instance never self-fences — there
  is no writability to take away.
- **C — MySQL-to-MySQL link broken, operator reaches both.** Both sites poll cleanly; only the
  binlog stream is severed. This one earns its own section.
- **D — asymmetric peer reachability.** `iad` reaches `pdx`, `pdx` cannot reach `iad`. One reachable
  peer keeps the primary writable, so nothing self-fences and nothing promotes.
- **E — both sites unreachable to the operator.** Total loss: no reachable candidate exists to
  promote at all.

```widget
{
  "type": "compare",
  "title": "The five documented partition shapes",
  "rows": [
    {
      "aspect": "Observed symptom in `playground`",
      "cells": [
        "`iad` → unreachable after 6 s; `pdx` stays read-only and replicating until the link fully breaks",
        "`pdx` → unreachable, or replication IO/SQL stops and lag climbs; `iad` stays writable",
        "Primary writable, replica read-only, IO thread stopped or lag rising — both sites poll fine",
        "Polls look healthy; replication or peer checks fail one way only",
        "Every site → unreachable; Degraded=True with total-loss semantics"
      ]
    },
    {
      "aspect": "Operator response",
      "cells": [
        "Promotes `pdx` via the normal emergency path; Services, DNS and taints follow",
        "None. The primary is healthy",
        "None. Indistinguishable from lag or IO pressure",
        "Follows what it can poll: no failover while the primary is reachable and writable",
        "No promotion — there is no reachable candidate"
      ]
    },
    {
      "aspect": "Sidecar response",
      "cells": [
        "`iad` self-fences at roughly T+20 s if isolated from operator *and* all peers",
        "No self-fence — read-only instances never self-fence",
        "No self-fence — the primary can still reach operator and peers",
        "Self-fences only if operator *and* every peer are unreachable; one reachable peer suppresses it",
        "Any still-running writable site self-fences after leaseTimeout"
      ]
    },
    {
      "aspect": "How it is actually tested",
      "cells": [
        "Exercised: chaos 09 and 06, plus DST fault `partitionOperatorSite`",
        "Exercised: chaos 17",
        "DST only — fault `partitionPair`; no live chaos scenario",
        "Analysis only — no test of any kind",
        "Indirect only, via chaos 11 (total loss by scale-to-0)"
      ]
    }
  ],
  "columns": [
    {
      "label": "A: operator↛site"
    },
    {
      "label": "B: replica isolated"
    },
    {
      "label": "C: MySQL↛MySQL"
    },
    {
      "label": "D: asymmetric"
    },
    {
      "label": "E: all sites lost"
    }
  ]
}
```

## Coverage, stated honestly

Two of the five have live-cluster chaos coverage. A: scenario 09 partitions the active pod with a
deny-all NetworkPolicy; scenario 06 reaches the same self-fence by scaling the operator and every
peer to zero. B: scenario 17, a negative-assertion suite that fails if partitioning the replica ever
*does* trigger a failover or a self-fence. C lives only in the deterministic simulator, as the
`partitionPair` fault. E is touched sideways by scenario 11, which reaches total loss by scaling
sites down, not by breaking a network.

D is **analysis only** — the simulator cannot express it. Its link state is keyed by a symmetric
`pairKey(a, b)`, which sorts the two site names before joining them, so "`iad` reaches `pdx`" and
"`pdx` reaches `iad`" are the same key. One-way reachability is unrepresentable in the fault model,
and everything above about D is argued from the code, never observed under injection. That is not an
apology. An untested failure mode is a known unknown you plan around; one you *believed* was tested
is the one that hurts.

## A broken replication link is not a failover

The instinct is to treat "replication is broken" as an emergency the operator should resolve. It
will not, and the reason is epistemic rather than lazy: from the operator's point of view this mode
is indistinguishable from "the replica fell behind because of I/O pressure". The evidence is
identical. Human judgement decides.

Connect it to the matrix you already know. The failover row needs zero writable sites, at least one
unreachable and at least one read-only. In scenario C `iad` is still writable, so that row is
unreachable by construction. Lag runs on a separate channel entirely:
`spec.replication.maxLagSeconds` (default 300, 30 in the playground) drives only the
`ReplicationLagging` Degraded condition, never a promotion. So your options are the human ones —
keep serving writes on `iad`, or wait for `pdx` to catch up and run a planned failover, which is
RPO 0 by construction. The operator declining to act here is the system working.

## Naive partition tests are no-ops

Two traps have already caught this project. Host-netns iptables rules on a k3d node do not partition
Kubernetes Service traffic: kube-proxy's DNAT happens in different chains, so the operator keeps
reaching MySQL through the ClusterIP while you believe you have severed it. And a NetworkPolicy can
be silently ineffective — chaos scenario 33 found this CNI evaluating the policy **post-DNAT**. The
rule excepted the kube-dns ClusterIP, but the packet's destination was already a CoreDNS *pod* IP by
the time the CNI saw it, so the exception never matched and DNS resolved through the entire
45-second hold.

```widget
{
  "type": "terminal",
  "title": "Chaos 33's canary — proving the injection before trusting it",
  "lines": [
    {
      "cmd": "while true; do if nslookup kubernetes.default.svc.cluster.local >/dev/null 2>&1; then echo PROBE dns=ok; else echo PROBE dns=fail; fi; sleep 2; done",
      "out": "PROBE dns=ok"
    },
    {
      "cmd": "# canary policy v1: except the kube-dns ClusterIP only, hold 45s",
      "out": "PROBE dns=ok\nPROBE dns=ok\nPROBE dns=ok"
    },
    {
      "cmd": "# canary policy v2: except the ClusterIP AND every CoreDNS backend pod IP",
      "out": "PROBE dns=fail"
    }
  ],
  "caption": "Recorded output. **Run** reveals what is already on the page — nothing executes, and no cluster is contacted."
}
```

The rule, hard: a chaos experiment that injects nothing produces a confident false pass, and a green
run of a broken experiment is worse than none. Every partition test needs an independent check that
the partition exists, run against a disposable canary before the real target is touched — scenario
33 now refuses to proceed unless its canary reaches `dns=fail`. The reported state can never be that
check: in Unit 2's frozen-poll incident the operator reported `activeSite=iad, state=writable,
Ready=True` for two minutes under a deny-all policy. Partitions surface as *believable* status, not
as errors.

## Why the fence has to come from inside

Partitions become a data-integrity topic the moment you ask what stops a partitioned primary from
writing. Four Kubernetes facts, none of them a bug:

1. Kubernetes will **not** delete pods merely because a node is unreachable. The pod sits
   `Terminating` or `Unknown` indefinitely — deliberately, to protect at-most-one identity.
2. Force-deleting it **breaks** at-most-one. The API object disappears while the process may still
   be running, and still writing, on the partitioned node.
3. `Terminating` is set by the API server, not the kubelet. On an unreachable node the container
   never gets the message: it keeps running and keeps writing to the PV.
4. `ReadWriteOnce` means one **node**, not one pod. Storage attach is not fencing.

The conclusion for `playground` is unavoidable: nothing above MySQL stops a partitioned
`iad` from writing — not the scheduler, not the API server, not the volume layer, and certainly not
you with a `kubectl delete --force`. That is why a partitioned site must fence *itself*.

## The on-call checklist

Work it in order.

1. **Confirm the partition is real** before believing any symptom — reachability from a third
   vantage point, never the operator's own status.
2. **Identify the shape.** `kubectl get mysqlfailovergroup playground -o jsonpath='{.status.activeSite}'`,
   then per-site state:
   `-o jsonpath='{range .status.sites[*]}{.name}: {.state} lag={.secondsBehindSource} recovery={.recoveryState}{"\n"}{end}'`.
   Which sites are unreachable, and is anything still writable? That maps to A–E directly.
3. **Read the sidecar logs** for which side fenced itself. `SELF-FENCED: super_read_only=ON has been
   set, only Bloodraven can restore` is the line; the `SELF-FENCING:` line above it names the rule.
4. **Decide whether the operator has an action at all.** Did `bloodraven_failovers_total` move? If
   not, it chose to alert rather than promote — in shapes B, C and D that is correct, not a stall.
5. **Only now decide whether to intervene.** If a site returns carrying `divergentGtid`, do not
   manually attach it as a replica; take the reclone path.

You can now name the shape from a symptom, you know which shapes are exercised and which are merely
argued, and you will verify an injection before trusting its result. Every shape here assumed the
operator was alive to watch it. Next: what keeps working when the operator pod is what goes away.
