# brprep — the day-0 pre-flight and the change plan

*Unit 7 project — Day 0 and day 2. No Python, no cluster: `bash` and `jq` against JSON fixtures.*

## Goal

Write a shell pre-flight that reads a `MysqlFailoverGroup` manifest and reports what the API server
will reject, what it will silently admit, and what an ordered update of it would actually do — so the
first time anyone sees your production group's problems is before it is applied, not during an
incident.

## How this works

Topic 1 of this unit drew the line that matters on day 0: **admission validates the object, and
nothing validates the object against the cluster.** A manifest that sets both credential modes is
refused in milliseconds. A manifest whose `taintNodeSelector` names labels no node carries is
accepted, applied, and silently does nothing for the rest of its life.

`brprep` makes that line executable. It reads a manifest as JSON and prints three kinds of line:

| Line | Meaning | Effect on the exit code |
|---|---|---|
| `REJECT <rule>` | the API server refuses this object — a CEL rule or a schema bound | exit `1` |
| `WARN <finding>` | admission accepts it, and it will hurt later | none — exit stays `0` |
| the change plan | what an ordered update to `--target-image` would do | none |

That split is the point of the project. A tool that treats every finding as an error is a tool people
turn off.

```bash
./starter/brprep.sh tests/fixtures/good.json --target-image mysql:9.8
```

Everything runs against the eight manifests in `tests/fixtures/`. No cluster is needed to finish the
code. If you have a live group, `kubectl get mysqlfailovergroup <name> -o json` produces exactly the
input shape, and so does `yq -o json < manifest.yaml`.

## Your tasks

Open `starter/brprep.sh`. Three functions are stubbed, marked `TODO A`, `TODO B` and `TODO C`. Run the
grader first, before you change anything:

```bash
./tests/run.sh
```

It reports nine failing fixtures. Read the diff for `good` first — that is the shape everything else
is measured against.

**TODO A — `check_admission`.** Print one `REJECT <rule>` line per admission rule the manifest
violates, in the order the `RULE_*` constants are declared. A clean manifest prints nothing at all. The nine rules
are documented in the script — seven CEL rules on the CRD plus the `MinItems`/`MaxItems` bound on
`spec.sites`, all of them refusals at `kubectl apply` — and two are worth flagging here.

*The role default is load-bearing.* `spec.sites[].role` is optional and defaults to
`primary-candidate`, so a site with no `role` key counts toward the two-candidate minimum and is
required to carry `lbIP` and `taintNodeSelector`. Reading a missing role as "not a candidate" fails
half the fixtures.

*The reader exemption is the discriminator.* `taintNodeSelector` and `lbIP` are required **unless**
the role is `read-only` — because a reader is never promoted and never tainted, so it needs neither.
`tests/fixtures/reader-lbip.json` has a legitimate reader without them *and* a `primary-candidate`
missing both. A check that demands them unconditionally fails it; a check that never demands them
fails too.

**TODO B — `check_silent`.** Print one `WARN <finding>` line per day-0 mistake nothing validates, using
the `WARN_*` templates verbatim. Six findings, documented in the script:

- a floating image tag — `mysql:9` and `mysql:latest` float, `mysql:9.7` does not
- a backup profile on `storage.type: PVC`, named
- a site whose `requests` do not equal its `limits`, named — no Guaranteed QoS, so the kubelet may
  evict your primary first
- `readOnlyMaxLagSeconds` above `maxLagSeconds`
- `encryptionAtRest` enabled with no backup profile at all
- `updateStrategy: Recreate`

None of these may change the exit code.

**TODO C — `plan_upgrade`.** Turn `--target-image` into a written plan: which standby is upgraded
first and why, that a real promotion follows, which site ends up active, and the three observable
consequences — a `bloodraven_failovers_total` increment, a `lastFailover` stamp that consumes the
anti-flap budget, and a DNS flip. Print `no change` when the target already matches `spec.image`.

A plan that says "the pods will restart" is not a plan. The whole reason this belongs in a change
record is that a routine image bump moves your primary and fires a failover alert, and somebody is
going to be woken up by it.

## What the scaffolding is for

You do not have to write any of this:

- argument parsing, the `MysqlFailoverGroup` kind check, and the exit-code plumbing
- `q <jq-filter>`, a one-line reader over the manifest — use it for every field access; `grep` over
  JSON is how a checker starts lying to you
- `secs <duration>`, Go-duration to whole seconds, so the `spec.sidecar` interval rules are arithmetic
  rather than string comparison. It returns `0` for an empty or unparsable value, which is never a
  valid setting and therefore always reads as "violates the minimum"
- the exact `RULE_*` and `WARN_*` strings. The grader matches these tokens, so do not reword them
- the report assembly and the `verdict:` line

## Expected output

A clean manifest with a plan:

```text
$ ./starter/brprep.sh tests/fixtures/good.json --target-image mysql:9.8
brprep: ledger-db/ledger
plan: mysql:9.7 -> mysql:9.8
1. upgrade standby pdx first (a replica may run a newer MySQL than its source; a source may not run a newer MySQL than its replica)
2. promote pdx through the ordinary nine-step sequence
3. upgrade the former active iad, now a standby
active site after this rollout: pdx
expect: bloodraven_failovers_total increments
expect: lastFailover is stamped and consumes the anti-flap cooldown
expect: the DNSEndpoint A record flips
verdict: APPLYABLE (0 finding(s))
$ echo $?
0
```

And a manifest that will apply cleanly and then disappoint you:

```text
$ ./starter/brprep.sh tests/fixtures/silently-wrong.json
brprep: ledger-db/ledger
WARN spec.image is a floating tag; pin an immutable one or a restart can drift you onto an unsupported MySQL
WARN backup profile nightly uses storage.type PVC; a backup sharing a failure domain with the data is an assumption, not a backup
WARN site iad sets resources.requests != resources.limits; without Guaranteed QoS the kubelet may evict this MySQL first
WARN replication.readOnlyMaxLagSeconds (300) is above maxLagSeconds (30); the reader endpoint is now looser than the group threshold
WARN updateStrategy Recreate patches every site Deployment in one pass; both sites can restart at once
verdict: APPLYABLE (5 finding(s))
$ echo $?
0
```

Five findings, zero rejections, exit `0`. That manifest is a perfectly valid `MysqlFailoverGroup` and
a bad idea, and no cluster anywhere will tell you so.

Then the cluster half, if you have one:

```bash
kubectl -n bloodraven-playground get mysqlfailovergroup playground -o json > playground-live.json
./starter/brprep.sh playground-live.json --target-image mysql:9.8
```

The playground group will show findings — it sets `failoverCooldown: 30s` and `maxLagSeconds: 30`
precisely so experiments finish while you are watching. Read them and decide which ones you would
carry into production and which are playground-only. That decision is the Unit 6 go-live gate, arrived
at from the other direction.

## Rules

- `bash` and `jq` only. No Python, no `kubectl`, no network, no `yq` at grading time.
- Read every field through `q` / `jq`. A regex over JSON works on the fixtures and fails on the first
  real manifest with different key ordering or a multi-line value.
- Keep `set -euo pipefail`, and keep the function names — the grader calls the script, but a
  structural check looks for them.
- Findings never change the exit code. Rejections always do.
- Output must be stable and diffable: no timestamps, no absolute paths, no colour.
- Do not edit anything under `tests/`. The fixtures and expectations are the grader's copy; changing
  them changes the question rather than the answer.
