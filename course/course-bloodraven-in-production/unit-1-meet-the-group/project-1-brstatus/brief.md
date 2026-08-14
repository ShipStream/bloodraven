# Project — brstatus, the one-screen status reader

**Unit 1 — Meet the group** · type: `code-notebook` · Python 3.13, standard library only

## Goal

Write a tool that turns a `MysqlFailoverGroup` status into a one-screen summary and a meaningful
exit code, so that from Unit 2 onward you can tell at a glance whether `playground` is healthy — and so
you learn, the hard way, that a lagging reader is not an unhealthy group.

## How this works

`playground` is your three-site failover group: `iad` and `pdx` are `primary-candidate`, `reader` is
`read-only`. From Unit 2 onward you will look at its status constantly, and
`kubectl get mysqlfailovergroup playground -o json` is 400 lines. `brstatus` squeezes it to one screen:

```
playground/bloodraven-playground  active=iad  ready=True  degraded=False(Healthy)
SITE    ROLE               STATE      REPL  LAG      SERVING
iad     primary-candidate  writable   no    unknown  no
pdx     primary-candidate  read-only  yes   0s       yes
reader  read-only          read-only  yes   0s       yes
VERDICT: OK
```

And it exits with a code you can act on:

| Exit | Verdict | Condition |
|---|---|---|
| 0 | `OK` | the `Degraded` condition is not `True` |
| 1 | `DEGRADED` | `Degraded` is `True` and `status.activeSite` is set |
| 2 | `CRITICAL` | `Degraded` is `True` and `status.activeSite` is empty |
| 3 | — | the input was not one MysqlFailoverGroup |

Exit 2 is the shape of split brain and of no-primary: in both, no site is the unambiguous authority,
so `status.activeSite` is empty.

Everything runs against JSON fixtures in `tests/fixtures/`. No cluster is needed to finish the code.

## Your tasks

Open `starter/brstatus.py`. Run it first, before you change anything:

```bash
python starter/brstatus.py tests/fixtures/playground-healthy.json
```

It runs. It is also wrong in three ways, and each one is a `TODO`.

**TODO A — `format_lag`.** `status.sites[].secondsBehindSource` is a pointer, and it is absent
whenever the operator has no replication reading for that site: the active primary, a site it could
not poll, a replica whose threads are stopped. Return `"unknown"` when it is absent or null and
`"<n>s"` otherwise. The starter prints `0s` for absent, which is how a detached replica ends up
looking perfectly caught up.

**TODO B — `is_serving`.** Decide whether a site is currently behind `mysql-playground-replicas`, the
group read endpoint. That Service selects on three labels — instance, `role=replica`, and
`healthy=yes` — and the operator's rule for stamping `healthy` depends on the site's role.

For a site whose role is `read-only`, all five must hold together:

1. `sourceConvergenceState` is `Converged`
2. `replicating` is true
3. `secondsBehindSource` is present (not null)
4. `canonical_host(sourceHost)` equals `expected_source_host(...)` for the active site — a replica
   chained off another replica does not count
5. `secondsBehindSource` is at or under `effective_readonly_max_lag(spec)`

For **every other role**, `healthy=yes` as soon as `state` is `writable` or `read-only`. There is no
lag gate on those sites at all. That asymmetry is real and it is the point of this project.

**TODO C — `verdict`.** Return `(word, exit_code)` from the table above. Read the `Degraded`
condition the operator already wrote — its `reason` is one of `Healthy`, `Degraded`, `SplitBrain`,
`NoPrimary`, `TotalLoss`, or a replication reason such as `ReplicationLagging`. Do not re-derive
group health from the site rows.

Then do the cluster half: with `playground` up, capture a live status and read it with your own tool.

```bash
mkdir -p artefacts
kubectl -n bloodraven-playground get mysqlfailovergroup playground -o json > artefacts/playground-live.json
python starter/brstatus.py artefacts/playground-live.json
```

## What the scaffolding is for

You do not have to write any of this, but you do have to call it:

- `site_role(site_spec)` applies the CRD default — `spec.sites[].role` is optional and defaults to
  `primary-candidate`.
- `effective_max_lag(spec)` is `spec.replication.maxLagSeconds`, defaulting to 300.
- `effective_readonly_max_lag(spec)` is `spec.replication.readOnlyMaxLagSeconds`. It has **no
  default of its own**: absent inherits `maxLagSeconds`, but an explicit `0` is meaningful and means
  the reader must report zero lag. `.get("readOnlyMaxLagSeconds") or ...` gets that wrong.
- `condition(status, "Degraded")` pulls one entry out of `status.conditions`.
- `canonical_host()` and `expected_source_host()` compare replication source hosts the way the
  operator does — lowercased, `:3306` stripped, trailing dot stripped.
- Inside `is_serving`, two guards are already written for you: an empty `status.activeSite` sheds
  every endpoint, and the active site itself is never serving, because it carries `role=primary`
  rather than `role=replica`.
- `render()` sizes the columns. Print whatever you like around it; the tests parse cells, not widths.

## Expected output

With all three TODOs done, `tests/fixtures/playground-reader-soaking.json` — where `reader` is 300
seconds behind a 30 second threshold — must produce this, and exit **0**:

```
playground/bloodraven-playground  active=iad  ready=True  degraded=False(Healthy)
SITE    ROLE               STATE      REPL  LAG      SERVING
iad     primary-candidate  writable   no    unknown  no
pdx     primary-candidate  read-only  yes   0s       yes
reader  read-only          read-only  yes   300s     no
VERDICT: OK
```

And `tests/fixtures/playground-candidate-lagging.json` — where `pdx` is 300 seconds behind the same
threshold — must exit **1**, with `pdx` still marked `SERVING yes`.

One site 300 seconds behind, two opposite answers. Work out why before you write the code.

Run everything:

```bash
python tests/test_brstatus.py
```

## Rules

- Standard library only. No `kubernetes` client, no `pyyaml`, no network.
- Do not edit anything in `tests/`. Fixtures and checks are the grader's copy.
- Keep the function names `format_lag`, `is_serving` and `verdict` — a structural check looks for
  them.
- Every threshold comes from the object you were handed. Do not hard-code `30`; the shipped default
  is 300 and the playground overrides it.
- `secondsBehindSource` of `0` and `secondsBehindSource` absent are different facts. Never collapse
  them.

## Steps

- [ ] **Run the starter and find the lie.** Run
      `python starter/brstatus.py tests/fixtures/playground-healthy.json` before changing anything. Look
      at the `iad` row: it is the writable primary, it reports no replication lag at all, and the
      tool prints `0s`.
      *Done when:* the command runs to completion and the `iad` row shows `0s` in the LAG column.
- [ ] **TODO A — absent lag is not zero lag.** Complete `format_lag`.
      *Done when:* for `playground-healthy.json` the `iad` row shows `unknown` and `pdx` shows `0s`.
- [ ] **TODO B — the reader rule and the candidate rule are different rules.** Complete `is_serving`.
      *Done when:* for `playground-reader-soaking.json` the `reader` row shows `SERVING no` and `pdx`
      shows `yes`; for `playground-candidate-lagging.json` the `pdx` row shows `SERVING yes`.
- [ ] **TODO C — the verdict and the exit code.** Complete `verdict`.
      *Done when:* `brstatus.py` exits 0 on `playground-healthy.json`, 1 on
      `playground-candidate-lagging.json`, and 2 on `playground-no-primary.json`, whose last line reads
      `VERDICT: CRITICAL`.
- [ ] **Point it at the real `playground`.** Capture
      `artefacts/playground-live.json` from the running playground and read it with your own tool.
      *Done when:* the file exists, contains `"kind": "MysqlFailoverGroup"`, and
      `python starter/brstatus.py artefacts/playground-live.json` exits 0, 1 or 2 rather than 3.
- [ ] **Write down the inversion.** Add a comment at the bottom of `starter/brstatus.py`, on a line
      starting `# READER-LAG:`, naming the fixture that exits 0 and saying in one sentence why a
      lagging reader leaves the group healthy while a lagging primary candidate does not.
      *Done when:* `starter/brstatus.py` contains a line beginning `# READER-LAG:` that names
      `playground-reader-soaking.json`.

## Grading

Four automated checks carry 100 points between them, and a human marks the five criteria in
[`rubric.md`](rubric.md). Read the rubric before you start — the craft criterion is worth 20 and is
the one people leave on the table.

## Without Python — the `jq` route

If you would rather not write Python, the reading half of this project is a `jq` filter, and it is
closer to what you would actually type at 03:00. It will not teach you the `is_serving` asymmetry —
that is what the Python is for — but it gets you a one-screen summary today:

```bash
kubectl -n bloodraven-playground get mysqlfailovergroup playground -o json | jq -r '
  .spec  as $spec |
  .status as $st  |
  ($spec.replication.maxLagSeconds // 300)                      as $maxlag |
  ($spec.replication.readOnlyMaxLagSeconds // $maxlag)          as $rolag  |
  ([$st.conditions[]? | select(.type=="Degraded")][0] // {})    as $deg    |
  "\(.metadata.name)/\(.metadata.namespace)  active=\($st.activeSite // "-")  " +
  "ready=\([$st.conditions[]?|select(.type=="Ready")][0].status // "-")  " +
  "degraded=\($deg.status // "-")(\($deg.reason // "-"))",
  (["SITE","ROLE","STATE","REPL","LAG"] | @tsv),
  ( $st.sites[]? as $s
    | ([$spec.sites[] | select(.name == $s.name)][0].role // "primary-candidate") as $role
    | [ $s.name, $role, ($s.state // "-"),
        (if $s.replicating == null then "-" else ($s.replicating|tostring) end),
        (if $s.secondsBehindSource == null then "unknown"
         else "\($s.secondsBehindSource)s" end) ] | @tsv )
' | column -t
```

Three things in that filter are the same three traps the Python version is built around, and they are
worth reading rather than pasting.

**`// 300` on `maxLagSeconds`, and `// $maxlag` on `readOnlyMaxLagSeconds`.** The first is the CRD
default. The second is *inheritance*, not a default — and note that `//` in `jq` treats `false` and
`null` as absent but **not** `0`, which is exactly the behaviour this field needs: an explicit `0` is
meaningful and must survive.

**`if $s.secondsBehindSource == null then "unknown"`.** Never `// 0`. Absent and zero are different
facts, and collapsing them is how a detached replica ends up looking perfectly caught up.

**`$spec.sites[] | select(.name == $s.name) | .role`.** Role is not in status; it lives in the spec,
and joining the two yourself is the whole reason a lagging reader reads differently from a lagging
candidate.

What this does *not* give you is `SERVING` — whether a site is currently behind
`mysql-playground-replicas` — because that needs the five-conjunct rule and a canonical host
comparison. If you want it in shell, that is the exercise; if you want it explained, that is `TODO B`.
