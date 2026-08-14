# Fencing forensics

**Unit 5 — When the world misbehaves · project · `code-notebook` · Python**

> **Optional.** The heaviest project in the course, and the most specialised. Nothing later depends on
> it. Skip it if you are short of time, and take the deliverable instead of the code: a read-only site
> is explained by exactly one of three causes — rule #1 (a live operator disagrees about the active
> site), rule #2 (operator *and* every peer silent past `leaseTimeout`), or the startup safety net (the
> pod never got permission) — and the log prefixes tell them apart. `safety net:` means never allowed
> to write; `SELF-FENCING:` / `SELF-FENCED:` means was writing and lost the argument. Being able to say
> which, from a log bundle, is the whole objective.

## Goal

Build a timeline tool that reads a bundle of operator and sidecar logs from an injected fault and
reports every fence, which rule caused it, and whether it was correct — so that a read-only site you
did not expect becomes a question you can answer from evidence rather than a guess.

You will:

1. Attribute every fence in a log bundle to the rule or safety net that caused it.
2. Judge from evidence whether a fence was correct, premature, or never happened at all.

Everything runs against the JSON-lines fixtures in `tests/fixtures/`. You do not need a live
cluster, and neither does the grader.

---

## How this works

A site on `playground` is read-only and you did not put it there. That is the whole problem. Three
different mechanisms could have done it — the sidecar's rule #1, the sidecar's rule #2, or the
startup safety net — plus three ways the operator fences a site itself, and they mean completely
different things. Rule #1 is a pod that was told somebody else is active. Rule #2 is a pod that
could not reach anyone. The safety net is a pod that was never allowed to write in the first place.
Same read-only outcome, three different incidents.

You have the evidence. Both binaries write structured JSON to stdout, and
`site/content/docs/8.observability/7.log-schema.md` is a stability contract: filter on `msg` and those strings will not move
without a deprecation note. So the forensics are mechanical, once you know the vocabulary.

You are building `brfence`. It takes one bundle directory:

```text
tests/fixtures/partition-a/
  operator.jsonl        the operator's operational (slog) stream
  sidecar-iad.jsonl     one file per site's bloodraven-sidecar container
  sidecar-pdx.jsonl
  sidecar-reader.jsonl
```

and prints one timeline of every fence, with its cause and a verdict, plus the writable sites
nobody fenced at all. Exit 0 when everything checks out, 1 when it does not.

Three bundles ship with the project, all from `playground`:

| Bundle | What it is |
|---|---|
| `tests/fixtures/partition-a` | A shape-A partition. `iad` is isolated, `pdx` is promoted in 12.0 s, `iad` self-fences on the lease, the `reader` pod restarts, and `iad` comes back writable for two seconds before its own monitor catches it. |
| `tests/fixtures/split-brain-tier3` | A split brain on a group with no `sitePriorities` — tier 3, alert only. One sidecar log was never collected. One fence is not supported by its own evidence. |
| `tests/fixtures/decoys` | Thirteen records, eleven of which contain the string `fenc`, and exactly one fence. |

## Your tasks

**TODO A — `classify(rec, stream)`.** Return one of six cause ids for a record that is a fence
*decision*, and `None` for everything else. The vocabulary is in the fence-vocabulary reference. Match
the whole `msg`. `SELF-FENCING:` is a stable prefix, not a synonym for "a fence happened" — it also
heads `SELF-FENCING: killed app connections` (a follow-up) and `SELF-FENCING FAILED: could not set
super_read_only` (a fence that did not land).

**TODO B — `fenced_site(rec, cause, file_site)`.** Return the site that got fenced. Records disagree
about how to say it: rule-1 and the operator's `old-primary` and `non-promotable` lines carry
`site`, the split-brain line carries `fencedSite`, and the rule-2 and two of the three safety-net
lines carry no site at all. The loader hands you `file_site` — `iad` for `sidecar-iad.jsonl`, `None`
for the operator stream — for exactly that case.

**TODO C — `judge(rec, cause, site)`.** Return `"correct"` or `"premature"` using the verdict-table
reference. Rule-2 is the one that earns its keep: the lease fence may only fire
when the operator **and every peer** have been silent for the whole `leaseTimeout`, so subtract
`bloodravenLastOk` and `latestPeerOk` from the record's own `time` and compare. A peer that answered
four seconds ago means the fence should not have happened. `parse_time()` and `parse_duration()` are
already written.

**TODO D — `unfenced_writable_sites(records, fences)`.** The operator emits `msg="ALERT"` with a
`message` field, and a split-brain alert reads exactly `SPLIT BRAIN: 2 sites are writable (iad,
pdx)`. Take the names from inside the parentheses of every such alert. Drop any site that has a
fence event anywhere in the bundle, and drop the site named by the most recent `failover complete`
`promotedSite` at or before the alert — that site holds primary authority and fencing it would be
the bug. Return `(site, alert_message)` pairs, each site once, in first-seen order.

## What the scaffolding is for

Everything that is not forensics is already wired, so you spend your time on the discriminations
rather than on plumbing:

- `load_bundle()` reads every `*.jsonl`, decodes it, skips controller-runtime (`zap`) records by
  checking for `time`/`msg`, sorts by event time, and labels each record with its stream and the
  site taken from the filename.
- `parse_time()` handles the RFC3339Nano stamps the binaries emit. `parse_duration()` handles
  `slog`'s duration rendering (`"20s"`).
- `evidence()`, `report()` and the exit code are done. **Do not change the report format** — the
  tests read it. `group_name()` pulls `playground` out of the `fg` field.
- The four TODOs already return placeholder values, so the starter runs before you touch it. It
  reports zero fences on a bundle that plainly contains several.

## Expected output

```text
FENCE TIMELINE — playground (bundle: split-brain-tier3)
  11 records scanned, 3 fence events

  2026-08-12T14:01:03.505Z  reader   safety-net     correct    error=Get "http://…/active-site": context deadline exceeded
  2026-08-12T14:02:15.130Z  reader   non-promotable correct    site=reader role=read-only
  2026-08-12T14:04:20.880Z  iad      rule-2         premature  bloodravenLastOk=… latestPeerOk=… leaseTimeout=20s

UNFENCED WRITABLE SITES
  pdx  — writable per ALERT "SPLIT BRAIN: 2 sites are writable (iad, pdx)", no fence event in this bundle

VERDICT: 3 fences, 1 premature, 1 unfenced writable site
```

`tests/fixtures/partition-a` gives three fences, all `correct`, `(none)` unfenced, exit 0.
`tests/fixtures/decoys` gives exactly one fence, exit 0.

## Rules

- Stdlib only. No network, no cluster, no `kubectl` — the bundles are the whole world.
- Key on the full `msg` string. Substring and prefix matching are graded down and one test exists
  purely to catch them.
- The rule-1 msg contains a real em dash (U+2014) between `mismatch` and `operator-authoritative`.
  Copy it, do not retype it as a hyphen.
- Do not hardcode a site name, a fixture path, or an expected count. The tests run three bundles
  and a fourth would be fair.
- Do not change the report format or the exit-code rule.
- Work in a copy of `starter/brfence.py` named `brfence.py` at the project root — the tests look
  for it there first.
- Run `python3 tests/test_brfence.py` from the project directory when you think you are done. It is
  the same code the grader runs.

---

## Reference: The fence vocabulary

Six causes. Eight `msg` strings. These are the stable identifiers from the log-schema contract —
match the **whole** string, in the stream named.

| Cause id | Stream | `msg` (verbatim) |
|---|---|---|
| `rule-1` | sidecar | `SELF-FENCING: topology mismatch — operator-authoritative active site disagrees with our site, setting super_read_only=ON` |
| `rule-2` | sidecar | `SELF-FENCING: Bloodraven and every peer unreachable beyond lease timeout, setting super_read_only=ON` |
| `safety-net` | sidecar | `safety net: could not query active site, staying fenced` |
| `safety-net` | sidecar | `safety net: no active site reported by operator, staying fenced` |
| `safety-net` | sidecar | `safety net: confirmed standby site, staying fenced` |
| `split-brain` | operator | `split-brain auto-resolve: fencing non-preferred site per spec.splitBrainPolicy.sitePriorities` |
| `old-primary` | operator | `fencing returning old primary (split brain after failover)` |
| `non-promotable` | operator | `fenced writable non-promotable site` |

The em dash in the `rule-1` string is U+2014, not a hyphen.

### Not a fence decision

Every one of these contains `fenc` and none of them is an event you count. They are in the fixtures
on purpose.

| `msg` | What it actually is |
|---|---|
| `SELF-FENCED: super_read_only=ON has been set, only Bloodraven can restore` | The status line that *follows* a fence. Counting it doubles every self-fence. |
| `SELF-FENCING FAILED: could not set super_read_only` | A fence that did not land. The sidecar retries next tick. |
| `SELF-FENCING: killed app connections` | Eviction after a fence that already succeeded. |
| `SELF-FENCING: failed to kill connections after fencing` | The fence holds; eviction was incomplete. |
| `SELF-FENCING: super_read_only write failed but the fence is in place; skipping connection eviction` | The write errored but a follow-up read shows it landed. |
| `failed to fence old primary (may be unreachable)` | Step 1 of the failover sequence missing an unreachable host. It only warns. |
| `failed to fence returning old primary` | The operator's fence write was rejected. |
| `fencing: adopted active-site view from peer` | A peer relayed a fresher topology view. This is what *drives* rule #1, not rule #1. |
| `fencing: MySQL is writable after prior self-fence; rearming monitor` | The opposite of a fence. |
| `fencing: could not check read_only status` / `fencing: could not confirm whether the super_read_only write landed` | Probe failures. |
| `re-asserting fenced promoted primary: …` | The operator *removing* a fence. |

### The fields you need

Common to every operational record: `time` (RFC3339Nano, always `Z`), `level`, `msg`, `fg`.
Sidecar records also carry `pod`. Then, per cause:

| Cause | Site named by | Evidence fields |
|---|---|---|
| `rule-1` | `site` | `authoritativeActiveSite`, `observedAt` |
| `rule-2` | *nothing* — use the filename | `bloodravenLastOk`, `latestPeerOk`, `peers`, `leaseTimeout` |
| `safety-net` | `site`, but only on `confirmed standby site` | `activeSite`, `error` |
| `split-brain` | `fencedSite` | `winner` |
| `old-primary` | `site` | — |
| `non-promotable` | `site` | `role` |

---

## Reference: The verdict table

A fence is `correct` when the record's own fields support the rule that fired, and `premature` when
they contradict it. You are not second-guessing the operator's judgement — you are checking whether
the evidence it logged actually meets the condition it claims to have met.

| Cause | `correct` when | Why |
|---|---|---|
| `rule-1` | `authoritativeActiveSite` is non-empty and is not the fenced site | Rule #1 is exactly "the authoritative active site is somebody else". If the record names this same site, the fence has no premise. |
| `rule-2` | `time − bloodravenLastOk ≥ leaseTimeout` **and** (`latestPeerOk` absent **or** `time − latestPeerOk ≥ leaseTimeout`) | The lease fence requires the operator **and every peer** silent for the full window. One reachable peer keeps the primary writable. A missing `latestPeerOk` means no peer ever answered — that is silence, so it counts as met. |
| `safety-net` | always | The safety net fails closed by *staying* fenced. It never takes writability away from a site that had it, so it cannot fire too early. |
| `split-brain` | `winner` is non-empty and is not the fenced site | Tier 2 fences the losers and re-promotes the winner. A record that fences its own winner is broken. |
| `old-primary` | always | The operator fences every writable site except the one holding live primary authority. The record carries nothing that could contradict it; read the timeline around it. |
| `non-promotable` | `role` is not `primary-candidate` | Promotability is exactly `role == primary-candidate`. A writable `dr-only` or `read-only` site is never authoritative and is fenced on every poll. Anything else in `role` means the wrong site was fenced. |

`leaseTimeout` arrives as `slog`'s duration rendering — the string `"20s"`. `parse_duration()`
handles it. The shipped defaults are `leaseTimeout: 20s` and `peerCheckInterval: 5s`, so a lease
covers four consecutive ticks of silence.

### The fence that never happened

There is a fourth possibility the timeline alone will not show you: nobody fenced anything. On a
group with no `splitBrainPolicy.sitePriorities` and no usable failover history, tier 3 is
**alert only** — the operator emits `SPLIT BRAIN: n sites are writable (…)` every poll and takes no
action, by design and by field documentation. That is a correct operator and an unresolved split
brain at the same time, and the only way to see it in a bundle is to notice which writable sites
never appear in the fence timeline.

Two sites you must not report as unfenced: one that *was* fenced later in the bundle, and the site
named by the most recent `failover complete` `promotedSite`, which is the one legitimately holding
primary authority.

---

## Steps

- [ ] **Run the starter and see what it misses**

      Copy `starter/brfence.py` to `brfence.py` at the project root and run `python3 brfence.py tests/fixtures/partition-a`. It already loads the bundle, playground it by time and prints a report. It finds nothing. Open `tests/fixtures/partition-a/sidecar-iad.jsonl` and confirm by eye that the bundle contains two `SELF-FENCED:` lines the tool is silent about. That gap is TODO A through TODO D.

      *Done when:* `python3 brfence.py tests/fixtures/partition-a`, unmodified, exits 0 and its second line ends with `0 fence events`.

- [ ] **TODO A — key on the stable fence vocabulary**

      Fill in `classify()`. Six cause ids, eight `msg` strings, listed in the fence-vocabulary reference. Match the whole `msg`, not a prefix and not a substring: `SELF-FENCING:` is a stable prefix but it also heads `SELF-FENCING: killed app connections` and `SELF-FENCING FAILED: could not set super_read_only`, neither of which is a fence decision. Note the em dash (U+2014) inside the rule-1 string.

      *Done when:* `python3 brfence.py tests/fixtures/partition-a` reports `3 fence events`, and `brfence.py` contains the full `SELF-FENCING: topology mismatch …` and `SELF-FENCING: Bloodraven and every peer unreachable …` msg strings verbatim.

- [ ] **TODO B — name the site that got fenced**

      Fill in `fenced_site()`. `rec["site"]` is not enough. The rule-2 lease-expiry record has no `site` key at all — it carries `peers` and `pod` — and the operator's split-brain line calls the field `fencedSite`. Fall back to `file_site`, the site name the loader already took from the `sidecar-<site>.jsonl` filename.

      *Done when:* the three timeline lines for `tests/fixtures/partition-a` name the sites `iad`, `reader`, `iad` in that order, and no line has `?` in the site column.

- [ ] **TODO C — check each fence against its own evidence**

      Fill in `judge()` from the verdict-table reference. The one with teeth is rule-2: it may only fire when the operator *and* every peer have been silent for the whole `leaseTimeout`, so a record whose `latestPeerOk` falls inside its own window is a fence that should not have happened. That is objective 2 turned into arithmetic — one reachable peer keeps a primary writable.

      *Done when:* `python3 brfence.py tests/fixtures/split-brain-tier3` marks its `rule-2` line `premature` and prints `1 premature`, while every line for `tests/fixtures/partition-a` stays `correct`.

- [ ] **TODO D — find the fence that never happened**

      Fill in `unfenced_writable_sites()`. Read the site names out of every `msg="ALERT"` record whose `message` begins `SPLIT BRAIN:`, then drop the sites that do have a fence event and drop the site named by the most recent `failover complete` `promotedSite` — that site holds primary authority, and fencing it would be the bug. What is left is the tier-3 signature: an alert, and nobody acting on it.

      *Done when:* `python3 brfence.py tests/fixtures/split-brain-tier3` lists `pdx` and only `pdx` under `UNFENCED WRITABLE SITES` and exits 1, while `tests/fixtures/partition-a` prints `(none)` and exits 0.

- [ ] **Run it against the decoy bundle and write down what you noticed**

      `tests/fixtures/decoys` is thirteen records, eleven of which contain the string `fenc`, and exactly one of which is a fence. Run your tool against it, then add a comment above `classify()` naming `SELF-FENCED:` and saying in one sentence why a line that announces a fence is not the fence event. This is the difference between a log pipeline that counts incidents and one that counts lines.

      *Done when:* `python3 brfence.py tests/fixtures/decoys` reports `1 fence event`, and `brfence.py` contains a comment mentioning `SELF-FENCED:`.

---

## Grading

Graded three ways: the steps above, the human rubric in [`rubric.md`](./rubric.md), and the four
machine test cases in `project.json` — mirrored in `tests/test_brfence.py`, which you can run
yourself at any time.
