# Make the writer survive

*Unit 4 project — Where failover meets your application. Running example: the `playground` failover
group on the three-site `bloodraven-playground`.*

## Goal

Instrument a writer against `playground`, run both a planned and an emergency failover underneath it, and produce a drill record with the measured write-gap for each — so the recovery time you claim for your application is one you observed rather than one you assumed.

## How this works

`playground` has moved its primary before. What you have never done is measure what that cost the
application. This project produces that number twice — once for a planned move and once for an
emergency one — and then makes you defend it.

The number is the **write-gap**: the interval between the last write your application completed
against the demoted site and the first it completed against the promoted one. Not the first
successful query. Not the first successful read. The first completed *write*. A read that succeeds
during the outage tells you the demoted host is alive — which it usually is — not that you recovered.

You are handed four captures from `bloodraven-playground`, each a pair:

- a **probe log** (`*.jsonl`), one line per write or read the writer attempted, with `ts`, `op`,
  `ok`, `site`, `dbHost`, `readOnly` and `error`;
- a **drill capture** (`*-drill.json`), holding what was triggered plus the group status afterwards:
  `activeSite`, `lastFailover`, `lastFailoverTarget`, and for a planned move
  `status.plannedFailover`.

| Capture | What happened |
| --- | --- |
| `emergency-probe.jsonl` + `emergency-drill.json` | `iad` scaled to 0, operator promoted `pdx`. Fixed writer: 30 s connection lifetime, read/write split, retry on 1290/1792 |
| `planned-probe.jsonl` + `planned-drill.json` | `bloodraven.shipstream.io/planned-failover=pdx`, same fixed writer |
| `planned-probe-unbounded.jsonl` + `planned-drill-unbounded.json` | the same planned procedure a day later, `pdx` -> `iad`, run against the **unfixed** writer: one pool for reads and writes, no lifetime bound, no split, no error-class retry |
| `unclosed-probe.jsonl` + `emergency-drill.json` | the emergency drill again, with the probe process killed before the gap ever closed |

If your playground is up, run both drills yourself and capture your own artefacts in the same shape —
`brdrill` reads either. If it is not, the supplied captures are real enough to do the work, and they
are what the grader uses.

## Your tasks

Open `starter/brdrill.py`. Four functions are stubbed, marked `TODO A` .. `TODO D`. Each returns
`None` today, which is why the starter prints `not computed` and exits 1.

**TODO A — `write_gap(samples, drill)`.** Find the last successful `write` on
`demoted_site(drill)` and the first successful `write` on `promoted_site(drill)` after it. Return
`oldSite`, `newSite`, `lastWriteOldSite`, `firstWriteNewSite`, `gapSeconds` (rounded to three
decimals) and `closed`. When either end is missing, `closed` is `False` and `gapSeconds` is `None` —
never `0`. A gap you did not observe is not a gap of zero.

**TODO B — `error_classes(samples)`.** Count failed samples into `readOnlyRefusal` (error code
1290 or 1792), `connection` (no error code at all — the transport failed, the server never
answered) and `other`. All three keys always present. This is the split your retry policy runs on:
retry the refusals, not the lock-wait timeouts.

**TODO C — `stale_read_window(samples, drill)`.** Count successful `read` samples served by the
demoted site at or after `promotion_instant(drill)`, and report `count`, `first`, `last` and
`seconds`. Two traps: `readOnly` is `true` on the `reader` site by design, so read-only is not the
same as stale; and a clean kill produces zero stale reads, because the host is gone.

**TODO D — `verdict(drill)`.** Return the RPO verdict, in exactly the wording given in the
docstring. A planned failover that reached `Succeeded` with `transactionsLost: 0` may claim RPO 0 by
construction — the operator fenced the source, snapshotted its `GTID_EXECUTED`, and promoted only
once the target's set *contained* that snapshot. An emergency failover may claim nothing of the
kind; its RPO comes from `divergentGtid` on the old primary, which this drill did not look at.

Then run the comparison, and write `starter/drill-record.md`.

## What the scaffolding is for

Everything that is not the measurement is already written: `argparse` wiring, JSONL and JSON
loading, RFC3339 parsing (`parse_ts`) and rendering (`iso`), timestamp-ordering of samples, the
`demoted_site` / `promoted_site` / `promotion_instant` accessors, record assembly in
`build_record`, both output formats, the `--baseline` comparison, and the exit codes (0 complete,
1 something not computed, 2 gap never closed). Do not rewrite them; they are there so the four
functions are the only thing being graded.

## Expected output

```
$ python3 brdrill.py --probe tests/fixtures/emergency-probe.jsonl \
      --drill tests/fixtures/emergency-drill.json
DRILL RECORD — playground / emergency / iad -> pdx
  namespace            bloodraven-playground
  trigger              kubectl -n bloodraven-playground scale deployment mysql-playground-iad --replicas=0
  promotedAt           2026-08-11T09:14:18.000Z
  lastWriteOldSite     2026-08-11T09:14:05.500Z
  firstWriteNewSite    2026-08-11T09:14:19.500Z
  writeGapSeconds      14.0
  gapClosed            true
  staleReads           0
  staleReadSeconds     0.0
  errors               readOnlyRefusal=0 connection=28 other=0
  verdict              RPO not established by this drill — audit divergentGtid on the old primary
  samples              102 (writes 51, reads 51)
```

## Rules

- Python standard library only. No live cluster is needed to complete or grade this.
- Do not edit anything under `tests/`. The captures are the evidence.
- Do not hardcode `iad` or `pdx`. One of the drills moves the primary the other way, and your tool
  should not care.
- Do not hardcode timestamps or counts. Every number in the record must fall out of the capture.
- Keep the three `verdict` strings exactly as the docstring gives them.
- 14.0 s and 4.5 s are properties of these captures, not figures this course claims. The only
  numbers here that belong to Bloodraven are its own: 6 s detection (`pollInterval` 2 s ×
  `failureThreshold` 3) and 12.0 s to the `activeSite` flip on a clean kill.

## Steps

- [ ] **Run the starter and read what it will not tell you** — From the project root, run `python3 starter/brdrill.py --probe tests/fixtures/emergency-probe.jsonl --drill tests/fixtures/emergency-drill.json`. It builds the whole record — group, mode, sites, promotion instant, sample counts — and reports `not computed` for every measurement. That is the shape of the answer; the four TODOs are the answer.
  *Done when:* The command runs without a traceback, prints a line containing `writeGapSeconds` and `not computed`, and exits 1.

- [ ] **TODO A — measure the write-gap** — Implement `write_gap`. Last successful write on the demoted site, first successful write on the promoted site after it, difference in seconds. Reads do not close a write-gap. A capture that never shows a write landing on the new primary returns `closed: False` and `gapSeconds: None`.
  *Done when:* `brdrill.py --probe tests/fixtures/emergency-probe.jsonl --drill tests/fixtures/emergency-drill.json` prints `writeGapSeconds` as `14.0` and `gapClosed` as `true`; and `--probe tests/fixtures/unclosed-probe.jsonl --drill tests/fixtures/emergency-drill.json` prints `writeGapSeconds` as `UNCLOSED` and exits 2.

- [ ] **TODO B — split the errors your writer actually saw** — Implement `error_classes`. Codes 1290 and 1792 are read-only refusals — the write that finally fails against a demoted primary. A null code is a transport failure. Everything else is `other`, and blanket retry-everything would replay it.
  *Done when:* `brdrill.py --probe tests/fixtures/planned-probe.jsonl --drill tests/fixtures/planned-drill.json` prints the `errors` line as `readOnlyRefusal=2 connection=6 other=2`.

- [ ] **TODO C — find the stale-read window** — Implement `stale_read_window`. Successful reads served by the demoted site at or after `status.lastFailover`. In the baseline capture that window is the unfixed writer reading from a site that stopped being authoritative, and it ends only when the `shipstream.io/db-readonly-playground:NoExecute` taint's `tolerationSeconds` expires and the pod is evicted.
  *Done when:* `brdrill.py --probe tests/fixtures/planned-probe-unbounded.jsonl --drill tests/fixtures/planned-drill-unbounded.json` prints `staleReads` as `56` and `staleReadSeconds` as `27.5`, while the emergency capture prints `staleReads` as `0`.

- [ ] **TODO D — say what the drill did and did not prove** — Implement `verdict`, in the exact wording from the docstring. Planned reaching `Succeeded` with `transactionsLost: 0` claims RPO 0 by construction — fence, snapshot `GTID_EXECUTED`, promote only on a superset. Emergency claims nothing; its RPO is a `divergentGtid` audit, which this capture does not contain.
  *Done when:* The planned run prints a `verdict` beginning `RPO 0 by construction`, the emergency run prints one beginning `RPO not established by this drill`, and the emergency command now exits 0.

- [ ] **Prove the pool fix, do not assert it** — Compare the fixed writer's planned drill against the baseline capture taken with the unfixed one. Same procedure, same operator, same 30 s `drainTimeout` — the only difference is bounded connection lifetime, a read/write split, and error-class retry.
  *Done when:* `brdrill.py --probe tests/fixtures/planned-probe.jsonl --drill tests/fixtures/planned-drill.json --baseline tests/fixtures/planned-probe-unbounded.jsonl --baseline-drill tests/fixtures/planned-drill-unbounded.json` prints `baselineGapSeconds` as `63.5` and `gapDeltaSeconds` as `-59.0`.

- [ ] **Write the drill record** — Fill in `starter/drill-record.md` from the tool's output. Both drills, all three captures, and a clean line between what these captures measured and what you are carrying over from elsewhere. If you ran the drills on your own playground, record your numbers instead and say which cluster they came from.
  *Done when:* `starter/drill-record.md` contains a `## Measured` heading, an `## Assumed` heading, no remaining `TODO`, both write-gap values, and both `verdict` strings.

- [ ] **Notice which seconds were never Bloodraven's** — Bloodraven's measured emergency promotion is 12.0 s to the `activeSite` flip, of which 6 s is detection (`pollInterval` 2 s × `failureThreshold` 3). Your emergency write-gap is longer than that. Subtract, and the remainder is the part of the outage that belongs to your writer — probe granularity before the kill, and pool recovery after the promotion. That remainder is the only part you can shorten.
  *Done when:* `starter/drill-record.md` contains a line beginning `Not Bloodraven's:` giving the emergency write-gap minus 12.0 s, with the subtraction shown (14.0 - 12.0 = 2.0).


## How this is graded

Three ways at once: these steps, the human rubric in [`rubric.md`](rubric.md), and four machine
tests run from `tests/` against the captures in `tests/fixtures/`. The tests do not need a cluster.

| Test | Weight |
| --- | --- |
| `emergency_drill_write_gap` | 40 |
| `reads_do_not_close_a_write_gap` | 30 |
| `unclosed_gap_is_not_zero` | 15 |
| `error_classes_and_verdict` | 15 |

See [`rubric.md`](rubric.md) for the criterion weights and what each one is looking for.
