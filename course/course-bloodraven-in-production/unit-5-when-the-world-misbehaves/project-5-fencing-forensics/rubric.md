# Rubric — Fencing forensics

Human grading, 100 marks. The machine test cases run separately and to the same total.

| Criterion | Weight |
|---|---|
| Fence vocabulary and classification | 30 |
| Site attribution across inconsistent record shapes | 25 |
| Verdicts and the missing fence, judged from the record | 25 |
| Craft — clarity, structure, and behaviour on imperfect bundles | 20 |
| **Total** | **100** |

### Fence vocabulary and classification — 30 marks

`classify()` maps whole `msg` strings to the six cause ids `rule-1`, `rule-2`, `safety-net`, `split-brain`, `old-primary`, `non-promotable`, and returns None for everything else. Full marks require exact-string matching: any solution that prefix-matches `SELF-FENCING:` or substring-matches `fenc` scores at most half, because it counts `SELF-FENCING: killed app connections`, `SELF-FENCING FAILED:` and `SELF-FENCED:` as fences. Deduct if a sidecar-only cause is accepted from the operator stream or vice versa.

### Site attribution across inconsistent record shapes — 25 marks

`fenced_site()` returns the right site for all six causes, not just the ones carrying a `site` key. Full marks require handling the rule-2 and safety-net records that name no site (fall back to the filename-derived `file_site`) and the operator's `fencedSite` key. A solution that emits `?`, `None`, or the pod name for any fence in the three fixtures loses most of this criterion.

### Verdicts and the missing fence, judged from the record — 25 marks

`judge()` implements the verdict table and, critically, computes the rule-2 verdict from `time - latestPeerOk` and `time - bloodravenLastOk` against `leaseTimeout` rather than assuming a lease fence is self-justifying. `unfenced_writable_sites()` parses the `SPLIT BRAIN:` alert, subtracts sites that were fenced and the site holding primary authority, and reports each remaining site once even when the alert repeats every poll. Deduct for a hardcoded verdict, a hardcoded site list, or reporting the promoted site as unfenced.

### Craft — clarity, structure, and behaviour on imperfect bundles — 20 marks

The four functions stay separate and single-purpose; no fixture path, site name, or expected count is hardcoded. The tool survives a bundle with a missing sidecar file, a record with no `site` key, an unknown `msg`, and an unexpected extra field without crashing, and the exit code is 1 exactly when there is a premature fence or an unfenced writable site. Comments explain the discriminations that are easy to get wrong — why `SELF-FENCED:` is not counted, why a lease fence needs checking — rather than restating the code.

---

## Machine test cases

Defined in `project.json`, mirrored in `tests/test_brfence.py`. Each prints `PASS` on success.

| Test | Weight |
|---|---|
| `canonical_timeline` | 40 |
| `awkward_bundle_generality` | 25 |
| `decoy_lines_are_not_fences` | 20 |
| `stable_msg_vocabulary_in_source` | 15 |
| **Total** | **100** |

### `canonical_timeline` — 40 marks

Correctness on the canonical input. Runs the tool against `tests/fixtures/partition-a`, a shape-A partition on `orders`: `iad` self-fences on rule-2 while isolated, the restarted `reader` sidecar stays fenced by the startup safety net, and `iad` self-fences again on rule-1 after its mysqld comes back writable. Asserts three fence events with the right site, cause and verdict in time order, no premature fences, no unfenced writable sites, and exit 0.

### `awkward_bundle_generality` — 25 marks

Generality on an awkward bundle. `tests/fixtures/split-brain-tier3` has no `sidecar-pdx.jsonl` at all, two fence records that carry no `site` key, a repeated `SPLIT BRAIN` alert, an operator fence of a `read-only` reader, and a rule-2 fence whose own `latestPeerOk` sits four seconds inside the twenty-second lease window. Asserts the rule-2 line is `premature`, that `pdx` and only `pdx` is reported unfenced, and that the tool exits 1.

### `decoy_lines_are_not_fences` — 20 marks

Catches a shortcut: matching `msg` by substring or by the `SELF-FENCING:` prefix instead of by the whole string. `tests/fixtures/decoys` holds thirteen records, eleven containing `fenc` — `SELF-FENCED:`, `SELF-FENCING FAILED:`, `SELF-FENCING: killed app connections`, `failed to fence old primary`, `fencing: adopted active-site view from peer` — and exactly one real fence decision. A substring matcher reports eleven, a prefix matcher four; the test demands exactly one, `iad`/`rule-1`/`correct`, and exit 0.

### `stable_msg_vocabulary_in_source` — 15 marks

Structural. Asserts the required construct is present in `brfence.py`: the two `SELF-FENCING:` msg strings and all three `safety net: … staying fenced` strings appear verbatim (whitespace and implicit string concatenation normalised), and the source reads `bloodravenLastOk`, `latestPeerOk` and `leaseTimeout` — proof the rule-2 verdict was computed from the record rather than assumed.
