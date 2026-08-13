# Rubric — The post-failover audit report

Human-marked criteria. Integer weights, total 100. The automated test cases are listed at the bottom
for reference; they are scored separately.

| Criterion | Weight |
| --- | ---: |
| The count is measured from the GTID sets | 30 |
| The failover record is rehydrated as the operator rehydrates it | 25 |
| Three verdicts, applied in order, with matching exit codes | 25 |
| Craft: legible under incident conditions, loud on bad input | 20 |
| **Total** | **100** |

### The count is measured from the GTID sets — 30

Every reported count comes from `transaction_count(gtid_subtract(...))` over sets parsed out of the capture. `status.sites[].divergentTransactionCount` and `status.sites[].divergentGtid` are never read as inputs (printing them as a labelled cross-check is fine). Subtraction handles multi-interval sets and interval splitting; a set that cannot be parsed raises rather than counting wrong. Full marks require the early-capture fixture — no operator-computed divergence anywhere in it — to report 8.

### The failover record is rehydrated as the operator rehydrates it — 25

Both durable copies are read; a copy more than five minutes ahead of `now` is discarded; a copy whose target is not in `spec.sites` is discarded; the later survivor wins; a tie goes to status. `recordSource` names which copy was used, so a reader can tell a status read from an annotation read. A submission that reads only status caps at half these marks even if it is otherwise correct.

### Three verdicts, applied in order, with matching exit codes — 25

`RPO 0`, `N transactions lost` and `UNMEASURED` are distinct outcomes with exit codes 0, 1 and 2. All four unmeasurable conditions are implemented and are checked in the stated order: no active-site `gtidExecuted`, no `promotionGtidExecuted`, an active site that does not contain the promotion GTID, and any non-active site with no `gtidExecuted`. `UNMEASURED` carries a reason naming the field or site that was missing. No submission that prints a number for an unmeasurable capture can score above half here.

### Craft: legible under incident conditions, loud on bad input — 20

The report reads top to bottom as an incident record and needs no explanation at 3am: one fact per line, per-site lines showing the actual divergent set beside its count. Functions are small and single-purpose, with the parsing, the arithmetic, the record selection and the verdict separable. Malformed input produces an exception or an `UNMEASURED` verdict naming the problem — never a silent zero, never a traceback with no message. Comments explain the non-obvious rules (the tie, the skew guard, the field not read), not the syntax.

## Automated test cases

| Test | Weight | What it checks |
| --- | ---: | --- |
| `canonical_audit_of_playground` | 40 | Correctness on the canonical capture: the post-failover `playground` status with `iad` blocked on four divergent transactions. Checks the full record — group, failover instant and source, from/to, the normalised promotion GTID, the per-site divergent sets and counts, the total, the verdict string and the exit code — and checks the clean-rejoin capture reports RPO 0 with exit 0. |
| `gtid_arithmetic_and_unmeasurable_captures` | 20 | Generality on awkward input, plus the structural check that the four required functions exist by name and are independently callable with the documented shapes. Multi-interval sets, bare single-transaction intervals, folded newlines, disjoint UUIDs, a tagged set that must raise `ValueError`, and the three unmeasurable captures: no `promotionGtidExecuted`, a site with no `gtidExecuted`, and an active site that does not contain the promotion GTID. |
| `catches_trusting_the_operators_divergence_fields` | 25 | Catches a shortcut: reading `status.sites[].divergentTransactionCount` or `divergentGtid` instead of subtracting the GTID sets. The early capture was taken while the old primary was still unreachable, so the operator has published neither field — a shortcut reports RPO 0 for a capture holding eight lost transactions, seven on `iad` across a split interval and one on the reader from the seconds it came up writable. A second case plants a stale `divergentTransactionCount` of 99 beside sets that say 4. |
| `catches_status_only_failover_record` | 15 | Catches a shortcut: reading `status.lastFailover`/`lastFailoverTarget` alone, which is what the starter ships. In this capture the status write was the one that failed, so the annotation pair is newer and the status copy names the previous promotion — a status-only tool reports the wrong site and a seventeen-minute-old instant. Also checks the guards that make the other direction safe: a skewed future annotation and a target absent from `spec.sites` are discarded, and an exact tie goes to status. |

Run them with `python3 tests/run_tests.py`, or against a specific file with
`BRAUDIT=/path/to/braudit.py python3 tests/run_tests.py`.
