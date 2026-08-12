# The post-failover audit report

**Unit 3 — Emergency failover, end to end · Project · `code-notebook` · Python**

## Goal

Build a tool that turns a post-failover status into an incident record — when it happened, from which site to which, the promotion GTID, the divergent set and its transaction count, and a verdict of RPO 0 or N transactions lost — so that the number you report after an outage is measured rather than assumed.

Running example: the `orders` failover group on the three-site playground — `iad`, `pdx`, and the
`reader`. Grading runs against the captures in `tests/fixtures/`, so you do not need a live cluster to
finish or to be marked.

## How this works

`orders` failed over. `pdx` took writes about twelve seconds after you held `iad` down, and the
incident review wants a number. `braudit` turns one captured status into an incident record: when the
promotion happened, which site it moved to, the promotion GTID, the divergent set held by every other
site, and a verdict — `RPO 0`, `N transactions lost`, or `UNMEASURED`.

Capture the artefact from your own playground:

```bash
kubectl -n bloodraven-playground get mysqlfailovergroup orders -o json > orders-after-failover.json
python3 braudit.py orders-after-failover.json
```

No cluster to hand? `tests/fixtures/orders-recovery-blocked.json` is that capture, taken after the
promotion this unit drove, with `iad` back and blocked on four divergent transactions.

The arithmetic is the operator's own. There is no divergence exactly when the new primary's GTID set
contains the old primary's; what it does not contain is the difference, `GTID_SUBTRACT(old, new)`, and
the cardinality of that difference is your lost-transaction count. You compute it here, from the sets,
for every site that is not the active one — including the reader, which is where a site that came up
writable for a few seconds before the sidecar fenced it will show up.

## Your tasks

**TODO A — `parse_gtid_set(text)`.** Return `{uuid: [(start, end), ...]}`. Accept several UUIDs
separated by commas, several intervals for one UUID separated by colons (`uuid:1-19:25-30`), a
single-transaction interval written bare (`uuid:7`), and newlines anywhere, because a captured set is
folded across lines. Empty string gives `{}`. Anything else raises `ValueError` — including a MySQL 9.x
tagged set such as `uuid:Domain_1:1-3`, because a tag is part of the UUID's identity and quietly
dropping it would understate the count.

**TODO B — `transaction_count(gtid_set)`.** The number of transactions in a parsed set. An interval
`(20, 23)` is four transactions, not one and not three.

**TODO C — `gtid_subtract(minuend, subtrahend)`.** `GTID_SUBTRACT`: the transactions in the first set
that are not in the second. Subtracting from the middle of an interval splits it —
`{u: [(1, 32)]}` minus `{u: [(1, 19), (25, 30)]}` is `{u: [(20, 24), (31, 32)]}`, which is seven
transactions. A replica's `gtid_executed` really does look like that mid-catch-up with a parallel
applier, so this is not a contrived case.

**TODO D — `select_failover_record(obj, now)`.** The operator writes the failover record twice, to the
status subresource and to the `bloodraven.shipstream.io/last-failover` /
`bloodraven.shipstream.io/last-failover-target` annotation pair, because the two paths fail
independently. Rehydrate it the way the operator does after a restart: discard a copy stamped more
than five minutes ahead of `now`, discard a copy whose target is not a site in `spec.sites`, take the
later of what survives, and give a tie to status — equal instants describe the same promotion. Return
`{"at": datetime|None, "target": str, "source": "status"|"annotations"|"none"}`.

**TODO E — `audit(obj, now)`.** Build the record. Apply the verdict rules in this order and stop at the
first that matches:

1. The active site is missing from `status.sites`, or its `gtidExecuted` is empty → `UNMEASURED`.
2. `status.promotionGtidExecuted` is empty → `UNMEASURED`. The failover step that records it is
   non-fatal, so a promotion can complete without it, and a capture without it cannot be audited.
3. The active site's `gtidExecuted` does not contain `promotionGtidExecuted` → `UNMEASURED`. The site
   writing now is not the site this record describes.
4. Any non-active site has an empty `gtidExecuted` → `UNMEASURED`. An unknown site is not a zero.
5. Total is 0 → `RPO 0`.
6. Otherwise → `N transactions lost`.

## What the scaffolding is for

Argument parsing, capture loading, `parse_time`/`fmt_time`, `render_gtid_set`, `render` and the exit
plumbing are wired. So is `gtid_contains`, written as `not gtid_subtract(subset, superset)` — it is
there to show that containment is subtraction asked the other way round, which is why TODO C buys you
both halves of the operator's test.

## Expected output

```text
$ python3 braudit.py tests/fixtures/orders-recovery-blocked.json --now 2026-04-30T21:00:00Z
group: bloodraven-playground/orders
failoverAt: 2026-04-30T20:55:52Z
recordSource: status
from: iad
to: pdx
promotionGtidExecuted: a2cc879c-5f9d-11f1-9fae-8e47bc2a4544:1-19,a3c3f9e8-5f9d-11f1-bf37-568bfb8d0365:1-7
site: iad divergent=a2cc879c-5f9d-11f1-9fae-8e47bc2a4544:20-23 lost=4
site: reader divergent=- lost=0
lost: 4
verdict: 4 transactions lost
$ echo $?
1
```

Exit `0` for `RPO 0`, `1` when transactions were lost, `2` for `UNMEASURED`.

## Rules

- **Count from the sets, never from the count.** `status.sites[].divergentTransactionCount` and
  `status.sites[].divergentGtid` are the operator's own answer, published only once it has fenced the
  returning site and run the comparison. Half the captures you will audit were taken before that
  existed. Read neither as an input; if you print them at all, print them as a cross-check.
- Verdict strings are exact: `RPO 0 — no transaction was lost`, `4 transactions lost`
  (`1 transaction lost` in the singular), `UNMEASURED — <reason>`.
- `promotionGtidExecuted` in the report is the captured set rendered through `render_gtid_set`, so the
  folded newline disappears.
- `from` is the non-active site holding the most divergent transactions, or `-` when nothing diverged.
  Nothing in the status records which site was demoted; divergence is the only evidence you have.
- Standard library only. No cluster access at grading time.
- Audit before you reclone. A capture taken after the reclone shows no divergence, because the
  divergent transactions are gone — that is what a reclone is.


## Steps

- [ ] **1. Get a capture worth auditing**
      Drive the emergency failover on `orders` again if the playground is up, then save `kubectl -n bloodraven-playground get mysqlfailovergroup orders -o json`. If you have no cluster, use `tests/fixtures/orders-recovery-blocked.json`, which is the same capture after the promotion this unit drove. Run `python3 braudit.py <capture>` before you change anything: the starter reports `RPO 0` against a capture that plainly shows four divergent transactions.

      *Done when:* A capture file exists whose `.status.lastFailoverTarget` and `.status.promotionGtidExecuted` are both non-empty, and `python3 braudit.py <capture>` prints a line beginning `group: ` and exits without a traceback.

- [ ] **2. Parse a GTID set and count it (TODO A, TODO B)**
      Implement `parse_gtid_set` and `transaction_count`. Handle several UUIDs, several intervals per UUID, a bare single-transaction interval, and the newlines a captured set is folded across. Refuse a tagged set rather than mis-counting it.

      *Done when:* `python3 -c "import braudit as b; print(b.transaction_count(b.parse_gtid_set('u:1-5:9-11,v:7')))"` prints `9`, and `python3 -c "import braudit as b; b.parse_gtid_set('u:Domain_1:1-3')"` exits non-zero with a `ValueError`.

- [ ] **3. Implement GTID_SUBTRACT (TODO C)**
      Return the transactions of the first set that are not in the second, splitting an interval when the subtrahend sits inside it. This one function gives you both halves of the operator's test: the difference, and — through the wired `gtid_contains` — containment.

      *Done when:* `python3 -c "import braudit as b; print(b.render_gtid_set(b.gtid_subtract(b.parse_gtid_set('u:1-32'), b.parse_gtid_set('u:1-19:25-30'))))"` prints `u:20-24:31-32`.

- [ ] **4. Rehydrate the failover record (TODO D)**
      Read both durable copies, apply the five-minute future-clock guard and the `spec.sites` membership check, take the later survivor, and give a tie to status. The starter ships the naive status-only read; `orders-annotation-newer.json` is the capture where that read names the wrong site and the wrong minute.

      *Done when:* `python3 braudit.py tests/fixtures/orders-annotation-newer.json --now 2026-05-02T05:40:00Z` prints `recordSource: annotations`, `failoverAt: 2026-05-02T05:34:11Z` and `to: iad`.

- [ ] **5. Assemble the record and the verdict (TODO E)**
      Compute each non-active site's divergent set against the active site's `gtidExecuted`, count it, and apply the six verdict rules in order. Three outcomes, three exit codes: measured loss, measured zero, and unmeasurable.

      *Done when:* `python3 braudit.py tests/fixtures/orders-recovery-blocked.json --now 2026-04-30T21:00:00Z` prints `verdict: 4 transactions lost` and exits 1; the same command on `orders-clean-rejoin.json --now 2026-05-02T05:25:00Z` prints `verdict: RPO 0 — no transaction was lost` and exits 0; on `orders-no-promotion-gtid.json --now 2026-05-02T05:20:00Z` it prints a line starting `verdict: UNMEASURED — ` and exits 2.

- [ ] **6. Name the field you did not read**
      Run the tool against `tests/fixtures/orders-early-capture.json`. It reports eight lost transactions, and that capture carries no `divergentTransactionCount` on any site — the old primary is still unreachable, so the operator has not run its comparison yet. A tool that had read the field would have reported RPO 0 and closed the incident. Record that in the source: add a comment at the top of `braudit.py` naming the field you did not read and the capture that shows why.

      *Done when:* `braudit.py` contains a comment line mentioning `divergentTransactionCount`, and `python3 braudit.py tests/fixtures/orders-early-capture.json --now 2026-05-02T05:18:00Z` prints `lost: 8`.

## How this is graded

Four automated test cases (weights 40 / 20 / 25 / 15) run against the fixtures in `tests/`; run them
yourself at any point with `python3 tests/run_tests.py`. A human marks the four criteria in
[`rubric.md`](rubric.md).

Two of the test cases are adversarial. One capture in `tests/fixtures/` was taken before the operator
had computed any divergence, and one has a status write that failed while its annotation write
succeeded. A tool that reads the easy field passes the canonical case and fails both.
