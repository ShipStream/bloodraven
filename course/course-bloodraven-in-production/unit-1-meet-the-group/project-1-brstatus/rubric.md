# Rubric — brstatus, the one-screen status reader

Human grading. Weights total 100. The automated test cases are scored separately and are listed at
the bottom for reference.

| # | Criterion | Weight |
|---|---|---|
| 1 | The verdict and exit code come from the group's conditions | 28 |
| 2 | Reader endpoint eligibility is role-aware and complete | 27 |
| 3 | Absent lag is distinguished from zero lag | 15 |
| 4 | The tool was run against the live `playground` group | 10 |
| 5 | Craft: the output stays one screen and the code survives a thin status | 20 |
| | **Total** | **100** |

## 1. The verdict and exit code come from the group's conditions — 28

`verdict` reads the `Degraded` condition out of `status.conditions` and combines it with
`status.activeSite` to return exactly `("OK", 0)`, `("DEGRADED", 1)` or `("CRITICAL", 2)`.

Full marks require that no per-site field — lag, `replicating`, `state` — is consulted inside
`verdict`. The operator evaluates its cross-site table on every poll and writes the answer into the
condition; re-deriving it from the site rows is a second, disagreeing implementation of the same
decision, and it will disagree first on exactly the fixture that matters.

- **28** — all three codes correct on all seven fixtures, derived only from the condition and
  `activeSite`.
- **14** — the three codes are right but group health is partly re-derived from the site rows (for
  example, an `or` clause that also fires on site lag).
- **0** — any fixture returns the wrong code.

## 2. Reader endpoint eligibility is role-aware and complete — 27

`is_serving` branches on `site_role(...)`.

The `read-only` branch tests all five conjuncts together: `sourceConvergenceState` is `Converged`,
`replicating` is true, `secondsBehindSource` is present, `canonical_host(sourceHost)` equals
`expected_source_host(...)` for the active site, and the lag is at or under
`effective_readonly_max_lag(spec)`.

Every other role is served on `state` alone — `writable` or `read-only` — with no lag gate. A
`primary-candidate` replica 300 seconds behind stays behind `mysql-playground-replicas` and is still
`SERVING yes`. That is the operator's real behaviour, not a simplification.

- **27** — both branches correct, five conjuncts present, the correct threshold helper called.
- **14** — the five conjuncts are right but the same gate is wrongly applied to `primary-candidate`
  sites, or one conjunct is missing.
- **0** — the reader is gated on `effective_max_lag` instead of `effective_readonly_max_lag`.

## 3. Absent lag is distinguished from zero lag — 15

`format_lag` returns `unknown` for an absent or null `secondsBehindSource` and `<n>s` otherwise,
**and** `is_serving` treats a null lag as disqualifying rather than as `0`. Both places, not one.

- **15** — both correct.
- **8** — only one of the two is right.
- **0** — `or 0`, `get(..., 0)`, or any other collapse of null into zero in both places.

## 4. The tool was run against the live `playground` group — 10

`artefacts/playground-live.json` is present, is a single `MysqlFailoverGroup` captured from the running
playground rather than a copied fixture — its `metadata.creationTimestamp`, `metadata.uid` and
`status.sites[].lastSeen` are populated — and the submission shows `brstatus` output for it.

- **10** — captured, distinguishable from the fixtures, and its output shown.
- **5** — captured but no output shown, or the capture is a trimmed excerpt rather than the whole
  object.
- **0** — absent, or a renamed fixture.

## 5. Craft: the output stays one screen and the code survives a thin status — 20

- The header, table and `VERDICT:` line are intact and readable.
- Absent optional fields — `state`, `secondsBehindSource`, `conditions`, `sourceHost` — produce a
  cell rather than a traceback.
- Unreadable input exits 3 with a message on stderr, not a stack trace on stdout.
- The three completed functions carry a comment a colleague could follow: it names the rule
  (`readOnlyMaxLagSeconds`, `role=replica` + `healthy=yes`, the `Degraded` condition), rather than
  restating the code in English.

Deduct for debug prints left in the output, for hard-coded thresholds where a helper exists, and for
rewriting scaffolding that was already correct.

## Automated checks (scored separately, 100 between them)

| Test case | Weight | What it proves |
|---|---|---|
| `healthy_group_summary` | 30 | The whole screen is right on the canonical input, exit 0. |
| `lagging_reader_is_not_an_unhealthy_group` | 30 | **Adversarial.** Two fixtures, both 300s behind a 30s threshold, with opposite correct answers. Catches one lag rule applied to every site, and a verdict folded out of the site rows. |
| `awkward_status_null_lag_and_lost_authority` | 20 | Null lag, an explicit `readOnlyMaxLagSeconds: 0`, and both authority-loss shapes. |
| `verdict_reads_conditions_and_reader_threshold` | 20 | Structural: the rules are implemented in the functions that own them. |
