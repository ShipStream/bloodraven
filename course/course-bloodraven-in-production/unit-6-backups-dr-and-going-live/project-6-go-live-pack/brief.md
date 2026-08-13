# The go-live pack for `playground`

**Unit 6 — Backups, disaster recovery, and going live.** The capstone.

## The goal

Assemble the artefacts you would actually hand an on-call rotation: a Prometheus rules file built only from metric names the operator really exports, a one-page alert-to-runbook-to-first-command map, and a DR drill record showing you restored `playground` and measured how far back you could reach. Then let a checker prove the thing that matters — that your rules stay silent while a reader soaks past three times `maxLagSeconds`.

## How this works

`playground` runs three sites: `iad` and `pdx` as `primary-candidate`, and `reader` with `role: read-only`. You have already backed it up, verified it, restored it in place and turned on encryption at rest. What is missing is the pack you hand to the rotation.

The pack lives in `starter/pack/`:

| File | What it is |
|---|---|
| `alerts.yml` | Prometheus alerting rules for `playground` |
| `runbooks.yml` | alert → runbook anchor → the one command typed first |
| `drill.json` | the record of the DR drill you ran |

`starter/golive.py` checks all three and replays six metric fixtures from `tests/fixtures/` through your rules. Run it:

```
python3 starter/golive.py
```

It runs as given and reports twelve problems. Fix them in order.

## Your tasks

**TODO A — `pack/alerts.yml`.** `BloodravenReplicationLagging` currently reads every site. Chaos scenario 42 soaks the reader past three times `maxLagSeconds` with both replication threads still running, and asserts the group stays `Ready`, no failover fires, no cooldown is consumed, and only the reader endpoint sheds. That is the role model doing its job, not a fault. Exclude the read-only site by label matcher and keep the threshold at `30` — the value `playground` actually sets in `spec.replication.maxLagSeconds` — so a genuinely lagging `primary-candidate` still pages. The shipped CRD default is `300`; a rule's number tracks the group's spec, never the default.

**TODO B — `pack/alerts.yml`.** `BloodravenBackupStale` reads `bloodraven_backup_age_seconds`. That metric does not exist. The shipped operator exports a last-success timestamp gauge, so an age has to be derived from it with `time() -`. Rewrite the expression against a name in `SHIPPED_METRICS`.

**TODO C — `pack/alerts.yml`.** Add `BloodravenKeyringNotSealed`. `Sealed` is the steady state; the metric is a one-hot gauge over the `phase` label, so alert when the sealed series reads `0`.

**TODO D — `pack/runbooks.yml`.** Three alerts have no usable entry. Each needs an `anchor` of the form `runbook.md#<slug>` and a `firstCommand` starting with `kubectl`. The plugin has exactly seven subcommands — `status`, `promote`, `reclone`, `backup`, `verify-backup`, `version`, `help` — and it only writes resources the operator already reads, never talking to MySQL. `kubectl bloodraven status playground` is the sensible default.

**TODO E — `pack/drill.json`.** Fill `proved`, `assumed`, `applicationSideAlertOwner` and `handoverNote`. Use only these terms:

- `proved`: `artifact-loads`, `sanity-check-passed`, `restore-in-place-completed`, `reader-endpoint-returned`, `dns-record-updated`
- `assumed`: `logical-equivalence-with-live-primary`, `application-traffic-cutover`, `cross-cluster-split-brain-detection`, `dns-propagation-time`

A `Succeeded` verification proves the artifact loads and your scalar assertion held. It never proves logical equivalence with the live primary or an application-level rehearsal of traffic cutover, so both of those belong in `assumed` on every honest record. `applicationSideAlertOwner` is a named human: no shipped alert fires for "the application is still broken after a successful failover", so somebody owns that one by name or nobody does. `handoverNote` is one line saying what this pack will not tell the rotation.

**TODO F — `starter/golive.py`.** Implement `check_metric_allowlist(rules)`. Return sorted `(alert, metric)` tuples for every metric outside `SHIPPED_METRICS` and `ALLOWED_FOREIGN_METRICS`. `promeval.metric_names_in(expr)` pulls the names out for you.

**TODO G — `starter/golive.py`.** Implement `check_runbook_coverage(rules, runbooks)`. Return sorted `(alert, problem)` tuples for every alert with no entry, an empty or malformed anchor, an empty `firstCommand`, or a `firstCommand` that does not start with `kubectl`. The wording of `problem` is yours; which alerts you flag is graded.

## What the scaffolding is for

`starter/promeval.py` is a small PromQL-subset evaluator. It handles `<selector> <cmp> <number>`, `absent(...)`, `time() - <selector> <cmp> <number>` and `increase(<selector>[<window>]) <cmp> <number>`, with `=`, `!=`, `=~` and `!~` matchers. Two simplifications you should know about: evaluation is instantaneous, so a rule's `for:` is never simulated (it is still required on every paging rule, because a rule without one pages on a single bad scrape); and `increase(m[15m])` reads a pre-computed series out of the fixture's `increases` block rather than computing anything.

`golive.py` already does argument parsing, YAML and JSON loading, the coverage and drill checks, evaluation and report formatting. Only TODO F and TODO G are yours.

A fixture verdict grades `Alert@site` keys, not bare alert names, so an alert that fires for the wrong site is a `MISMATCH`.

## Expected output

When the pack is finished:

```
[metrics]  clean
[runbooks] clean
[coverage] clean
[drill]    clean
[owner]    application-side alerting owned by: <a named human>

fixture replay
----------------------------------------------
  fixture candidate-lagging: 1 firing, expected 1  OK
  fixture operator-down: 1 firing, expected 1  OK
  fixture post-failover-divergence: 5 firing, expected 5  OK
  fixture primary-lost: 2 firing, expected 2  OK
  fixture reader-soak-3x: 0 firing, expected 0  OK
  fixture split-brain-resolved: 2 firing, expected 2  OK

RESULT: READY
```

Exit code 0.

## Rules

- Use only metric names the shipped operator exports. `SHIPPED_METRICS` in `golive.py` is the list; do not add to it from memory. A rule against a metric that does not exist is a rule that can never fire.
- Do not edit anything under `tests/`. The fixtures are the grading input.
- Keep the `BloodravenReplicationLagging` threshold at `30`, which is what `playground` sets. The backup-staleness and failover-window thresholds are SLOs you choose, not Bloodraven defaults.
- Do not remove a rule to silence it. The reader exclusion has to be an exclusion.
- `BloodravenFailoverOccurred` carries `severity: info`. It says the operator finished, not that traffic recovered.
- There is no `Failover` condition reason to match on. The failover row of the decision matrix emits `Reason="Degraded"`.

## Steps

- [ ] **1. Run the checker and read the twelve problems**
      Run `python3 starter/golive.py` before changing anything. It loads nine alert rules, eight runbook entries and the drill record, then replays six fixtures. Read the whole report: two checks are unimplemented, one required alert is missing, the drill record claims nothing and hands over nothing, and two fixtures mismatch. The `SPURIOUS BloodravenReplicationLagging@reader` line under `reader-soak-3x` is the one this project is about.
      *Done when:* `python3 starter/golive.py` exits 1 and prints a line starting `RESULT: NOT READY`.

- [ ] **2. Implement the metric allowlist and the runbook coverage check (TODO F, TODO G)**
      In `starter/golive.py`, replace the two `return None` stubs. `check_metric_allowlist(rules)` returns sorted `(alert, metric)` tuples for every metric outside `SHIPPED_METRICS | ALLOWED_FOREIGN_METRICS`; use `promeval.metric_names_in(expr)` to extract them. `check_runbook_coverage(rules, runbooks)` returns sorted `(alert, problem)` tuples for every alert with no entry, an anchor that is empty or does not match `ANCHOR_RE`, an empty `firstCommand`, or a `firstCommand` that does not start with `kubectl`. Both return `[]` when clean. The report will now tell you the truth about the other files.
      *Done when:* `python3 starter/golive.py` prints neither `[metrics]  not implemented (TODO F)` nor `[runbooks] not implemented (TODO G)`, and its `[metrics]` line names `BloodravenBackupStale -> bloodraven_backup_age_seconds`.

- [ ] **3. Rebuild BloodravenBackupStale on a real metric and add BloodravenKeyringNotSealed (TODO B, TODO C)**
      `bloodraven_backup_age_seconds` does not exist, so the rule can never fire — the exact failure mode the checker exists to catch. The operator exports `bloodraven_backup_last_success_timestamp_seconds` per `(group, profile)`; derive the age with `time() -` and keep the 24-hour SLO. Then add `BloodravenKeyringNotSealed`: `bloodraven_keyring_phase` is a one-hot gauge over the `phase` label and `Sealed` is the steady state, so alert when the sealed series reads `0`. Any other phase means the site is running with a writable keyring or failed to escrow one.
      *Done when:* `python3 starter/golive.py` prints `[metrics]  clean` and `[coverage] clean`, and the `post-failover-divergence` fixture line reads `5 firing, expected 5  OK`.

- [ ] **4. Make the lag alert ignore the reader on purpose (TODO A)**
      This is the centrepiece. Add a site-label exclusion for the `read-only` site to `BloodravenReplicationLagging` and leave the threshold at `30`. Do not delete the rule and do not raise the threshold — `pdx` at 64 seconds behind is a genuine RPO drift on a promotable candidate and must still page. Remember what the threshold is and is not: `maxLagSeconds` drives only the `ReplicationLagging` condition. It is not a promotion gate, so a candidate past it is still promoted, because no writable site at all is almost always worse.
      *Done when:* `python3 starter/golive.py` prints both `fixture reader-soak-3x: 0 firing, expected 0  OK` and `fixture candidate-lagging: 1 firing, expected 1  OK`.

- [ ] **5. Finish the alert-to-runbook-to-first-command map (TODO D)**
      Three alerts have no usable entry: `BloodravenDivergentTransactions` has none, `BloodravenPITRArchiveLagging` has an empty `firstCommand`, and `BloodravenKeyringNotSealed` is the alert you just added. Give each an anchor of the form `runbook.md#<slug>` and one command starting with `kubectl`. Every command must be something a rotation member can run cold, which is why the plugin is safe to hand over: it only writes resources the operator already reads and never talks to MySQL.
      *Done when:* `python3 starter/golive.py` prints `[runbooks] clean`.

- [ ] **6. Write the drill record honestly (TODO E)**
      Fill `proved`, `assumed`, `applicationSideAlertOwner` and `handoverNote` in `pack/drill.json` using only the vocabulary in the instructions. Your verification returned `Succeeded` and your in-place restore reached `Succeeded` through an RFC 3339 confirm token, so the artifact loads and the sanity check held. Neither proves logical equivalence with the live primary, and neither rehearses an application traffic cutover — both belong in `assumed`. Name a human for the application-side alert: Bloodraven cannot see your pool, your driver's `rejectReadOnly` handling, or your JVM's DNS cache, so no shipped alert fires when the application is still broken after a successful failover. Then write `handoverNote`: one line on what this pack will not tell the rotation.
      *Done when:* `python3 starter/golive.py` prints `[drill]    clean` and an `[owner]` line that is not `application-side alerting owned by: (nobody)`.

- [ ] **7. Green the whole pack and hand it over**
      Run the checker one last time against every fixture. `RESULT: READY` means your rules page for the four incident fixtures, stay silent for the soaked reader, use only metrics that exist, carry a first command each, and sit behind a drill record that separates proof from assumption. Read your own `handoverNote` back and decide whether you would sign it. That statement — what `playground` will and will not do for an on-call rotation — is the deliverable of the whole course.
      *Done when:* `python3 starter/golive.py` exits 0 and prints `RESULT: READY`.

## How this is graded

Four machine-run test cases (weights 30 / 30 / 25 / 15) replay the fixtures in `tests/fixtures/` through your rules and inspect the two checks you implement. The adversarial case is `reader_soak_stays_silent`: it fails a rules file that pages for the scenario 42 reader soak, and it also fails one that buys silence by deleting the rule or raising the threshold.

A human grades five criteria against [`rubric.md`](rubric.md).
