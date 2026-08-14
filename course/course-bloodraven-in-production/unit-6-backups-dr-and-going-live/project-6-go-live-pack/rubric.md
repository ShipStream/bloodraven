# Rubric — The go-live pack for `playground`

| Criterion | Weight |
|---|---|
| The alert set discriminates real loss from designed behaviour | 30 |
| Every rule is built from a metric the shipped operator exports | 20 |
| The runbook map gets on-call to a command in thirty seconds | 20 |
| The drill record separates what was proved from what was assumed | 15 |
| Craft: the pack reads like something you would hand over | 15 |
| **Total** | **100** |

---

## The alert set discriminates real loss from designed behaviour — 30

All ten required alerts are present. `BloodravenReplicationLagging` excludes the `read-only` site by label matcher while keeping the threshold at 30 — the value `playground` sets — so the scenario 42 soak is silent and a 64-second `pdx` still pages. The rule was excluded, not deleted, not defanged by a raised threshold, and not narrowed to a hard-coded single site. `BloodravenFailoverOccurred` carries `severity: info`; every paging rule carries a non-zero `for:`. Full marks require both halves — silence on the reader and noise on the candidate.

## Every rule is built from a metric the shipped operator exports — 20

No expression references a name outside `SHIPPED_METRICS` plus Prometheus' own `up`. `BloodravenBackupStale` derives an age from `bloodraven_backup_last_success_timestamp_seconds` with `time() -` rather than an invented age gauge, and `BloodravenKeyringNotSealed` reads the one-hot `bloodraven_keyring_phase{phase="sealed"} == 0`. `check_metric_allowlist` is implemented and catches a planted bad metric rather than returning an empty list unconditionally.

## The runbook map gets on-call to a command in thirty seconds — 20

Every alert in the rules file has an entry with a well-formed `runbook.md#<slug>` anchor and one `kubectl` command. The commands are real: they use the seven subcommands the plugin actually has, and where a generic `kubectl bloodraven status playground` is not the right first move the entry says what is. `check_runbook_coverage` flags a removed entry and an emptied `firstCommand` and reports each exactly once.

## The drill record separates what was proved from what was assumed — 15

`proved` and `assumed` both use the supplied vocabulary and are non-empty. `logical-equivalence-with-live-primary` and `application-traffic-cutover` appear in `assumed` and never in `proved`. `backupSourceSite` is not the read-only site and `backupSourceReason` is one of `override`, `replica-preferred`, `primary-fallback`. `applicationSideAlertOwner` names a human, and `handoverNote` states one thing the pack will not tell the rotation.

## Craft: the pack reads like something you would hand over — 15

Rule names and annotations say what the alert means at 3am, not what the expression computes. The two implemented checks are short, return the documented shape, and do not crash on a malformed or missing entry — a missing runbook key is a finding, not a `KeyError`. The report runs clean with no stray debug output. A second engineer could read `alerts.yml` and `runbooks.yml` and take the pager without asking a question.

---

## Machine-run test cases

| Test | Weight | Catches |
|---|---|---|
| `reader_soak_stays_silent` | 30 | Catches a shortcut: alerting on `bloodraven_replication_lag_seconds` with no site-label exclusion, or silencing it by deleting the rule or raising the threshold. Replays the scenario 42 soak (`reader` at 91s, both threads running, group Ready) and requires zero firing alerts, then replays `candidate-lagging` (`pdx` at 64s) and requires exactly `BloodravenReplicationLagging@pdx`. A blanket suppression fails the second half; a missing exclusion fails the first. |
| `real_loss_still_pages` | 30 | Correctness on the canonical inputs. Replays four incident fixtures — no writable site with a stopped receiver thread, a post-failover group with 7 divergent transactions and an unsealed keyring and a stale backup and an archiver backlog, an operator-down scrape with a perfectly healthy data plane, and an auto-resolved split brain — and requires the exact `Alert@site` set for each. Firing for the wrong site is a failure. |
| `only_shipped_metrics_and_full_runbook_map` | 25 | Structural: the two checks must exist and work, not just return empty. Asserts all ten required alerts are present, that `check_metric_allowlist` clears the finished rules but returns exactly `[('BogusAlert', 'bloodraven_backup_age_seconds')]` for a planted rule, and that `check_runbook_coverage` clears the finished map but flags a removed entry and an emptied `firstCommand` exactly once each. |
| `drill_record_separates_proved_from_assumed` | 15 | Grades the DR drill record. `logical-equivalence-with-live-primary` and `application-traffic-cutover` must sit in `assumed` and never in `proved`, both lists must use the supplied vocabulary, `backupSourceSite` must not be the read-only site, `backupSourceReason` must be one of the three reason strings, `restore.confirm` must parse as RFC 3339, and `applicationSideAlertOwner` must name someone. |

**Total: 100**
