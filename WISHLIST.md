# Bloodraven — Feature & Documentation Wishlist

## Checklist

- [x] 7. Cross-region/cross-cluster DR as a first-class feature
- [ ] 27. Backup/restore performance guide
- [ ] 30. Public repo, license, release cadence
- [x] 32. Real-cluster E2E CI gate
- [ ] 41. Safe Secret watch narrowing design
- [ ] 42. Namespace-scoped watch/cache mode evaluation
- [x] 43. Dedicated backup/PITR real-cluster E2E scenarios

## P0 — Production adoption blockers

**32. Real-cluster E2E CI gate.** Done: `make test-e2e` runs the release profile of `playground-chaos run-all` against a real cluster instead of the former placeholder. `make test-e2e-smoke` runs a fast smoke subset (3 scenarios). Three profiles (`smoke`/`release`/`full`) filter scenarios via `--profile` on `playground-chaos run-all` and `make chaos-run-all-profile PROFILE=`. CI uses a reusable workflow (`_e2e.yml`) that creates a kind cluster with Calico CNI, deploys the playground, and runs the selected profile. Nightly and manual runs use the release profile; PRs with the `e2e` label trigger a smoke run. Release publishing blocks on the E2E release-profile gate. JUnit, forensics, setup logs, and kind logs are uploaded as artifacts. Dedicated MySQL backup restore and PITR verification scenarios are split out as follow-up #43 so the gate can start enforcing the existing real-cluster chaos suite now without misrepresenting that coverage.

**43. Dedicated backup/PITR real-cluster E2E scenarios.** Done: added release-profile playground-chaos scenarios `30-backup-verification-rustfs` and `31-pitr-verification-rustfs`. They configure the playground backup profile against RustFS with isolated per-run prefixes, create real `MysqlBackup` CRs, pin `MysqlBackupVerification.spec.backupRef` to the created backup, and assert deterministic marker rows after restore. Scenario 31 enables PITR/binlog archival, waits for sidecar archiver coverage, and verifies timestamp replay includes baseline + before-target rows while excluding the after-target row.

## P1 — DR and operational completeness

**7. Cross-region/cross-cluster DR as a first-class feature.** Today DR = "create a new MysqlFailoverGroup with `initFromBackup` in another cluster." This works but is ad-hoc. Consider a `MysqlDRTarget` CR that continuously ships backups + binlogs to a designated target cluster/bucket and can be promoted with one command. At minimum, document the recommended multi-cluster DR topology with a runbook.

_In progress: branch `megamind/dr-7-phase-0-1`, Phase 0 + Phase 1 — `MysqlStandbyCluster` CRD, Phase 0 runbook (`docs/docs/multi-cluster-dr.mdx`), and Phase 1 bucket-discovery reconciler._

## P3 — Documentation deliverables

**27. Backup/restore performance guide.** For a 500 GB dataset, how long does `util.dumpInstance` take with what `threads`/`bytesPerChunk`? How long does `loadDump` take? At what `maxLagSeconds` does your replica-as-source fallback trigger and what's the primary-impact if it does? Users need ballparks before they commit.

**30. Public repo, license, release cadence.** If this isn't going external, skip. If it might — Apache-2.0, semver on the CRD and the operator separately, `CHANGELOG.md`, GitHub releases with signed images, published Helm chart index. The bar for "a real project someone else will adopt" is higher than the bar for "our internal tool."

**41. Safe Secret watch narrowing design.** Low priority: evaluate whether the broad `Secret` watch in `MysqlFailoverGroupReconciler.SetupWithManager` can be narrowed safely without missing credential or TLS rotation. Possible approaches include labels for Bloodraven-managed or referenceable Secrets, field indexes, a narrower event predicate, or cache selectors. Do not change behavior without controller or envtest coverage for every Secret reference path.

**42. Namespace-scoped watch/cache mode evaluation.** Low priority: decide whether Bloodraven should support namespace-scoped manager watch/cache mode in addition to the current cluster-scoped model. This is useful for locked-down tenant installs, but it changes reconciliation coverage and deployment assumptions, so treat it as an install-mode design rather than a quick flag.

---

## Suggested sequencing

- **Production adoption gate:** #32
- **DR muscle:** #7
- **Operator SDK follow-ups:** #41, #42 if install scale or tenant constraints require them
- **Documentation:** #27
- **When stable enough for external use:** #30 (open-source prep if that's the path)
