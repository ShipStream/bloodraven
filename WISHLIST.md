# Bloodraven — Feature & Documentation Wishlist

## Checklist

- [ ] 7. Cross-region/cross-cluster DR as a first-class feature
- [ ] 27. Backup/restore performance guide
- [ ] 30. Public repo, license, release cadence
- [ ] 32. Real-cluster E2E CI gate
- [ ] 41. Safe Secret watch narrowing design
- [ ] 42. Namespace-scoped watch/cache mode evaluation

## P0 — Production adoption blockers

**32. Real-cluster E2E CI gate.** Unit/component/envtest coverage is not enough for a MySQL failover operator. Add an optional-but-required-before-release k3d/kind CI job that installs the chart and exercises real MySQL pods, PVCs, Services, DNS/DNSEndpoint behavior, taints, planned failover, emergency failover, operator restart, PVC loss, NetworkPolicy partition, backup restore, and PITR verification. This should run at least on release tags and nightly; if cost is acceptable, run a reduced smoke subset on PRs.

## P1 — DR and operational completeness

**7. Cross-region/cross-cluster DR as a first-class feature.** Today DR = "create a new MysqlFailoverGroup with `initFromBackup` in another cluster." This works but is ad-hoc. Consider a `MysqlDRTarget` CR that continuously ships backups + binlogs to a designated target cluster/bucket and can be promoted with one command. At minimum, document the recommended multi-cluster DR topology with a runbook.

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
