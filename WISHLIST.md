# Bloodraven — Feature & Documentation Wishlist

## Checklist

- [ ] 7. Cross-region/cross-cluster DR as a first-class feature
- [ ] 9. Restore duration and size metrics
- [ ] 18. `kubectl` plugin
- [ ] 27. Backup/restore performance guide
- [ ] 30. Public repo, license, release cadence
- [ ] 31. Documentation publishing parity
- [ ] 32. Real-cluster E2E CI gate
- [x] 33. True shared-node placement model
- [ ] 34. Investigate using [Scorecard](https://sdk.operatorframework.io/docs/testing-operators/scorecard/) to test Bloodraven operator.
- [x] 35. Read the [Operator SDK Best Practices](https://sdk.operatorframework.io/docs/best-practices/) and see if there are any lessons we can learn and apply to Bloodraven.
- [x] 36. CR replication enrichment stalls after in-lifecycle recovery
- [x] 37. Auto-fail-back to returning original primary is undocumented
- [x] 38. `status.lastFailoverTarget` not durable across operator restart

## P0 — Production adoption blockers

**31. Documentation publishing parity.** The public ReadTheDocs site has lagged behind `main`, causing current features such as planned failover, multi-site, backup verification/encryption, dashboards, and security docs to appear missing or 404 during external evaluation. Make docs publishing part of CI/release: build Docusaurus on every PR, publish on merge to `main`, verify `llms-full.txt` includes all current docs, and add a link-check job for the public site. The docs site must be a trustworthy source of truth before anyone evaluates Bloodraven for production.

**32. Real-cluster E2E CI gate.** Unit/component/envtest coverage is not enough for a MySQL failover operator. Add an optional-but-required-before-release k3d/kind CI job that installs the chart and exercises real MySQL pods, PVCs, Services, DNS/DNSEndpoint behavior, taints, planned failover, emergency failover, operator restart, PVC loss, NetworkPolicy partition, backup restore, and PITR verification. This should run at least on release tags and nightly; if cost is acceptable, run a reduced smoke subset on PRs.

## P1 — DR and operational completeness

**7. Cross-region/cross-cluster DR as a first-class feature.** Today DR = "create a new MysqlFailoverGroup with `initFromBackup` in another cluster." This works but is ad-hoc. Consider a `MysqlDRTarget` CR that continuously ships backups + binlogs to a designated target cluster/bucket and can be promoted with one command. At minimum, document the recommended multi-cluster DR topology with a runbook.

**9. Restore duration and size metrics.** Add `bloodraven_restore_duration_seconds` and `bloodraven_restore_last_success_timestamp_seconds`, plus per-restore GTID and binlog-replay coordinates in status. DR confidence requires knowing your actual measured restore time, not estimated.

**33. True shared-node placement model.** Done: each site now declares an explicit required `spec.sites[].taintNodeSelector`, allowing per-group labels such as `shipstream.io/failover-group.orders=true` and `shipstream.io/site.orders=iad`. Tainting, cleanup, docs, tests, and playground manifests use the selector model so failover in one group does not require dedicated node pools or affect unrelated tenants.

**36. CR replication enrichment stalls after in-lifecycle recovery.** Done: no-divergence old-primary recovery now persists `status.sites[].recoveryState=RecoveryInProgress` before running `RecoverOldPrimary`, keeps that state through a stabilization window, suppresses immediate recovery re-entry, and clears it only after healthy replication is observed so `replicating` and `gtidExecuted` are written back to CR status.

**37. Auto-fail-back to returning original primary is undocumented.** Done: the current contract is documented as GTID-freshest/current-state-driven rather than identity-driven. A returning original primary can be promoted again only through the normal candidate selection path, with anti-flap cooldown and site-priority tie-breaking still applying.

**38. `status.lastFailoverTarget` not durable across operator restart.** Done: `runner.startManager` rehydrates `lastFailoverTarget`, `lastFailover`, `RecoveryBlocked`, and `RecoveryInProgress` from CR status. A restarted operator clears in-progress recovery if replication is already healthy, or retries recovery immediately if it is still unhealthy.

## P2 — Observability and operability

**18. `kubectl` plugin.** `kubectl bloodraven status`, `kubectl bloodraven promote <group> <site>`, `kubectl bloodraven backup <group> --profile nightly`, `kubectl bloodraven verify-backup <name>`. Reduces the `kubectl exec ... mysql -e ...` surface area that's currently in the ops docs.

## P3 — Documentation deliverables

**27. Backup/restore performance guide.** For a 500 GB dataset, how long does `util.dumpInstance` take with what `threads`/`bytesPerChunk`? How long does `loadDump` take? At what `maxLagSeconds` does your replica-as-source fallback trigger and what's the primary-impact if it does? Users need ballparks before they commit.

**30. Public repo, license, release cadence.** If this isn't going external, skip. If it might — Apache-2.0, semver on the CRD and the operator separately, `CHANGELOG.md`, GitHub releases with signed images, published Helm chart index. The bar for "a real project someone else will adopt" is higher than the bar for "our internal tool."

**35. Operator SDK Best Practices review.** Done: the reviewed Operator SDK best-practices pages are captured in the [Operator SDK Best Practices Assessment](docs/docs/operator-sdk-best-practices-assessment.mdx), with applicability decisions for controller shape, namespaces, CRDs/status, resources and security context, pruning, multitenancy, lean watches, observability, and follow-up tradeoffs such as the broad `Secret` watch.

---

## Suggested sequencing

- **Production adoption gate:** #31, #32
- **Operator correctness (fix before next release):** #36, #38 — silent post-chaos data gaps in the CR
- **Operator semantics to nail down:** #37 — pick a fail-back rule and document it
- **Next quarter:** #7, #9, #18 (DR muscle + day-2 ergonomics)
- **When stable enough for external use:** #30 (open-source prep if that's the path)
