# Bloodraven — Feature & Documentation Wishlist

## Checklist

- [ ] 7. Cross-region/cross-cluster DR as a first-class feature
- [x] 9. Restore duration and size metrics
- [x] 18. `kubectl` plugin
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
- [x] 39. Default resource requests and security contexts audit
- [ ] 40. Observability review gate for metrics and alerts
- [ ] 41. Safe Secret watch narrowing design
- [ ] 42. Namespace-scoped watch/cache mode evaluation

## P0 — Production adoption blockers

**31. Documentation publishing parity.** The public ReadTheDocs site has lagged behind `main`, causing current features such as planned failover, multi-site, backup verification/encryption, dashboards, and security docs to appear missing or 404 during external evaluation. Make docs publishing part of CI/release: build Docusaurus on every PR, publish on merge to `main`, verify `llms-full.txt` includes all current docs, and add a link-check job for the public site. The docs site must be a trustworthy source of truth before anyone evaluates Bloodraven for production.

**32. Real-cluster E2E CI gate.** Unit/component/envtest coverage is not enough for a MySQL failover operator. Add an optional-but-required-before-release k3d/kind CI job that installs the chart and exercises real MySQL pods, PVCs, Services, DNS/DNSEndpoint behavior, taints, planned failover, emergency failover, operator restart, PVC loss, NetworkPolicy partition, backup restore, and PITR verification. This should run at least on release tags and nightly; if cost is acceptable, run a reduced smoke subset on PRs.

## P1 — DR and operational completeness

**7. Cross-region/cross-cluster DR as a first-class feature.** Today DR = "create a new MysqlFailoverGroup with `initFromBackup` in another cluster." This works but is ad-hoc. Consider a `MysqlDRTarget` CR that continuously ships backups + binlogs to a designated target cluster/bucket and can be promoted with one command. At minimum, document the recommended multi-cluster DR topology with a runbook.

**9. Restore duration and size metrics.** Done: restores now publish data-plane duration, last-success timestamp, and last-source-size metrics with bounded labels. Both bootstrap and in-place restore status surfaces source backup size/coordinates, target GTID/binlog coordinates, and PITR replay summary when available.

**39. Default resource requests and security contexts audit.** Done: every operator-created Pod, Deployment, StatefulSet, Job, and CronJob path was inventoried for resource requests and security-context coverage. Resource gaps are filled on the cleanup, restore, and verification Jobs (main container reuses `spec.backup.resources`; init containers — `pitr-download`, `decrypt-download` — default to `100m`/`128Mi` requests, no limits, and accept the same override). Execution Jobs (backup, cleanup, restore, restore-in-place, verification) now set `automountServiceAccountToken: false`; schedule-trigger CronJob pods keep the token because they POST a CR through the in-cluster API. The Helm operator container picks up `seccompProfile: RuntimeDefault` to match the pod-level setting. New opt-in CR fields `spec.podSecurityContext`, `spec.containerSecurityContext`, `spec.dragonfly.podSecurityContext`, and `spec.dragonfly.containerSecurityContext` let cluster operators turn on Kubernetes Restricted PSS for the MySQL and Dragonfly StatefulSets on their own schedule; defaults stay unchanged so existing PVCs are not disrupted on upgrade. See `docs/docs/production-hardening.mdx` for the worked migration. Follow-up (#39-followup): tune `terminationGracePeriodSeconds` for MySQL and replace the conservative 100m/128Mi init-container default with profile-driven sizing.

**40. Observability review gate for metrics and alerts.** High priority: add a repeatable release/PR checklist for new metrics, recording rules, alerts, dashboard panels, Events, and runbook links. Each new metric or alert should have documentation, label-cardinality review, and a mapped runbook or explicit reason one is not needed. This is cheap process work that prevents observability drift as new operator behaviors ship.

**33. True shared-node placement model.** Done: each site now declares an explicit required `spec.sites[].taintNodeSelector`, allowing per-group labels such as `shipstream.io/failover-group.orders=true` and `shipstream.io/site.orders=iad`. Tainting, cleanup, docs, tests, and playground manifests use the selector model so failover in one group does not require dedicated node pools or affect unrelated tenants.

**36. CR replication enrichment stalls after in-lifecycle recovery.** Done: no-divergence old-primary recovery now persists `status.sites[].recoveryState=RecoveryInProgress` before running `RecoverOldPrimary`, keeps that state through a stabilization window, suppresses immediate recovery re-entry, and clears it only after healthy replication is observed so `replicating` and `gtidExecuted` are written back to CR status.

**37. Auto-fail-back to returning original primary is undocumented.** Done: the current contract is documented as GTID-freshest/current-state-driven rather than identity-driven. A returning original primary can be promoted again only through the normal candidate selection path, with anti-flap cooldown and site-priority tie-breaking still applying.

**38. `status.lastFailoverTarget` not durable across operator restart.** Done: `runner.startManager` rehydrates `lastFailoverTarget`, `lastFailover`, `RecoveryBlocked`, and `RecoveryInProgress` from CR status. A restarted operator clears in-progress recovery if replication is already healthy, or retries recovery immediately if it is still unhealthy.

## P2 — Observability and operability

**18. `kubectl` plugin.** Done: `cmd/kubectl-bloodraven` ships `status`, `promote`, `reclone`, `backup`, and `verify-backup`. Each writes only API objects the operator already understands, validates inputs before posting, and supports synchronous `--wait` on the long-running ones. Built via `make build-kubectl-plugin`; documented under [kubectl plugin](docs/docs/kubectl-plugin.mdx).

## P3 — Documentation deliverables

**27. Backup/restore performance guide.** For a 500 GB dataset, how long does `util.dumpInstance` take with what `threads`/`bytesPerChunk`? How long does `loadDump` take? At what `maxLagSeconds` does your replica-as-source fallback trigger and what's the primary-impact if it does? Users need ballparks before they commit.

**30. Public repo, license, release cadence.** If this isn't going external, skip. If it might — Apache-2.0, semver on the CRD and the operator separately, `CHANGELOG.md`, GitHub releases with signed images, published Helm chart index. The bar for "a real project someone else will adopt" is higher than the bar for "our internal tool."

**35. Operator SDK Best Practices review.** Done: the Operator SDK best-practices review was converted into internal follow-ups in this wishlist. High-priority follow-ups are #39 and #40; lower-priority follow-ups are #41 and #42. NetworkPolicy ownership is intentionally not a follow-up: Bloodraven should continue treating tenant NetworkPolicy as platform-owned rather than operator-owned by default.

**41. Safe Secret watch narrowing design.** Low priority: evaluate whether the broad `Secret` watch in `MysqlFailoverGroupReconciler.SetupWithManager` can be narrowed safely without missing credential or TLS rotation. Possible approaches include labels for Bloodraven-managed or referenceable Secrets, field indexes, a narrower event predicate, or cache selectors. Do not change behavior without controller or envtest coverage for every Secret reference path.

**42. Namespace-scoped watch/cache mode evaluation.** Low priority: decide whether Bloodraven should support namespace-scoped manager watch/cache mode in addition to the current cluster-scoped model. This is useful for locked-down tenant installs, but it changes reconciliation coverage and deployment assumptions, so treat it as an install-mode design rather than a quick flag.

---

## Suggested sequencing

- **Production adoption gate:** #31, #32
- **Operator correctness (fix before next release):** #36, #38 — silent post-chaos data gaps in the CR
- **Operator semantics to nail down:** #37 — pick a fail-back rule and document it
- **Operator SDK follow-ups:** #39, #40 first; #41, #42 later if install scale or tenant constraints require them
- **Next quarter:** #7, #9, #18 (DR muscle + day-2 ergonomics)
- **When stable enough for external use:** #30 (open-source prep if that's the path)
