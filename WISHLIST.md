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
- [ ] 34. CR replication enrichment stalls after in-lifecycle recovery
- [ ] 35. Auto-fail-back to returning original primary is undocumented
- [ ] 36. `status.lastFailoverTarget` not durable across operator restart

## P0 — Production adoption blockers

**31. Documentation publishing parity.** The public ReadTheDocs site has lagged behind `main`, causing current features such as planned failover, multi-site, backup verification/encryption, dashboards, and security docs to appear missing or 404 during external evaluation. Make docs publishing part of CI/release: build Docusaurus on every PR, publish on merge to `main`, verify `llms-full.txt` includes all current docs, and add a link-check job for the public site. The docs site must be a trustworthy source of truth before anyone evaluates Bloodraven for production.

**32. Real-cluster E2E CI gate.** Unit/component/envtest coverage is not enough for a MySQL failover operator. Add an optional-but-required-before-release k3d/kind CI job that installs the chart and exercises real MySQL pods, PVCs, Services, DNS/DNSEndpoint behavior, taints, planned failover, emergency failover, operator restart, PVC loss, NetworkPolicy partition, backup restore, and PITR verification. This should run at least on release tags and nightly; if cost is acceptable, run a reduced smoke subset on PRs.

## P1 — DR and operational completeness

**7. Cross-region/cross-cluster DR as a first-class feature.** Today DR = "create a new MysqlFailoverGroup with `initFromBackup` in another cluster." This works but is ad-hoc. Consider a `MysqlDRTarget` CR that continuously ships backups + binlogs to a designated target cluster/bucket and can be promoted with one command. At minimum, document the recommended multi-cluster DR topology with a runbook.

**9. Restore duration and size metrics.** Add `bloodraven_restore_duration_seconds` and `bloodraven_restore_last_success_timestamp_seconds`, plus per-restore GTID and binlog-replay coordinates in status. DR confidence requires knowing your actual measured restore time, not estimated.

**33. True shared-node placement model.** Done: each site now declares an explicit required `spec.sites[].taintNodeSelector`, allowing per-group labels such as `shipstream.io/failover-group.orders=true` and `shipstream.io/site.orders=iad`. Tainting, cleanup, docs, tests, and playground manifests use the selector model so failover in one group does not require dedicated node pools or affect unrelated tenants.

**34. CR replication enrichment stalls after in-lifecycle recovery.** *(Surfaced 2026-04-29 by the `cmd/playground-chaos` runner.)* After any operator-driven recovery — emergency failover, auto-fail-back, "no GTID divergence" recovery — `MysqlFailoverGroup.status.sites[].replicating` and `gtidExecuted` stop populating on the post-recovery read-only site, and stay null for the rest of the operator lifecycle. The topology manager keeps polling (state transitions still log) and the sidecar `/status` endpoint correctly reports `replica_io_running=true,replica_sql_running=true`, but `siteRepl[i]` for the recovered replica is never re-populated; only an operator restart fixes it. The Ready condition is unaffected because `replicationHealthy` defaults true when all `Replication` snapshots are nil (`internal/controller/runner.go:702-712`), so the gap is silent. Impact: `internal/controller/planned_failover.go:209` reads `targetStatus.Replicating` directly as a safety check, so any planned switchover after the first chaos event in the same lifecycle is rejected as `TargetUnhealthy` even though replication is healthy. Investigate `internal/controller/topology.go` poll path — likely a stale internal role or condition gates the replica check after promotion/recovery transitions. Add a regression test in the chaos runner that asserts `replicating=true` becomes visible within ~30s after `01-clean-primary-kill` cleanup, without requiring a restart.

**35. Auto-fail-back to returning original primary is undocumented.** *(Surfaced 2026-04-29 by scenario `12-old-primary-recovery-no-divergence`.)* When the original primary is scaled to 0 and a peer is promoted, scaling the original back up causes the operator to fail *back* to it rather than rejoining it as a replica. The peer is then demoted, and the "no GTID divergence, auto-recovering" recovery path runs on the *peer* (now the new old primary), not on the originally-killed site. The cluster still converges to one primary + one replica, but identity-based test assertions and runbooks that say "old primary becomes a replica" are wrong. Either: (a) document the fail-back rule and the conditions under which it triggers (anti-flap cooldown, GTID-freshness winner, `lastFailoverTarget` recency, etc.), or (b) suppress fail-back unless an explicit `spec.failbackPolicy` opt-in is set so the system stays where the most-recent failover left it.

**36. `status.lastFailoverTarget` not durable across operator restart.** Documented in `CLAUDE.md` and `playground/chaos-results.md` but worth tracking: after an operator restart, `lastFailoverTarget` is restored from the CR (so it survives), but related anti-flap state and old-primary-recovery dispatch keys are in-memory only. Recovery for an old primary won't re-trigger until the next failover within the new operator lifecycle. Reproducible via `kubectl rollout restart deployment bloodraven` mid-recovery. Likely fix: persist the relevant in-flight recovery dispatch keys to `status` rather than only memory, or recompute them from CR on startup.

## P2 — Observability and operability

**18. `kubectl` plugin.** `kubectl bloodraven status`, `kubectl bloodraven promote <group> <site>`, `kubectl bloodraven backup <group> --profile nightly`, `kubectl bloodraven verify-backup <name>`. Reduces the `kubectl exec ... mysql -e ...` surface area that's currently in the ops docs.

## P3 — Documentation deliverables

**27. Backup/restore performance guide.** For a 500 GB dataset, how long does `util.dumpInstance` take with what `threads`/`bytesPerChunk`? How long does `loadDump` take? At what `maxLagSeconds` does your replica-as-source fallback trigger and what's the primary-impact if it does? Users need ballparks before they commit.

**30. Public repo, license, release cadence.** If this isn't going external, skip. If it might — Apache-2.0, semver on the CRD and the operator separately, `CHANGELOG.md`, GitHub releases with signed images, published Helm chart index. The bar for "a real project someone else will adopt" is higher than the bar for "our internal tool."

---

## Suggested sequencing

- **Production adoption gate:** #31, #32
- **Operator correctness (fix before next release):** #34, #36 — silent post-chaos data gaps in the CR
- **Operator semantics to nail down:** #35 — pick a fail-back rule and document it
- **Next quarter:** #7, #9, #18 (DR muscle + day-2 ergonomics)
- **When stable enough for external use:** #30 (open-source prep if that's the path)
