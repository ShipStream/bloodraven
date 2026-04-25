# Bloodraven — Feature & Documentation Wishlist

## Checklist

- [ ] 7. Cross-region/cross-cluster DR as a first-class feature
- [ ] 9. Restore duration and size metrics
- [ ] 18. `kubectl` plugin
- [ ] 25. CRD version-migration plan
- [ ] 27. Backup/restore performance guide
- [ ] 28. Network-partition behavior
- [ ] 30. Public repo, license, release cadence
- [ ] 31. Documentation publishing parity
- [ ] 32. Real-cluster E2E CI gate
- [ ] 33. True shared-node placement model
- [ ] 34. Production install examples and alert manifests

## P0 — Production adoption blockers

**31. Documentation publishing parity.** The public ReadTheDocs site has lagged behind `main`, causing current features such as planned failover, multi-site, backup verification/encryption, dashboards, and security docs to appear missing or 404 during external evaluation. Make docs publishing part of CI/release: build Docusaurus on every PR, publish on merge to `main`, verify `llms-full.txt` includes all current docs, and add a link-check job for the public site. The docs site must be a trustworthy source of truth before anyone evaluates Bloodraven for production.

**32. Real-cluster E2E CI gate.** Unit/component/envtest coverage is not enough for a MySQL failover operator. Add an optional-but-required-before-release k3d/kind CI job that installs the chart and exercises real MySQL pods, PVCs, Services, DNS/DNSEndpoint behavior, taints, planned failover, emergency failover, operator restart, PVC loss, NetworkPolicy partition, backup restore, and PITR verification. This should run at least on release tags and nightly; if cost is acceptable, run a reduced smoke subset on PRs.

## P1 — DR and operational completeness

**7. Cross-region/cross-cluster DR as a first-class feature.** Today DR = "create a new MysqlFailoverGroup with `initFromBackup` in another cluster." This works but is ad-hoc. Consider a `MysqlDRTarget` CR that continuously ships backups + binlogs to a designated target cluster/bucket and can be promoted with one command. At minimum, document the recommended multi-cluster DR topology with a runbook.

**9. Restore duration and size metrics.** Add `bloodraven_restore_duration_seconds` and `bloodraven_restore_last_success_timestamp_seconds`, plus per-restore GTID and binlog-replay coordinates in status. DR confidence requires knowing your actual measured restore time, not estimated.

**33. True shared-node placement model.** The docs now describe per-group taints, but node discovery still relies on single-valued `shipstream.io/failover-group=<group>` and `shipstream.io/site=<site>` labels. That prevents one physical node from participating in multiple failover groups at the same site. Replace or extend this with a multi-valued-compatible selector model, such as per-group labels (`shipstream.io/failover-group.orders=true`, `shipstream.io/site.orders=iad`) or explicit `spec.sites[].taintNodeSelector`. Update tainting, cleanup, docs, and tests so failover in one group does not require dedicated node pools or affect unrelated tenants.

## P2 — Observability and operability

**18. `kubectl` plugin.** `kubectl bloodraven status`, `kubectl bloodraven promote <group> <site>`, `kubectl bloodraven backup <group> --profile nightly`, `kubectl bloodraven verify-backup <name>`. Reduces the `kubectl exec ... mysql -e ...` surface area that's currently in the ops docs.

**34. Production install examples and alert manifests.** The docs mention hardening, NetworkPolicy, ServiceMonitor, PrometheusRule, Cloudflare/external-dns, and k3s, but users still have to assemble production manifests themselves. Ship copy-pasteable examples for: NetworkPolicy around operator/sidecar/auxiliary HTTP, PrometheusRule alerts for failover/data-loss/backup/PITR/archiver lag, Cloudflare external-dns configuration notes, k3s storage-class guidance, and a production Helm values overlay. Keep examples generic, but make the ShipStream/k3s/Cloudflare path first-class enough to validate adoption.

## P3 — Documentation deliverables

**25. CRD version-migration plan.** Currently `v1alpha1`. Document the path to `v1beta1` → `v1`, with conversion-webhook commitments. Users pinning to `v1alpha1` need to know the breaking-change contract.

**27. Backup/restore performance guide.** For a 500 GB dataset, how long does `util.dumpInstance` take with what `threads`/`bytesPerChunk`? How long does `loadDump` take? At what `maxLagSeconds` does your replica-as-source fallback trigger and what's the primary-impact if it does? Users need ballparks before they commit.

**28. Network-partition behavior.** Explicitly documented scenarios: (a) operator ↔ site-A partition (site-B reachable); (b) site-A ↔ site-B partition (operator reachable to both); (c) asymmetric partition (operator reachable to A, A not reachable to B). For each, the expected observable behavior, which metric moves, which event fires.

**30. Public repo, license, release cadence.** If this isn't going external, skip. If it might — Apache-2.0, semver on the CRD and the operator separately, `CHANGELOG.md`, GitHub releases with signed images, published Helm chart index. The bar for "a real project someone else will adopt" is higher than the bar for "our internal tool."

---

## Suggested sequencing

- **Production adoption gate:** #31, #32, #33, #34
- **Next quarter:** #7, #9, #18, #28 (DR muscle + day-2 ergonomics)
- **When stable enough for external use:** #25, #30 (open-source prep if that's the path)
