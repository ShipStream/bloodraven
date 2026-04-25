# Bloodraven — Feature & Documentation Wishlist

## Checklist

- [ ] 7. Cross-region/cross-cluster DR as a first-class feature
- [ ] 9. Restore duration and size metrics
- [ ] 12. PVC loss recovery runbook
- [ ] 18. `kubectl` plugin
- [ ] 25. CRD version-migration plan
- [ ] 27. Backup/restore performance guide
- [ ] 28. Network-partition behavior
- [ ] 29. Known limitations, up-front
- [ ] 30. Public repo, license, release cadence

## P1 — DR and operational completeness

**7. Cross-region/cross-cluster DR as a first-class feature.** Today DR = "create a new MysqlFailoverGroup with `initFromBackup` in another cluster." This works but is ad-hoc. Consider a `MysqlDRTarget` CR that continuously ships backups + binlogs to a designated target cluster/bucket and can be promoted with one command. At minimum, document the recommended multi-cluster DR topology with a runbook.

**9. Restore duration and size metrics.** Add `bloodraven_restore_duration_seconds` and `bloodraven_restore_last_success_timestamp_seconds`, plus per-restore GTID and binlog-replay coordinates in status. DR confidence requires knowing your actual measured restore time, not estimated.

**12. PVC loss recovery runbook.** If site `iad`'s PVC is irrecoverable, what exactly do I do? Delete the PVC, let the operator auto-clone from `pdx`? Is there a failure mode where auto-clone runs against a still-replicating stale state? Write the runbook with the expected `Bootstrapping` condition transitions and timing.

## P2 — Observability and operability

**18. `kubectl` plugin.** `kubectl bloodraven status`, `kubectl bloodraven promote <group> <site>`, `kubectl bloodraven backup <group> --profile nightly`, `kubectl bloodraven verify-backup <name>`. Reduces the `kubectl exec ... mysql -e ...` surface area that's currently in the ops docs.

## P3 — Documentation deliverables

**25. CRD version-migration plan.** Currently `v1alpha1`. Document the path to `v1beta1` → `v1`, with conversion-webhook commitments. Users pinning to `v1alpha1` need to know the breaking-change contract.

**27. Backup/restore performance guide.** For a 500 GB dataset, how long does `util.dumpInstance` take with what `threads`/`bytesPerChunk`? How long does `loadDump` take? At what `maxLagSeconds` does your replica-as-source fallback trigger and what's the primary-impact if it does? Users need ballparks before they commit.

**28. Network-partition behavior.** Explicitly documented scenarios: (a) operator ↔ site-A partition (site-B reachable); (b) site-A ↔ site-B partition (operator reachable to both); (c) asymmetric partition (operator reachable to A, A not reachable to B). For each, the expected observable behavior, which metric moves, which event fires.

**29. Known limitations, up-front.** Today "known limitations" appears mid-way through the PITR section. Move to a top-level page: two-site only, single-operator-replica (until #1), bootstrap-only restore (until #6), `status.pitr` unpopulated (until #15), `BackupPITRNotImplemented` event semantics, etc. Calibrate user expectations before they write production manifests.

**30. Public repo, license, release cadence.** If this isn't going external, skip. If it might — Apache-2.0, semver on the CRD and the operator separately, `CHANGELOG.md`, GitHub releases with signed images, published Helm chart index. The bar for "a real project someone else will adopt" is higher than the bar for "our internal tool."

---

## Suggested sequencing

- **Next quarter:** #7, #9, #12, #18, #28 (DR muscle + day-2 ergonomics)
- **When stable enough for external use:** #25, #30 (open-source prep if that's the path)
