# Bloodraven — Feature & Documentation Wishlist

## Checklist

- [x] 1. Operator HA / leader election (docs-only; multi-replica HA is an intentional non-goal)
- [x] 2. Document the RPO model explicitly
- [x] 3. Split-brain auto-resolution option
- [x] 4. Fencing durability during operator-down + partial partition
- [x] 5. Reclone safety interlocks
- [x] 6. In-place restore path
- [ ] 7. Cross-region/cross-cluster DR as a first-class feature
- [x] 8. DR drill automation / backup verification (Phases 1 and 2 shipped; `kubectl` plugin + Grafana panels via #16/#18)
- [ ] 9. Restore duration and size metrics
- [x] 10. Three-or-more-site topology
- [ ] 11. Graceful planned-failover API
- [ ] 12. PVC loss recovery runbook
- [ ] 13. Backup encryption at rest
- [x] 14. Sidecar archiver resilience
- [x] 15. Populate `status.pitr`
- [ ] 16. Grafana dashboards
- [x] 17. Log schema contract
- [ ] 18. `kubectl` plugin
- [x] 19. WebSocket vs REST casing inconsistency
- [x] 20. Metric naming conformance
- [x] 21. "Why not group replication?" page
- [x] 22. Production hardening checklist
- [x] 23. Failure-mode matrix
- [x] 24. Upgrade / version-skew policy
- [ ] 25. CRD version-migration plan
- [ ] 26. Security model / threat model doc
- [ ] 27. Backup/restore performance guide
- [ ] 28. Network-partition behavior
- [ ] 29. Known limitations, up-front
- [ ] 30. Public repo, license, release cadence

## P0 — Correctness and safety gaps

**1. Operator HA / leader election.** The docs state the operator runs as a single-replica Deployment. The sidecar's self-fencing mitigates data-corruption risk during operator downtime, but *new failovers are blocked* until the operator is back. Add leader-elected multi-replica support (controller-runtime's built-in lease-based election) and document the exact behavior during operator-down windows — specifically: what a primary failure during operator downtime looks like from the application's perspective.

**2. Document the RPO model explicitly.** Async replication means non-zero RPO on emergency failover. The operator correctly surfaces `promotionGtidExecuted` and `divergentGtid`, but the docs never give the reader a bounded RPO statement. Add a "Durability and RPO" page that spells out: (a) committed-but-unreplicated transactions are lost on hard primary failure; (b) the theoretical lower bound is `secondsBehindSource` at the moment of failure; (c) PITR narrows this to `max_binlog_size` rotation cadence *only* if the primary's binlog tail survives (which it doesn't on PVC loss). Users should not have to infer this.

**3. Split-brain auto-resolution option.** Today, `writable/writable` → alert only. For users with a strict authoritative-site policy (e.g., "iad always wins ties"), offer an opt-in `spec.splitBrainPolicy` with `manual` (current) and `preferSite: <name>` options. Document the data-reconciliation implications loudly.

**4. Fencing durability during operator-down + partial partition.** ~~Current design: sidecar self-fences when both operator *and* peer are unreachable.~~ — done. The sidecar now polls the operator's `/active-site` every `peerCheckInterval` tick, and peer sidecars relay their cached view via `/peer/active-site`. When the authoritative `activeSite` disagrees with `mySite` and MySQL is still writable, the sidecar fences immediately — independent of lease timing and operator reachability. The lease-expiry rule remains as a backstop for the "everything silent" case. A returning stale primary that can reach only its peer (not the operator) now fences within one tick of adopting the peer's fresher view. Tests in `internal/sidecar/{fencing,topology_cache,server}_test.go`; sequence diagram in `docs/docs/operator-availability.mdx#operator-down--partial-partition-stale-primary-scenario`.

**5. Reclone safety interlocks.** The reclone annotation triggers `CLONE INSTANCE` which wipes the target. Add a confirmation mechanism — either a `confirmReclone: <site-name>` field that must match, or require the annotation value to include the current `divergentGtid` prefix. Fat-fingering `reclone-site=iad` when you meant `pdx` is a career-ending mistake.

**6. In-place restore path.** Bootstrap-only restore is fine for greenfield DR, but real incidents often want "roll this cluster back to 14:32 UTC" without teardown/rename. Add `spec.restoreInPlace` with a required confirmation field and a clear pre-flight that fences both sites, blocks the `-primary` Service, runs the restore, and resumes. Document the exact state transitions.

## P1 — DR and operational completeness

**7. Cross-region/cross-cluster DR as a first-class feature.** Today DR = "create a new MysqlFailoverGroup with `initFromBackup` in another cluster." This works but is ad-hoc. Consider a `MysqlDRTarget` CR that continuously ships backups + binlogs to a designated target cluster/bucket and can be promoted with one command. At minimum, document the recommended multi-cluster DR topology with a runbook.

**8. DR drill automation / backup verification.** ~~Ship a `MysqlBackupVerification` CRD or CronJob template that periodically restores the latest backup into a throwaway namespace, runs a sanity query, and emits a `bloodraven_backup_verified_timestamp_seconds` gauge.~~ — Phases 1 and 2 shipped. A `MysqlBackupVerification` CRD plus `spec.backup.profiles[].verification` block schedules periodic restore-of-the-latest-backup into an ephemeral mysqld pod+PVC in the operator namespace. `spec.pointInTime` drives PITR binlog replay via a `bloodraven pitr-download` init container + `mysqlbinlog` piped into the ephemeral mysqld; `spec.sanityCheck` runs a scalar SELECT with a client-side timeout. The `bloodraven_backup_verified_timestamp_seconds` gauge plus duration, attempts-counter, and `bloodraven_backup_verification_replay_lag_seconds` metrics mirror the `bloodraven_backup_*` family. `MysqlBackup.status.mysqlImage` captures the active-site image tag at dump time for version-pinned drills. See `proposals/08-backup-verification.md` and `docs/docs/backup-verification.mdx`.

**9. Restore duration and size metrics.** Add `bloodraven_restore_duration_seconds` and `bloodraven_restore_last_success_timestamp_seconds`, plus per-restore GTID and binlog-replay coordinates in status. DR confidence requires knowing your actual measured restore time, not estimated.

**10. Three-or-more-site topology.** ~~Hard-coded two-site is fine for now~~ — done. `spec.sites[]` now accepts 2–N entries with a `role` of `primary-candidate` (promotable) or `dr-only` (passive follower). Split-brain ties are broken by `spec.splitBrainPolicy.sitePriorities` (ordered list). Replication is a star: every replica follows the active primary. Sidecar self-fencing was generalised to "operator AND every peer unreachable". See `docs/docs/multi-site.mdx`.

**11. Graceful planned-failover API.** Manual promotion today is a multi-step `kubectl exec` dance documented in ops. Replace with a single annotation or subresource — `kubectl annotate mysqlfailovergroup orders bloodraven.shipstream.io/planned-failover=pdx` — that drains writes, waits for zero replication lag, and promotes atomically. The anti-flap cooldown bypass is noted as a caution; make the planned path not bypass it.

**12. PVC loss recovery runbook.** If site `iad`'s PVC is irrecoverable, what exactly do I do? Delete the PVC, let the operator auto-clone from `pdx`? Is there a failure mode where auto-clone runs against a still-replicating stale state? Write the runbook with the expected `Bootstrapping` condition transitions and timing.

**13. Backup encryption at rest.** `util.dumpInstance` supports zstd compression but the docs don't mention encryption. For compliance-sensitive workloads, add either native dump encryption (a passphrase secret) or document the KMS/bucket-encryption story and make it a first-class config field.

**14. Sidecar archiver resilience.** The archiver has `lastError` in its status endpoint but no metric and no retry/backoff guarantees documented. Add `bloodraven_archiver_upload_failures_total`, `bloodraven_archiver_last_upload_timestamp_seconds`, and `bloodraven_archiver_backlog_files` (count of sealed binlogs not yet uploaded). Stale binlog archival is a silent RPO regression.

## P2 — Observability and operability

**15. Populate `status.pitr`.** Docs admit it's unpopulated ("reserved for future use"). Without this, `kubectl describe` doesn't tell the operator's story — they have to `kubectl exec` or scrape metrics. Fill in `oldestArchivedBinlog`, `newestArchivedBinlog`, `archiveCoverageFrom`, `archiveCoverageTo`, and `lastArchiveError` per site.

**16. Grafana dashboards.** Ship opinionated dashboards in the Helm chart (`bloodraven/grafana-dashboards/`). One for per-failover-group health, one for backup/PITR, one for operator internals. Users will build their own otherwise, poorly.

**17. Log schema contract.** ~~"Structured JSON" is mentioned but the field set isn't documented. Publish a log-schema reference — what fields are stable, what the `msg` values are for key events (`failover`, `promotion`, `divergence-detected`, `reclone-started`). Downstream log pipelines need this.~~ — shipped. `docs/docs/log-schema.mdx` is the contract: it splits the operational `slog` stream from the controller-runtime `zap` stream, lists every common field, and pins the stable `msg` vocabulary for failover (`initiating failover` → `failover complete` → `promotion confirmed: site is writable`, plus `failover failed`), divergence (`divergence detected`), recovery (`old primary recovery complete`), and reclone (the canonical `starting bootstrap` event with `source="reclone"`). Cross-linked from `monitoring.mdx` and `operations.mdx`. As part of the cleanup, removed two duplicate emissions (`starting bootstrap` and `old primary recovery complete` were each emitted twice) and migrated the remaining `snake_case` log keys in the sidecar startup paths to `camelCase` to match the rest of the codebase.

**18. `kubectl` plugin.** `kubectl bloodraven status`, `kubectl bloodraven promote <group> <site>`, `kubectl bloodraven backup <group> --profile nightly`, `kubectl bloodraven verify-backup <name>`. Reduces the `kubectl exec ... mysql -e ...` surface area that's currently in the ops docs.

**19. WebSocket vs REST casing inconsistency.** Docs already call out that WebSocket uses camelCase and REST uses snake_case. Fix this. Pick one (camelCase is more common in Kubernetes APIs) and migrate the other with a deprecation window.

**20. Metric naming conformance.** `bloodraven_failovers_total`, `bloodraven_backup_runs_total` — good. `bloodraven_websocket_clients` should be `bloodraven_websocket_connected_clients` (Prometheus convention: gauge names describe the quantity). Tiny nit, but worth cleaning up while you still can.

## P3 — Documentation deliverables

**21. "Why not group replication?" page.** State the architectural rationale explicitly: zero commit latency, single-node write availability, no quorum requirement, simpler mental model. Readers evaluating this against MySQL InnoDB Cluster need this up-front so they stop asking.

**22. Production hardening checklist.** One page, bullets, no prose:
- Pin `image`, `sidecarImage`, `backup.image` to immutable tags
- Configure per-role credentials (not legacy DSN mode)
- Set `failoverCooldown` ≥ 5m
- Configure `replication.maxLagSeconds` based on your RPO tolerance
- Enable PITR with `maxBinlogSize` sized to your RPO target
- Mirror operator + sidecar images to a private registry
- Enable backup verification CronJob
- Configure alert rules (provide as PrometheusRule resource)
- Label nodes with fault-domain zones
- Use `OrderedUpdate`
- etc.

**23. Failure-mode matrix.** A table: *failure* × *observable signal* × *operator action* × *operator-time-to-act* × *operator limitations*. Rows: pod killed, node lost, PVC lost, AZ partition, cross-site partition, operator pod crash, both operator + one site down, split brain, DNS provider down, S3 unreachable. Users currently have to piece this together from the state-machine docs.

**24. Upgrade / version-skew policy.** ~~What MySQL versions are supported? What's the MySQL major-version upgrade path (8.0 → 8.4 → 9.x)? Does the operator support running the two sites at different MySQL versions transiently during upgrade? What's the sidecar ↔ operator version skew guarantee? The docs are silent.~~ — shipped. `docs/docs/upgrade-policy.mdx` states a narrow, opinionated stance: `mysql:9.6` today, `mysql:9.7` once the first 9.x LTS ships (and pinned there long-term); MySQL 8.x and post-9.7 Innovation releases are out of scope. The page covers the `OrderedUpdate` replica-first rollout as the only supported upgrade path, permits transient cross-site skew during rollout but not steady-state, and commits to a one-minor sidecar ↔ operator skew window tied to the additive-only HTTP surface. Cross-linked from `operations.mdx`, `production-hardening.mdx`, and `failover.mdx`.

**25. CRD version-migration plan.** Currently `v1alpha1`. Document the path to `v1beta1` → `v1`, with conversion-webhook commitments. Users pinning to `v1alpha1` need to know the breaking-change contract.

**26. Security model / threat model doc.** Who can do what? What happens if the operator's ServiceAccount token leaks? What happens if the MySQL root password leaks? What's the blast radius of a compromised sidecar? The credentials docs cover *how* but not *what an attacker sees*.

**27. Backup/restore performance guide.** For a 500 GB dataset, how long does `util.dumpInstance` take with what `threads`/`bytesPerChunk`? How long does `loadDump` take? At what `maxLagSeconds` does your replica-as-source fallback trigger and what's the primary-impact if it does? Users need ballparks before they commit.

**28. Network-partition behavior.** Explicitly documented scenarios: (a) operator ↔ site-A partition (site-B reachable); (b) site-A ↔ site-B partition (operator reachable to both); (c) asymmetric partition (operator reachable to A, A not reachable to B). For each, the expected observable behavior, which metric moves, which event fires.

**29. Known limitations, up-front.** Today "known limitations" appears mid-way through the PITR section. Move to a top-level page: two-site only, single-operator-replica (until #1), bootstrap-only restore (until #6), `status.pitr` unpopulated (until #15), `BackupPITRNotImplemented` event semantics, etc. Calibrate user expectations before they write production manifests.

**30. Public repo, license, release cadence.** If this isn't going external, skip. If it might — Apache-2.0, semver on the CRD and the operator separately, `CHANGELOG.md`, GitHub releases with signed images, published Helm chart index. The bar for "a real project someone else will adopt" is higher than the bar for "our internal tool."

---

## Suggested sequencing

- **This quarter:** #1, #2, #5, #6, #15, #22, #23 (close the worst operational sharp edges)
- **Next quarter:** #7, #8, #9, #11, #12, #18, #28 (DR muscle + day-2 ergonomics)
- **When stable enough for external use:** #21, #25, #26, #30 (open-source prep if that's the path)

