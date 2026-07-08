# Ultra Production-Readiness Audit — bloodraven

- **Scope:** whole repo (all tracked files)
- **Branch / SHA:** main @ dedf71f
- **Date:** 2026-07-02
- **Roles:** hardening, operability, stewardship
- **Models:** Opus 4.8, GPT-5.5 (xhigh), Qwen 3.7 Max, GLM 5.2
- **Buckets:** 8 (root+ci, api, charts, cmd+config, docs+examples, internal, playground, proposals+test)
- **Method:** 96 audit threads (8 buckets × 3 roles × 4 models) → 487 raw findings → 8 adversarial validation agents verified every finding against the code, deduped, and re-severitied → 6 rejected as INVALID, remainder merged into 239 distinct clusters.
- **Issues (validated, deduped):** 189 solid + 50 low-confidence — **Blocker: 0, High: 21, Medium: 83, Low: 85**

> Severity was re-assigned by validators after reading the actual code, and calibrated against the project's documented non-goals (async replication / non-zero RPO, non-HA control plane, best-effort relay-log drain, deferred DNS flip, intentionally-unauthenticated internal endpoints with NetworkPolicy guidance). Documented, intentional trade-offs were not counted as findings. Playground/dev-tooling and test-only defects are capped at Medium.

## Remediation checklist

- [x] **H6:** Redact credential SQL containing `IDENTIFIED BY` from MySQL credential reconciliation errors so plaintext passwords cannot leak to logs/events. Fixed in PR #109.
- [x] **H17:** Protect failover-critical topology state reads/writes with `tm.mu` in the emergency promotion path. Fixed in PR #109.
- [x] **H19:** Treat planned-failover rollback source unfence as best-effort so terminal failure handling and guard release still run. Fixed in PR #109.
- [x] **H1/H18/H13/H12/H7/H11/H10:** Shipped production-install correctness slice: fixed NetworkPolicy examples, auxiliary Service defaults/addressing, TLS DNS names, backup verification snippets, runbook pod/PVC names, and DR passphrase handling.
- [ ] **Next recommended slice:** version-skew and data-protection defaults: H9, H2, H21, and H14.
- [ ] **Then:** remaining failover/runtime hardening: H3, H5, H8, H15, H16, and H20.

## Cross-cutting themes (appear across many findings)

1. **Docs/examples drift from shipped code** — the single largest category. Multiple copy-pasteable production manifests and runbooks are wrong: the NetworkPolicy examples (H1, H18), the verification example (H7), the TLS cert (H12), the split-brain doc's removed `preferSite` field (H4), the StatefulSet-style runbook names (H11), and the DR-runbook credential handling (H10). These break real installs/incidents, not just reading comprehension.
2. **Sidecar/operator version skew** — the CRD hard-codes a 4-minor-stale sidecar image (`0.1.6`) as the default (H9); the chart `appVersion` (0.1.0), examples, and NOTES.txt all pin different/stale tags; `helm upgrade` never moves already-created CRs. One "resolve the sidecar image from the operator's own build version" fix collapses ~6 findings.
3. **Missing CRD validation for conditional/bounded fields** — PITR `profileName` (H2), sidecar durations (H3), verification `schedule`/`timestamp`, image `:latest`, profile-name uniqueness — all documented-as-required but unenforced, so misconfigurations pass admission and fail silently or at the worst moment. DragonflySpec already uses the correct CEL pattern; these fields just didn't copy it.
4. **Self-fencing / failover-loop robustness** — the WebSocket poll-loop freeze (H5, currently masked — see note), the data race on failover state (H17), DNS-before-promotion with no rollback (H15), the planned-failover rollback that strands the emergency-failover guard (H19), and credentials+TLS breaking all operator connections (H20).
5. **CI/release supply-chain & gate gaps** — unpinned `@latest` tool installs, unpinned third-party Actions on the signing pipeline, Trivy only on PRs, no RBAC-drift guard, release gate weaker than the PR gate, and 54 `cmd/`+`api/` test functions that never run in CI.

# Full finding tables

## High (21)

### H1. Shipped example NetworkPolicy blocks the operator<->sidecar and sidecar<->peer :8080 traffic the control plane requires, inducing a spurious self-fence / write outage when applied.
- **File(s):** `examples/networkpolicy.yaml:26`
- **Flagged by:** GLM 5.2, GPT-5.5, Opus 4.8, Qwen 3.7 Max (4 models, 6 raw findings); roles: hardening, operability, stewardship
- **Evidence:** In examples/networkpolicy.yaml the `orders-mysql-ingress` policy (lines 26-44) selects the MySQL pods with `podSelector.matchLabels: app.kubernetes.io/name: mysql` and permits ingress ONLY on `port: 3306` (two rules, lines 37-39 and 42-44). No rule opens the sidecar HTTP port `:8080`. The MySQL pod (which also contains the `sidecar` container) really does carry that label: internal/controller/reconciler.go:43 defines `labelAppName = "app.kubernetes.io/name"` and the pod template at reconciler.go:763 sets `labelAppName: "mysql"`. The operator reaches each sidecar on :8080 (internal/mysql/sidecar_client.go:41 baseURL `...svc.cluster.local:8080`, hitting `/peer/ping` and `/archiver/status`) and the peer sidecars ping each other on :8080 (internal/sidecar/fencing.go:220 `http://%s/peer/ping`). The first policy `bloodraven-operator-ingress` (lines 1-19) allows the operator only `monitoring ->
- **Impact:** A user who applies examples/networkpolicy.yaml (the intended use of a NetworkPolicy example -- and no placeholder substitution would ever add the missing rule) silently severs the operator's sidecar polling, the sidecar-to-peer fencing ping, and the sidecar-to-operator health path. Within one lease timeout the primary's fencing monitor loses both its operator-health and peer signals and self-fences (writes stop), which the operator then observes as a read-only primary and can escalate into an unnecessary failover -- a self-inflicted write outage from a shipped manifest, and precisely the "spurious unreachable" failure production-hardening.mdx warns is the #1 source.
- **Fix:** Add ingress rules to `orders-mysql-ingress` permitting TCP :8080 from the bloodraven namespace and from the co-selected MySQL pods (peer-ping), mirroring `mysql-sidecar-ingress` in production-install-examples.mdx:122-143; and either open the tenant sidecar's path to the operator health/aux ports in `bloodraven-operator-ingress` or document that self-fence depends on it. Cross-check the shipped example against the hardening doc so the two NetworkPolicy examples agree.

### H2. PITR can be enabled with an empty profileName; the CRD admits it and binlog archival is silently disabled, contradicting the field's own "Required when Enabled=true" contract.
- **File(s):** `api/v1alpha1/backup_types.go:122`
- **Flagged by:** GLM 5.2, GPT-5.5, Opus 4.8, Qwen 3.7 Max (4 models, 6 raw findings); roles: operability, stewardship
- **Evidence:** PITRSpec (backup_types.go:122) has no `XValidation` enforcing profileName when enabled. `ProfileName` (line 134) is `json:"profileName,omitempty"` with only `+kubebuilder:validation:MinLength=1` (line 133) — MinLength does not fire on an absent optional field, so `pitr:{enabled:true}` with profileName omitted passes CRD OpenAPI validation. The field comment (lines 132-134) states "Required when Enabled=true", but nothing enforces it: there is no validating webhook anywhere in the repo (grep for ValidatingWebhook/ValidateCreate returns nothing outside bitpoke/orchestrator), and `buildPITRSidecarFragments` at internal/controller/pitr.go:86-92 returns `out, nil` (no error, no status Condition) when `pitr.ProfileName == ""`. The only runtime signal is a transient Kubernetes Warning event (`BackupPITRInvalid`, internal/controller/backup_schedule.go:69) that ages out by default and is not surf
- **Impact:** An operator who enables PITR but forgets profileName gets a MysqlFailoverGroup that looks healthy: no admission rejection, no persistent degraded condition, only a Warning event that soon disappears. Continuous binlog archival never runs, so the point-in-time recovery window the feature exists to provide does not exist. The gap is discovered at the worst possible moment — during a restore, when only the last full dump is available and every transaction since it is unrecoverable (unbounded RPO gap for a data-protection feature the user believed was on).
- **Fix:** Add `+kubebuilder:validation:XValidation:rule="!self.enabled || (has(self.profileName) && size(self.profileName) > 0)",message="spec.backup.pitr.profileName is required when spec.backup.pitr.enabled is true"` to PITRSpec, mirroring DragonflySpec, and regenerate/copy the CRD to charts/. Additionally have the PITR path stamp a persistent status Condition (not just a transient event) when archival is disabled by misconfiguration.

### H3. SidecarSpec.LeaseTimeout / PeerCheckInterval have no minimum bound, so a too-small value collapses the self-fence tolerance window (spurious primary write outage) and 0/negative panics the sidecar.
- **File(s):** `api/v1alpha1/types.go:390`
- **Flagged by:** GLM 5.2, GPT-5.5, Opus 4.8 (3 models, 6 raw findings); roles: hardening, operability, stewardship
- **Evidence:** SidecarSpec.LeaseTimeout (types.go:390) and PeerCheckInterval (types.go:394) are `*metav1.Duration` carrying only `+kubebuilder:default` ("20s"/"5s") with NO `Minimum`/CEL floor. reconciler.go:804-810 stringifies them into the LEASE_TIMEOUT / PEER_CHECK_INTERVAL env vars; internal/sidecar/config.go:187-203 parses them with `time.ParseDuration` and no floor. In the fencing monitor: internal/sidecar/fencing.go:164 does `ticker := f.clock.NewTicker(f.checkInterval)` and internal/clock/clock.go:41 implements `RealClock.NewTicker` as `time.NewTicker(d)`, which panics on a non-positive duration. The lease check is fencing.go:395 `bloodravenDown := now.Sub(f.lastBloodravenOK) > f.leaseTimeout` and :403 `peersDown = latest.IsZero() || now.Sub(latest) > f.leaseTimeout`; evaluate() fences (super_read_only=ON) when both are down. lastBloodravenOK/lastPeerOK are refreshed only when the current cycle
- **Impact:** A leaseTimeout smaller than peerCheckInterval (any sub-5s value, e.g. "1s", or "0s") removes the fault-tolerance window: a single check cycle in which the operator /healthz AND every peer /peer/ping momentarily fail (a transient network blip, a brief operator rollout/restart, a GC pause) trips self-fencing, driving the active primary to super_read_only=ON — a write outage that persists until contact is re-established and the fence clears. This defeats the exact purpose of the lease window. peerCheckInterval="0s" or a negative value makes RealClock.NewTicker panic at sidecar startup (fencing.go:164) -> sidecar CrashLoopBackOff with the self-fencing safety net offline. Both values are accepted
- **Fix:** Add CEL/Minimum floors to both fields mirroring DiscoveryInterval, e.g. `+kubebuilder:validation:XValidation:rule="duration(self) >= duration('1s')"` on each, plus a spec-level cross-field rule requiring leaseTimeout to be a small multiple of peerCheckInterval; additionally clamp both to safe minimums in internal/sidecar/config.go as defense-in-depth so an older operator driving a new sidecar cannot pass a lethal value.

### H4. The canonical Failover doc's entire "Split-brain resolution" section documents a removed CRD field (`spec.splitBrainPolicy.preferSite`) whose replacement is `sitePriorities`, so its YAML example is apply-rejected and its documented log message contradicts the log-schema contract and the code.
- **File(s):** `docs/docs/failover.mdx:198`
- **Flagged by:** GPT-5.5, Opus 4.8, Qwen 3.7 Max (3 models, 5 raw findings); roles: hardening, stewardship
- **Evidence:** failover.mdx:198 shows the config example `splitBrainPolicy:` / ` preferSite: iad # "iad always wins ties"`, and lines 172/173/175/177/179/180/182/201/206/211/214/219 all describe a `preferSite` scalar. But the actual CRD type api/v1alpha1/types.go:229-243 defines `SplitBrainPolicySpec` with ONLY `SitePriorities []string` (json `sitePriorities`) — there is no `preferSite` field anywhere in api/ or internal/. git commit 9a34f43 ("Wishlist #10: N-site topology") removed `PreferSite` and added `SitePriorities`. Three other docs were updated correctly: crd-reference.mdx:138, multi-site.mdx:129 (`sitePriorities: [iad, pdx]`), and log-schema.mdx:145. failover.mdx:219 claims the log event is `split-brain auto-resolve: fencing non-preferred site per spec.splitBrainPolicy.preferSite`, but the code at internal/controller/topology.go:1048 actually emits `...per spec.splitBrainPolicy.sitePriorities`
- **Impact:** An operator configuring split-brain auto-resolution by following the canonical Failover page writes `splitBrainPolicy.preferSite: <site>`; strict server-side field validation (default for `kubectl apply`/`create` since k8s 1.25) rejects it as an unknown field, so the manifest fails to apply. Worse than a typo: the doc teaches the wrong mental model — a single "preferred site" tiebreaker instead of an ordered priority LIST — so even a user who guesses the field name will misunderstand the tie-break behavior and the primary-candidate-only constraint. This is a safety-adjacent feature usually configured during incident preparation, so the failure lands at the worst time. The doc also breaks the
- **Fix:** Rewrite failover.mdx "Split-brain resolution" to use `spec.splitBrainPolicy.sitePriorities` (a `[]string`); change the YAML at line 198 to e.g. `sitePriorities: [iad, pdx]`; correct line 201 to "entries must reference sites with `role: primary-candidate`"; and correct line 219's log `msg` to `split-brain auto-resolve: fencing non-preferred site per spec.splitBrainPolicy.sitePriorities` to match log-schema.mdx:145 and topology.go:1048.

### H5. A single slow/half-open WebSocket dashboard client freezes the operator's failover poll loop indefinitely (no write deadline, blocking write held under the hub mutex, called synchronously from Poll).
- **File(s):** `internal/platform/websocket.go:147`
- **Flagged by:** GLM 5.2, GPT-5.5, Opus 4.8 (3 models, 4 raw findings); roles: hardening, operability
- **Evidence:** In Hub.Broadcast the code takes `h.mu.Lock()` (websocket.go:143) then loops `for conn := range h.clients { conn.WriteMessage(websocket.TextMessage, data) }` (websocket.go:146-147). No `SetWriteDeadline` is ever set on the connection (grep for SetWriteDeadline across internal/ returns nothing), so gorilla/websocket's WriteMessage blocks on the socket write until the peer's TCP receive window drains — for a client that stopped reading (or a half-open connection after a network blip) this is effectively unbounded. This blocking write runs while `h.mu.Lock()` is held. Broadcast is invoked synchronously from the failover control loop: TopologyManager.Run (topology.go:521/531) calls Poll in a loop; Poll's last step is `tm.broadcastTopology(...)` (topology.go:775); broadcastTopology holds `tm.mu.RLock()` and calls `tm.hub.Broadcast(msg)` (topology.go:891). The per-client read goroutine (websock
- **Impact:** Once any WebSocket client stops reading (malicious, or a normal dashboard behind a flaky network producing a half-open TCP connection), Broadcast blocks, which blocks broadcastTopology (holding tm.mu.RLock) and therefore the entire Poll cycle. Because Run drives Poll in a serial loop, no further poll cycles run: MySQL health polling, cross-site evaluation, promotion, and old-primary recovery all stop. If a primary then fails while the loop is frozen, automated failover never fires — a full write outage with no auto-recovery, i.e. the exact failure this operator exists to prevent. The stall is immediate and persists until the stuck TCP connection is torn down (OS keepalive timeout: minutes to
- **Fix:** Never write to a WebSocket under the shared mutex without a deadline. Give each client a buffered send channel drained by its own writer goroutine; in that goroutine call `conn.SetWriteDeadline(time.Now().Add(writeWait))` before each WriteMessage and close/evict the client on error or a full buffer (drop slow consumers rather than block). Add ping/pong keepalive with a read deadline so half-open connections are reaped. Broadcast should only enqueue to per-client channels (non-blocking) and must

### H6. Plaintext MySQL passwords leaked into operator logs and Kubernetes Events via truncateSQL on ALTER USER statements
- **File(s):** `internal/controller/credentials.go:183`
- **Flagged by:** GLM 5.2, GPT-5.5, Qwen 3.7 Max (3 models, 3 raw findings); roles: hardening, operability
- **Evidence:** In `reconcileRole`, SQL statements containing the plaintext password are constructed at lines 169-172: `fmt.Sprintf("ALTER USER '%s'@'%%' IDENTIFIED BY '%s'", escapeSingleQuotes(username), escapeSingleQuotes(password))`. On execution failure, line 183 wraps the error with the truncated statement: `return fmt.Errorf("exec %q: %w", truncateSQL(stmt), err)`. The `truncateSQL` helper (line 252) truncates to 80 characters — but the `IDENTIFIED BY '` prefix sits at char ~36, so for a typical 3-char username the first 44 characters of the password remain visible in the error string (shorter passwords are fully exposed). This error propagates through `reconcileCredentials` to `reconciler.go:279` where it is logged via `logger.Error(err, ...)` and emitted as a Kubernetes Event at line 281 via `r.Recorder.Eventf(..., "failed to reconcile MySQL users: %v", err)`.
- **Impact:** Any MySQL password (app, operator, backup, monitor, readonly) is exposed in plaintext to anyone with access to operator logs or `kubectl describe`/`kubectl get events`. Log aggregation systems (Datadog, Splunk, ELK) typically have broader read access than Kubernetes Secrets, so the credential boundary is broken. This fires whenever `ALTER USER` fails — e.g., password validation policy rejection, transient MySQL unreachability mid-reconcile, or permission errors — all realistic production scenarios.
- **Fix:** Replace `truncateSQL(stmt)` with a static label that does not include the password: `return fmt.Errorf("exec %q: %w", role.name+" statement", err)`. Alternatively, use `fmt.Sprintf("ALTER USER '%s'@'%%' ...", username)` without the password in the error context. Never include raw SQL containing `IDENTIFIED BY` in error wrapping.

### H7. Verification sanityCheck example has `minRows` at wrong nesting level and missing `enabled: true`
- **File(s):** `docs/docs/production-install-examples.mdx:338`
- **Flagged by:** GPT-5.5, Qwen 3.7 Max (2 models, 5 raw findings); roles: hardening, operability, stewardship
- **Evidence:** Lines 336-340 show: ```yaml verification: schedule: "17 */6 * * *" sanityCheck: query: "SELECT COUNT(*) FROM information_schema.tables" minRows: 1 ``` Two problems: (1) `minRows` is placed directly under `sanityCheck`, but the CRD type `SanityCheckSpec` (api/v1alpha1/mysqlbackupverification_types.go:152-164) nests it under `expect`: the correct shape is `sanityCheck.expect.minRows`. The field `minRows` does not exist at the `sanityCheck` level and will be rejected by strict CRD validation. (2) The `verification` block omits `enabled: true`. `VerificationSpec.Enabled` defaults to `false` (kubebuilder:default=false at line 361), so the CronJob is never created — the entire verification config is silently inert.
- **Impact:** Users copying this example will either get a CRD validation error (minRows at wrong level) or, if validation is lenient, the sanityCheck threshold is silently dropped and verification never runs because `enabled` defaults to false. Both defeat the purpose of backup verification — a key production-readiness control.
- **Fix:** Change the example to: ```yaml verification: enabled: true schedule: "17 */6 * * *" sanityCheck: query: "SELECT COUNT(*) FROM information_schema.tables" expect: minRows: 1 ```

### H8. Operator aux-server RED metrics use the raw request path as the Prometheus "handler" label, giving unbounded cardinality that can OOM the operator and pollute Prometheus.
- **File(s):** `cmd/bloodraven/main.go:288`
- **Flagged by:** GLM 5.2, Opus 4.8 (2 models, 2 raw findings); roles: operability
- **Evidence:** In auxLoggingMiddleware (which wraps the ENTIRE aux mux, including the 404 path) main.go:288 does `metrics.HTTPRequestsTotal.WithLabelValues("aux", r.URL.Path, r.Method, metrics.StatusClass(sw.status)).Inc()` and :289 does the same for HTTPRequestDurationSeconds. metrics.go:281 declares that vector's labels as `{"server","handler","method","status"}`, so `r.URL.Path` becomes the "handler" label value verbatim. Every request to an unmatched path (e.g. GET /aaaa, /aab, …) 404s but is still recorded, minting a new series per distinct path (and the HistogramVec adds ~13 series each). This directly contradicts the project's own documented practice: internal/sidecar/server.go:49-50 says "The handler label is fixed rather than derived from URL to keep the cardinality bounded" and passes a constant handlerName — the operator aux server omits that fix. The endpoint is reachable: charts/bloodraven
- **Impact:** Any in-cluster workload (ClusterIP Service, no default NetworkPolicy) — or a misconfigured probe / security scanner — hitting the aux port with novel paths grows the operator's in-process metric registry without bound; series are never evicted, so operator RSS climbs until it is OOMKilled. Because the operator IS the failover control plane, sustained probing crash-loops the very process responsible for promotion/DNS steering. It simultaneously balloons the shared Prometheus TSDB with junk series.
- **Fix:** Label by a fixed route identifier, not the raw path — mirror internal/sidecar/server.go's instrument() by wrapping each registered handler with a constant handler name, and record unmatched requests under a single "other"/"notfound" bucket. Optionally also constrain the method label to a known allowlist (r.Method is likewise caller-controlled).

### H9. MysqlFailoverGroup CRD hard-codes a 4-minor-stale sidecar image default (0.1.6) that no longer matches the operator, silently breaking PITR binlog archival.
- **File(s):** `api/v1alpha1/types.go:44`
- **Flagged by:** Opus 4.8, Qwen 3.7 Max (2 models, 2 raw findings); roles: operability
- **Evidence:** types.go:44 sets `+kubebuilder:default="ghcr.io/shipstream/bloodraven-sidecar:0.1.6"`. The repo is at tag v0.5.2 (2026-06-16); v0.1.6 is dated 2026-04-09. release.yml:110-111 publishes the operator AND sidecar under the same `${VERSION}` (GITHUB_REF_NAME minus the leading 'v'), so the current sidecar is `:0.5.2` and `:0.1.6` is four minor releases stale. The controller consumes the value verbatim — reconciler.go:783 `sidecarImage := fg.Spec.SidecarImage` — with no operator-side default/override, and the Helm chart templates never set the sidecar image (only NOTES.txt offers guidance), so this CRD default is the sole source of truth. CRD defaults are persisted into the object at admission time, so a later `helm upgrade` of the operator does NOT move already-created CRs off 0.1.6. `git grep BLOODRAVEN_PITR_ENABLED v0.1.6 -- internal/sidecar` returns nothing (the archiver/PITR code did not
- **Impact:** Any MysqlFailoverGroup created without an explicit sidecarImage (including the shipped examples/minimal-failovergroup.yaml, which itself pins 0.1.6) runs a sidecar four minor releases behind the operator. With spec.backup.pitr.enabled=true, a 0.5.x operator injects BLOODRAVEN_PITR_ENABLED and the PITR env contract (reconciler.go:1007, pitr.go) into a 0.1.6 sidecar binary that has no archiver code — the env vars are ignored, no binlog files are uploaded, and the point-in-time recovery window the operator believes it maintains does not exist. The failure is silent until a restore is attempted (data-protection / RPO gap). Self-fencing, lease, and health-endpoint protocol evolution between 0.1.6
- **Fix:** Stop baking a released sidecar tag into the CRD schema. Default an empty spec.sidecarImage to the operator's own build version (inject via ldflags/env and resolve at reconcile time) so operator and sidecar are versioned from one source of truth; failing that, bump this default in lockstep on every release and add it to the release checklist. Also correct the examples/ manifests and docs that pin 0.1.6.

### H10. DR runbook exposes backup encryption passphrase in shell history and process listing
- **File(s):** `docs/docs/multi-cluster-dr.mdx:176`
- **Flagged by:** GPT-5.5, Qwen 3.7 Max (2 models, 2 raw findings); roles: hardening
- **Evidence:** multi-cluster-dr.mdx:175-176 instructs: `kubectl -n orders create secret generic orders-backup-passphrase --from-literal=passphrase='<value from step 1>'`. This places the backup encryption passphrase as a command-line argument, which is visible in: (1) shell history (`~/.bash_history`), (2) `/proc/<pid>/cmdline` while the command runs, (3) Kubernetes audit logs if the API server logs at `RequestResponse` level for secrets, and (4) any process-monitoring tool that captures command lines. The backup-encryption.mdx page (line 124) correctly uses `--from-file` with `shred -u` cleanup for the initial passphrase creation, but the DR runbook — the exact scenario where an engineer is under pressure during a cluster loss — uses the less secure `--from-literal` path.
- **Impact:** The backup encryption passphrase is the sole key protecting backup confidentiality. An attacker with access to shell history, process monitoring, or audit logs on the DR cluster (or the operator's workstation) can recover the passphrase and decrypt all backup artifacts in the shared S3 bucket, defeating the client-side encryption described in backup-encryption.mdx.
- **Fix:** Use `--from-file` with a temp file and `shred -u` cleanup, matching the pattern already shown in backup-encryption.mdx:119-124. Alternatively, pipe via stdin: `kubectl -n orders create secret generic orders-backup-passphrase --from-file=passphrase=/dev/stdin`.

### H11. Manual-recovery runbooks reference StatefulSet-style pod/PVC names that never exist because MySQL runs as a Deployment.
- **File(s):** `docs/docs/operations.mdx:213`
- **Flagged by:** GPT-5.5, Opus 4.8 (2 models, 2 raw findings); roles: operability, stewardship
- **Evidence:** operations.mdx:213 tells operators `kubectl delete pvc data-mysql-orders-iad-0`; :212 `kubectl delete pod mysql-orders-iad-0`; and the break-glass promotion/split-brain/divergent runbooks repeatedly use `kubectl exec mysql-orders-iad-0 -c mysql` (:69,:76,:83,:100,:103,:110,:117); monitoring.mdx:563 uses `kubectl logs mysql-orders-iad-0 -c sidecar`. But the operator provisions MySQL as a Deployment: internal/controller/reconciler.go reconcileDeployment names it `resourceName(fg,site)` = `mysql-<fg>-<site>` (line 560-562/706-709), and reconcilePVC names the PVC `resourceName(fg,site)+"-data"` = `mysql-orders-iad-data` (line 676-679/1069), corroborated by internal/playground/kube/pods.go MysqlPVCName (`mysql-<fg>-<site>-data`). Deployment-managed pods carry a ReplicaSet hash + random suffix (e.g. `mysql-orders-iad-6d4b8c9f7-x2k9p`), never `-0`. troubleshooting.mdx uses `<mysql-pod>` / `-l a
- **Impact:** In exactly the scenarios these break-glass runbooks target (operator unavailable, split-brain, divergent old-primary recovery, PVC loss), every hard-coded command fails with `NotFound`. The PVC-loss step `kubectl delete pvc data-mysql-orders-iad-0` deletes nothing — the real PVC `mysql-orders-iad-data` survives — so the operator cannot force storage recreation and recovery stalls mid-incident.
- **Fix:** Replace hard-coded pod names with a selector lookup (e.g. `POD=$(kubectl get pod -n orders -l app.kubernetes.io/instance=orders,shipstream.io/site=iad -o name)`), and correct the PVC name to `mysql-orders-iad-data` (pattern `mysql-<group>-<site>-data`).

### H12. TLS Certificate dnsNames use wrong Service names, breaking VERIFY_IDENTITY for internal connections
- **File(s):** `docs/docs/credentials-and-tls.mdx:75`
- **Flagged by:** GLM 5.2, GPT-5.5 (2 models, 2 raw findings); roles: stewardship
- **Evidence:** The copy-pasteable cert-manager Certificate example lists dnsNames `orders-primary.orders.svc.cluster.local` and `orders-replicas.orders.svc.cluster.local` (lines 75-76). The operator creates Services named `mysql-<group>-primary` and `mysql-<group>-replicas`, verified at internal/controller/reconciler.go:1266 (`fmt.Sprintf("mysql-%s-primary", fg.Name)`) and internal/controller/reconciler.go:1305 (`mysql-%s-replicas`), and internal/controller/credentials.go:63 (`mysql-%s-primary.%s.svc.cluster.local`). For a group named `orders` the real FQDNs are therefore `mysql-orders-primary.orders.svc.cluster.local`. The sibling example examples/tls-enabled-failovergroup.yaml:14-15 uses the correct `mysql-orders-primary`/`mysql-orders-replicas` names, confirming the credentials doc is the outlier.
- **Impact:** An operator who copies the credentials-and-tls.mdx Certificate (the canonical TLS setup page that getting-started.mdx and the CRD reference both link to) issues a cert that does not cover the actual primary/replicas Service FQDNs. Any client using `ssl-mode=VERIFY_IDENTITY` (which this same page recommends at line 140) against the internal Service hostname fails the TLS handshake. The error surfaces only after rollout, when pods/clients first connect over the verified TLS channel.
- **Fix:** Change the two dnsNames in docs/docs/credentials-and-tls.mdx to `mysql-orders-primary.orders.svc.cluster.local` and `mysql-orders-replicas.orders.svc.cluster.local` to match the `mysql-%s-primary` / `mysql-%s-replicas` naming the operator actually creates.

### H13. The chart leaves sidecars without a reachable auxiliary Service needed to clear startup self-fencing.
- **File(s):** `charts/bloodraven/values.yaml:138`
- **Flagged by:** GPT-5.5 (1 model, 2 raw findings); roles: hardening, stewardship
- **Evidence:** `values.yaml` sets `auxiliary.service.enabled: false`; `service-auxiliary.yaml` renders the Service only under `{{- if .Values.auxiliary.service.enabled }}` and names it `{{ include "bloodraven.fullname" . }}`; the operator injects the sidecar default as `bloodraven.<fg namespace>.svc.cluster.local:8082`; the sidecar safety net “clears the fence only if the operator confirms this is the active site” and otherwise logs “staying fenced”.
- **Impact:** A default Helm install has no stable `bloodraven` auxiliary Service. On MySQL pod restart the sidecar sets `super_read_only=ON` and cannot confirm the active site, so the active primary can remain read-only; release names other than `bloodraven` or failover groups outside the release namespace break the same path even if the Service is enabled. This undermines startup safety, topology-mismatch fencing, and PITR cutoff polling.
- **Fix:** Render an internal auxiliary Service by default, make its DNS name/namespace match what the operator injects, or pass the rendered Service FQDN into the operator and have it inject that exact address. Add a Helm validation/fail path when the auxiliary Service is disabled without an explicit reachable `sidecar.bloodravenAddress` strategy.

### H14. decrypt-download reuses upload-only storage parsing, breaking encrypted PVC restores and S3 verification paths.
- **File(s):** `cmd/bloodraven/encrypt_upload.go:431`
- **Flagged by:** GPT-5.5 (1 model, 2 raw findings); roles: operability, stewardship
- **Evidence:** `runDecryptDownload` passes `BLOODRAVEN_SOURCE_PREFIX` into `storageConfigFromEnv(storageType, sourcePrefix)`, but `storageConfigFromEnv` documents S3 `outputURL` as "already the relative key prefix" and its PVC branch requires `outputURL` to be under `BLOODRAVEN_PVC_MOUNT_PATH` via `filepath.Rel(mount, outputURL)`. The decrypt env contract says PVC uses a "PVC-relative path", and verification passes S3 `backup.Status.Location` directly as `BLOODRAVEN_SOURCE_PREFIX`.
- **Impact:** Encrypted PVC restore/verification init containers fail config resolution for valid relative prefixes, and encrypted S3 verification can list keys prefixed with `s3://bucket/...` instead of the object key prefix. Operators can have successful encrypted backups that are not restorable/verifiable.
- **Fix:** Split upload destination parsing from decrypt source parsing. For decrypt, accept PVC-relative prefixes as-is and parse `s3://bucket/key` into bucket/key or ensure callers pass storage-relative S3 keys consistently. Add end-to-end tests for `decrypt-download` config on encrypted S3 and PVC backups.

### H15. DNS is flipped before promotion succeeds, so a failed promotion can route production traffic to a non-writable site.
- **File(s):** `test/component/failover_test.go:47`
- **Flagged by:** GPT-5.5 (1 model, 2 raw findings); roles: hardening, stewardship
- **Evidence:** `test/component/failover_test.go:47-49` says `DNS should have flipped immediately at failover trigger (before promotion)`; `test/component/safety_invariants_test.go:55-64` calls that a safety invariant; `proposals/11-planned-failover.md:145` says DNS is flipped `immediately before the MySQL steps`; current code matches by calling `UpdateDNSRecord` before `failover.Execute`, while `README.md:177` promises DNS is deferred until promoted-site confirmation.
- **Impact:** If `FailoverController.Execute` fails after DNS is updated, clients are sent to a replica that may still be read-only or partially promoted. Planned failover is worse because the source has already been fenced, creating a write outage until manual DNS or MySQL recovery.
- **Fix:** Move DNS updates after successful promotion and confirmed `read_only=0`, or implement automatic DNS rollback on promotion failure. Update these tests/proposal to assert no DNS flip on failed or unconfirmed promotion.

### H16. Recloning can skip CLONE for a divergent recipient
- **File(s):** `internal/controller/topology.go:1894`
- **Flagged by:** GPT-5.5 (1 model, 1 raw finding); roles: hardening
- **Impact:** `canSkipClone` returns true when the recipient GTID is a superset of the donor. That means the recipient may contain errant transactions the active primary does not have. `runBootstrap` then skips destructive CLONE and only calls `SetupReplication`, which resets replica metadata but does not remove data or errant GTIDs. A manual reclone of a divergent old primary can therefore report success while preserving divergent rows.
- **Fix:** Only skip clone when the donor contains the recipient GTID set, or when the sets are exactly equal. For explicit reclone, consider never using the prior-clone shortcut.

### H17. Data race on failover-critical state — applyCrossSiteAction writes promotedSite/lastFailoverTarget/promotionGtidExecuted/promotedAt/lastFailover without holding tm.mu, while Status() reads them concurrently under RLock from the operator's /active-site and /status HTTP handlers.
- **File(s):** `internal/controller/topology.go:1115`
- **Flagged by:** Opus 4.8 (1 model, 1 raw finding); roles: hardening
- **Evidence:** applyCrossSiteAction is called from Poll (topology.go:652) with no lock held (it must be lock-free — it internally acquires tm.mu.RLock at topology.go:983). Inside it, after a promotion it writes shared fields with no lock: `tm.promotionGtidExecuted = promotionGtid` / `tm.promotedSite = candidate.name` / `tm.promotedAt = tm.clock.Now()` / `tm.lastFailover = tm.clock.Now()` / `tm.lastFailoverTarget = candidate.name` (topology.go:1115-1119), and reads tm.sites[i].state / tm.lastFailoverTarget unlocked at 1013-1024. Meanwhile Status() (topology.go:417-443) takes `tm.mu.RLock()` and reads the very same fields — tm.promotionGtidExecuted (442) and tm.promotedSite via effectiveActiveSiteLocked (451). Status() is invoked from a different goroutine (runner.go:696 mt.tm.Status(), served by the operator aux HTTP /active-site and /status handlers, which every sidecar's fencing monitor polls continuo
- **Impact:** A textbook Go data race on the exact state that drives promotion and self-fencing, triggered during the failover window itself (writes) against constant sidecar-driven /active-site reads. Because lastFailoverTarget/promotedSite are multi-word Go strings, a concurrent read can observe a torn value: an out-of-bounds slice header can panic the (non-HA) operator, and a garbled active-site string returned to a sidecar can cause a wrong fencing decision (self-fence a healthy primary → write outage, or fail to fence). `go test -race` on any concurrent Status()+Poll path would flag it.
- **Fix:** Guard the post-promotion writes with `tm.mu.Lock()` (as initiateRecovery already does) covering promotionGtidExecuted, promotedSite, promotedAt, lastFailover, and lastFailoverTarget; take a short RLock for the unlocked reads at 1013-1024 or snapshot them once at the top of applyCrossSiteAction. Run the controller suites with -race to confirm.

### H18. The production NetworkPolicy example selects a label Bloodraven does not apply, leaving unauthenticated sidecar HTTP unrestricted.
- **File(s):** `docs/docs/production-install-examples.mdx:125`
- **Flagged by:** GPT-5.5 (1 model, 1 raw finding); roles: hardening
- **Evidence:** Lines 125-127 select pods with `shipstream.io/managed-by: bloodraven`, and lines 138-140 use the same key for peer sidecar sources. The reconciler defines the managed-by label as `app.kubernetes.io/managed-by` at internal/controller/reconciler.go:49, and the page states at lines 85-87 that sidecar HTTP endpoints are internal and unauthenticated and must be restricted.
- **Impact:** Clusters copying this production example get a sidecar policy that selects zero default Bloodraven MySQL pods, so the unauthenticated sidecar `:8080` endpoints remain reachable according to namespace defaults.
- **Fix:** Change selectors to the actual labels, for example `app.kubernetes.io/managed-by: bloodraven` plus group/site labels, and include explicit ingress rules for operator and peer sidecar access to `:8080` and intended MySQL access to `:3306`.

### H19. Failed planned-failover rollback can keep emergency failover disabled
- **File(s):** `internal/controller/planned_failover_reconciler.go:760`
- **Flagged by:** GPT-5.5 (1 model, 1 raw finding); roles: hardening
- **Impact:** If lag wait times out and source unfencing fails, rollback returns a requeue before stamping the planned failover as failed or clearing the topology guard. The status remains in an in-flight phase, the runner keeps `plannedFailoverActive=true`, and automatic cross-site actions are suppressed. If the source crashed during the planned switchover, the cluster can remain stuck without emergency promotion until manual intervention.
- **Fix:** Treat unfence as best-effort as the comment says: stamp terminal failure and release the topology guard even if the source cannot be unfenced, while surfacing that the source may remain fenced/unreachable.

### H20. Credentials-mode operator connections omit TLS while MySQL requires it
- **File(s):** `internal/controller/runner.go:1327`
- **Flagged by:** GPT-5.5 (1 model, 1 raw finding); roles: hardening
- **Impact:** TLS-enabled groups generate MySQL config with `require-secure-transport=ON`, but credentials-mode DSNs built by `buildSiteDSNFromCreds` do not set any MySQL driver TLS option. `openMySQL` used by credential reconciliation also omits TLS. In a credentials-mode TLS deployment, operator probes, failover actions, bootstrap/reclone, and credential reconciliation can fail to connect to every MySQL site.
- **Fix:** Build/register a TLS config from `spec.tls.secretName` and apply it to every credentials-mode MySQL DSN, including `openMySQL`; add a regression test for `UsesCredentials() && spec.tls != nil`.

### H21. Encrypted PITR sidecar mounts passphrase Secret unreadable by its non-root user
- **File(s):** `internal/controller/pitr.go:219`
- **Flagged by:** GPT-5.5 (1 model, 1 raw finding); roles: stewardship
- **Evidence:** The PITR sidecar is forced to run as UID/GID 999 when PITR is enabled, and the MySQL Deployment pod security context is nil by default. The S3 credentials Secret path was explicitly changed to `0444` for non-root readability, but the encrypted PITR passphrase Secret still uses `0400`. The sidecar reads that passphrase file during archive-store initialization.
- **Impact:** Enabling encryption for PITR can make the sidecar unable to read its passphrase, preventing encrypted binlog archival from starting.
- **Fix:** Use a sidecar-readable Secret mode for `pitr-passphrase` or set an appropriate pod `fsGroup`, add a test mirroring the AWS creds readability assertion, and bump `pitrPodRenderVersion`.

## Medium (83)
- **[4×]** `api/v1alpha1/credentials_helpers.go:35` — AllReferencedSecretNames omits the Dragonfly auth/snapshot credential secrets, so rotating them is silently ignored and later breaks operator↔Dragonfl
- **[4×]** `playground/manifests/backup-profile.yaml:44` — Playground backup/verify workflow targets a MinIO S3 endpoint and secret that the playground never provisions (RustFS replaced MinIO but the manual ma
- **[4×]** `charts/bloodraven/values.yaml:60` — Default nodeSelector pins the operator to control-plane nodes, leaving it Pending on managed clusters (EKS/GKE/AKS), and this is undocumented.
- **[4×]** `.github/workflows/ci.yml:75` — setup-envtest installed via @latest, allowing version skew with the pinned client-go v0.36.2.
- **[4×]** `.golangci.yml:29` — .golangci.yml globally suppresses SA1019 (deprecation warnings), directly contradicting AGENTS.md which states SA1019 is enforced and blocks CI
- **[4×]** `docs/docs/install-production.mdx:45` — Manual CRD install and post-install verification omit the MysqlStandbyCluster CRD
- **[4×]** `.github/workflows/scan.yml:4` — Container images are vulnerability-scanned only on pull requests; there is no scheduled scan and no scan gate in the release pipeline, so signed relea
- **[4×]** `cmd/bloodraven/main.go:326` — pitrCutoffCache never evicts expired entries, allowing unbounded memory growth on an unauthenticated pod-network endpoint
- **[4×]** `.github/workflows/release.yml:30` — Release ci-gate claims to mirror the PR gate but omits the test-envtest job, so a tag can ship with broken API-server-facing behavior.
- **[4×]** `.github/workflows/ci.yml:100` — CI guards CRD chart drift but has no equivalent guard for the hand-maintained Helm RBAC (clusterrole.yaml), so RBAC drift ships silently.
- **[4×]** `docs/docs/monitoring.mdx:130` — Documented "no primary" alert is dead code that can never fire, and duplicates the correct alert defined 50 lines below it
- **[4×]** `internal/controller/topology.go:1114` — The automatic/emergency failover path emits no failure counter and no duration histogram; bloodraven_failovers_total is success-only, unlike the fully
- **[3×]** `test/envtest/backup_verification_test.go:244` — Backup-verification datadir is an unbounded emptyDir, contradicting proposal 08's "no impact on the live cluster" isolation guarantee and its document
- **[3×]** `.github/workflows/release.yml:130` — The privileged release workflow (contents/packages/id-token write + cosign keyless signing) pins third-party Actions by mutable major-version tags ins
- **[3×]** `.github/workflows/README.md:124` — The workflows README claims releases block on the "release-profile" E2E gate, but the actual pre-publish gate runs only the smoke profile; the release
- **[3×]** `internal/controller/topology.go:1103` — Emergency failover flips the DNSEndpoint before promotion, contradicting the documented failover sequence (docs place DNS after promotion + write-conf
- **[3×]** `charts/bloodraven/templates/NOTES.txt:56` — NOTES.txt example MFG CR omits required `taintNodeSelector` field — `kubectl apply` will be rejected by CRD validation
- **[3×]** `docs/docs/multi-cluster-dr.mdx:366` — Production DR recovery runbook manifests use the legacy single-DSN `spec.secretName` mode that the security model explicitly forbids in production.
- **[3×]** `examples/prometheusrule.yaml:1` — Shipped example PrometheusRule implements only 2 of the 9 documented "minimum alert set" signals, including none of the data-loss/writability alerts
- **[3×]** `charts/bloodraven/argocd/argocd-cm-patch.yaml:16` — Shipped ArgoCD health customizations cover only MysqlFailoverGroup and MysqlBackup, so a Failed MysqlBackupVerification shows Healthy in the GitOps UI
- **[3×]** `config/rbac/role.yaml:7` — ClusterRole grants cluster-wide CRUD on all secrets — over-broad for operator's actual needs
- **[2×]** `api/v1alpha1/types.go:38` — BackupProfile.Name uniqueness within spec.backup.profiles[] is not enforced at the CRD level
- **[2×]** `.github/workflows/ci.yml:51` — CI and `make test` never execute 54 test functions across cmd/ and api/ packages, including backup-encryption, PITR-download, credential-resolution, a
- **[2×]** `.github/workflows/release.yml:8` — Release workflow grants contents:write + packages:write + id-token:write at workflow scope, so the ci-gate job (which runs `npm ci` and `go install @l
- **[2×]** `docs/docs/monitoring.mdx:554` — Operator/sidecar log jq filters reference msg strings that never exist in the log stream
- **[2×]** `test/component/safety_invariants_test.go:243` — No error-injection test for old-primary fencing failure during failover
- **[2×]** `api/v1alpha1/mysqlbackupverification_types.go:133` — PointInTimeVerificationSpec missing CEL validation for conditional Timestamp requirement
- **[2×]** `playground/verify-backup.sh:1` — verify-backup.sh is the only playground script that does not source _guard.sh, allowing accidental mutation of production clusters
- **[2×]** `playground/chaos.sh:112` — `chaos.sh network-partition` reports success but is a silent no-op on the default CNI of kind and minikube (two of the three documented cluster tools)
- **[2×]** `charts/bloodraven/dashboards/README.md:3` — Dashboards README falsely claims coverage of "every metric" when 12 metrics (all Dragonfly, all backup-encryption, all restore) are uncovered
- **[2×]** `charts/bloodraven/values.yaml:57` — Default operator memory limit of 256Mi combines with an unscoped cluster-wide informer cache and can OOMKill the operator on large multi-tenant cluste
- **[2×]** `cmd/bloodraven/main.go:198` — BLOODRAVEN_OPERATOR_IMAGE silently defaults to non-existent "bloodraven:latest" with no startup warning, breaking all scheduled backups/verifications/
- **[2×]** `cmd/bloodraven/main.go:402` — The documented /pitr-cutoff endpoint that drives irreversible binlog pruning has zero test coverage.
- **[2×]** `api/v1alpha1/types.go:41` — MySQL / sidecar / backup image fields accept floating tags like ":latest" while Dragonfly rejects them
- **[2×]** `docs/docs/monitoring.mdx:46` — 16 Prometheus metrics exist in code but are absent from the monitoring docs "Available metrics" table
- **[2×]** `internal/metrics/metrics.go:6` — Core MySQL-topology metrics are labelled with only `site`, so they conflate across multiple MysqlFailoverGroups that share site names
- **[2×]** `api/v1alpha1/site_helpers.go:44` — Bootstrap/promotion decision helpers (DefaultSeedSite, EffectiveRole, IsPromotable) have zero unit-test coverage and no behavioral coverage in envtest
- **[1×]** `api/v1alpha1/types.go:287` — Site names are not constrained to Kubernetes resource-safe names even though they are concatenated directly into child object names.
- **[1×]** `charts/bloodraven/templates/_helpers.tpl:55` — serviceAccount.create=false without a name binds the powerful operator ClusterRole to the namespace default ServiceAccount.
- **[1×]** `cmd/bloodraven/main.go:38` — Unknown `bloodraven` subcommands fall through and start the operator manager.
- **[1×]** `.github/workflows/README.md:24` — Trivy high/critical CVE failures are outside the documented required merge and release gates.
- **[1×]** `docs/docs/production-install-examples.mdx:173` — The production NoWritable alert groups by namespace/group labels the documented metric does not expose.
- **[1×]** `docs/docs/operations.mdx:200` — PVC loss runbook cold-reclone example uses bare annotation form that the operator rejects
- **[1×]** `docs/docs/backup-verification.mdx:55` — Backup verification docs incorrectly say encrypted backups cannot be verified.
- **[1×]** `internal/controller/backup_reconciler.go:344` — Backup and verification freshness metrics are not rehydrated after operator restart
- **[1×]** `cmd/bloodraven/main.go:226` — The aux logging middleware breaks `/ws/status` WebSocket upgrades.
- **[1×]** `cmd/bloodraven/main.go:427` — `/pitr-cutoff` ignores successful backups that lack the expected labels, so PITR retention can silently stop pruning.
- **[1×]** `api/v1alpha1/types.go:401` — serviceTemplate.type can expose the unauthenticated sidecar HTTP API through LoadBalancer or NodePort Services.
- **[1×]** `api/v1alpha1/backup_types.go:460` — PVC backup subPath is not confined, so a successful backup can be written outside the backup PVC and disappear.
- **[1×]** `.github/workflows/release.yml:227` — Released Helm charts can deploy a stale sidecar image by default.
- **[1×]** `cmd/bloodraven/encrypt_upload.go:556` — Decrypted dumps are written `0600` even though decrypt init containers may run as a different UID than the mysqlsh container.
- **[1×]** `cmd/bloodraven/encrypt_upload.go:473` — `decrypt-download` reports success when it decrypts zero backup files.
- **[1×]** `cmd/bloodraven/encrypt_upload.go:396` — Backup encryption docs claim plaintext upgrade passthrough, but decrypt paths default to strict rejection.
- **[1×]** `docs/docs/monitoring.mdx:342` — The event reference documents obsolete `BackupPITRNotImplemented` events even though PITR is implemented and invalid config emits `BackupPITRInvalid`.
- **[1×]** `charts/bloodraven/templates/NOTES.txt:41` — Quickstart secret guidance names the wrong keys for the legacy `secretName` path and never mentions the required `dsn` key, so the CR never reconciles
- **[1×]** `docs/docs/multi-cluster-dr.mdx:110` — The DR read-only S3 IAM policy allows bucket-wide listing instead of prefix-scoped listing.
- **[1×]** `docs/docs/backup-verification.mdx:203` — Backup verification docs advertise PVC isolation and sizing that the verifier does not provide.
- **[1×]** `docs/docs/operations.mdx:95` — Split-brain recovery instructions contradict actual operator behavior, risking double-action during incidents
- **[1×]** `test/component/safety_invariants_test.go:243` — Fence-before-promote test cannot detect candidate being promoted before old primary is fenced (false-confidence safety net for the #1 split-brain inva
- **[1×]** `api/v1alpha1/mysqlbackupverification_types.go:67` — Backup verification storage fields document and expose a PVC that the controller no longer creates or honors.
- **[1×]** `internal/controller/reconciler.go:458` — Deletion finalizer removes the CR even when node-taint cleanup fails.
- **[1×]** `test/component/failover_test.go:204` — Topology and failover metrics are scoped only by site name, causing cross-group collisions in multi-cluster installs.
- **[1×]** `charts/bloodraven/templates/deployment.yaml:80` — Scheduled backup and verification trigger pods reuse or misresolve the operator ServiceAccount.
- **[1×]** `charts/bloodraven/templates/deployment.yaml:10` — The Deployment uses Recreate strategy, so upgrades remove all operator pods despite leader election support.
- **[1×]** `config/crd/bases/shipstream.io_mysqlfailovergroups.yaml:6055` — Site names are not constrained to the DNS/label-safe format the operator assumes.
- **[1×]** `charts/bloodraven/templates/clusterrole.yaml:39` — The chart installs cluster-wide Secret read/list/watch authority with no namespace-scoped mode.
- **[1×]** `charts/bloodraven/dashboards/overview.json:54` — Shipped dashboards cannot distinguish multiple failover groups or namespaces for core health signals.
- **[1×]** `cmd/kubectl-bloodraven/promote.go:194` — `promote --wait` can report a stale prior planned failover as the result of the new promotion.
- **[1×]** `internal/sidecar/config.go:187` — Non-positive duration settings are accepted and can self-fence primaries, crash sidecars, or hot-loop polling.
- **[1×]** `internal/playground/scenarios/s31_pitr_verification_rustfs.go:34` — PITR backup verification is known-broken and excluded from release coverage
- **[1×]** `api/v1alpha1/dragonfly_types.go:56` — Dragonfly Args "reserved flags" contract is unenforced at the CRD and the controller filter omits --maxmemory/--proactor_threads, so spec.args can def
- **[1×]** `internal/sidecar/binlog_archiver.go:355` — PITR binlog retention sweep has zero test coverage despite being the path that deletes objects from production backup storage
- **[1×]** `cmd/bloodraven/pitr_download.go:249` — PITR restore init container uses a bucket-derived `site` name unvalidated in path construction, defeating the AUDIT H1 traversal defense for the site
- **[1×]** `cmd/bloodraven/pitr_download.go:231` — PITR restore binlog-selection decision logic (downloadSiteBinlogs) has no test coverage
- **[1×]** `cmd/bloodraven/pitr_download.go:277` — PITR manifest `remotePath` validation can be bypassed with `../` segments on PVC storage.
- **[1×]** `internal/sidecar/fencing.go:436` — Sidecar self-fencing — the safety-critical event where a primary sets super_read_only=ON on quorum loss — emits no metric and the sidecar exposes no /
- **[1×]** `cmd/bloodraven/trigger.go:77` — Scheduled backup/verification triggers use GenerateName with no idempotency, so an at-least-once Job retry creates duplicate CRs and duplicate concurr
- **[1×]** `cmd/playground-chaos/reset.go:347` — clearMFGStatus misses five status fields and targets one phantom field, leaving stale state after reset
- **[1×]** `internal/sidecar/mysql.go:42` — Sidecar fencing-monitor MySQL calls have no driver-level I/O deadline, so a server-level MySQL stall wedges the self-fencing loop indefinitely
- **[1×]** `docs/docs/known-limitations.mdx:96` — Known-limitations page falsely claims no kubectl plugin exists, steering operators away from the safer CLI path
- **[1×]** `docs/docs/examples.mdx:10` — The examples page labels backup manifests complete, but the S3/PVC examples contain invalid partial MysqlFailoverGroup specs.
- **[1×]** `docs/docs/alert-runbook-map.mdx:24` — Two alerts referenced in alert-runbook-map have no PromQL expression defined anywhere in the docs
- **[1×]** `internal/controller/backup_improvements_test.go:669` — The only tests for backup terminal-metric emission assert nothing but "does not panic", despite names promising gauge behavior, and the parallel resto

## Low (85)
- **[4×]** `README.md:93` — README lists stale dependency versions (controller-runtime v0.23.3 / k8s.io/api v0.35.3) that are a full minor behind go.mod.
- **[4×]** `.github/workflows/README.md:144` — Dependabot docs promise weekly security updates but config is quarterly (~90-day lag).
- **[4×]** `playground/manifests/rustfs.yaml:70` — The RustFS backup store — a hard dependency of the E2E backup scenarios and CI — uses a floating `:latest` image tag, breaking build reproducibility a
- **[4×]** `test/component/partition_test.go:1` — Sidecar partition/self-fencing tests are build-tag-gated and never compiled or executed by CI or the documented pre-PR gate
- **[4×]** `Dockerfile:6` — No `.dockerignore` exists, so `COPY . .` sends the entire ~1.1G build context (including 135M `.git` and 593M `docs/`) into the builder layer on every
- **[4×]** `playground/dns-webhook/main.go:200` — dns-webhook HTTP servers have no Read/Write/Idle timeouts (slowloris / FD exhaustion)
- **[3×]** `charts/bloodraven/values.yaml:148` — auxiliary.service.enabled only hides the Service; the unauthenticated :8082 status/WebSocket server is always on, and the chart ships no NetworkPolicy
- **[3×]** `cmd/bloodraven/encrypt_upload.go:305` — Operational-stream log records in the backup/restore subcommands use snake_case field keys, violating the documented camelCase log convention.
- **[3×]** `Dockerfile:1` — Container base images are referenced by floating tags with no digest pin, making image builds non-reproducible and allowing silent base drift.
- **[3×]** `proposals/08-backup-verification.md:159` — Proposal 08 describes a dedicated-PVC + headless-Service + NetworkPolicy isolation model that contradicts the shipped emptyDir-in-Job implementation
- **[3×]** `docs/docs/crd-reference.mdx:344` — Canonical "Full example" and Getting-Started manifests set `sidecarImage: ...:latest`, contradicting both the field's own documented pinned default an
- **[3×]** `api/v1alpha1/types.go:451` — MysqlFailoverGroupStatus has no top-level observedGeneration, breaking standard reconcile-health tooling
- **[2×]** `charts/bloodraven/templates/NOTES.txt:71` — Post-install NOTES.txt step list is mis-numbered (two "3." entries)
- **[2×]** `charts/bloodraven/templates/tests/test-connection.yaml:1` — Helm test pod is not gated on metrics.service.enabled, so `helm test` fails whenever a user disables the metrics Service
- **[2×]** `cmd/bloodraven/encrypt_upload.go:302` — Per-file encrypt-upload log lines drop the backup identity the surrounding code explicitly binds for correlation
- **[2×]** `charts/bloodraven/crds/shipstream.io_mysqlbackups.yaml:1` — CRDs live in the install-only crds/ directory, so helm upgrade silently never applies CRD schema changes
- **[2×]** `internal/controller/credentials.go:247` — The SQL-injection escaping helper escapeSingleQuotes is duplicated byte-for-byte in two packages and has zero unit tests in either, and the whole cred
- **[2×]** `internal/sidecar/server.go:125` — Sidecar /status endpoint leaks internal MySQL error messages to unauthenticated callers
- **[2×]** `test/envtest/reconciler_test.go:160` — No envtest or component test coverage of planned failover reconcile state machine
- **[2×]** `test/envtest/reconciler_test.go:65` — ensureNamespace and ensureSecret swallow all errors instead of only AlreadyExists, masking real API-server failures
- **[2×]** `playground/counter-app/main.go:74` — connectLoop leaks a *sql.DB pool on every MySQL ping failure
- **[2×]** `AGENTS.md:12` — AGENTS.md describes `make test` as running `go test ./...` across e2e-style packages, but it only runs unit + component.
- **[2×]** `internal/sidecar/binlog_archiver.go:410` — snake_case log key "manifest_pruned" violates AGENTS.md camelCase log-key convention
- **[2×]** `playground/dashboard/main.go:215` — Dashboard proxies verbatim Kubernetes API error bodies to HTTP clients
- **[2×]** `playground/manifests/mysql-secret.yaml:8` — Plaintext demo credentials (root/replicator passwords) are committed and a root DSN is baked into the counter-app binary's default, with MYSQL_ROOT_HO
- **[2×]** `playground/dashboard/index.html:266` — The dashboard renders operator-status and DNS-record fields into the DOM via innerHTML with no escaping, a stored-DOM-XSS sink on an unauthenticated,
- **[2×]** `playground/manifests/dashboard-rbac.yaml:10` — Dashboard RBAC uses ClusterRole/ClusterRoleBinding when namespace-scoped Role/RoleBinding suffices
- **[1×]** `api/v1alpha1/dragonfly_types.go:14` — Dragonfly image validation claims to require a pinned image but still admits implicit `latest`.
- **[1×]** `api/v1alpha1/backup_types.go:731` — RestoreInPlaceSpec.Confirm lacks RFC 3339 format validation in CRD schema
- **[1×]** `AGENTS.md:36` — Pre-PR gate requires `make vet` but CI has no standalone vet job; coverage depends on golangci-lint's govet linter equivalence
- **[1×]** `test/envtest/backup_verification_test.go:108` — PITR timestamp validation is documented and assumed by tests but not enforced by the CRD.
- **[1×]** `docs/docs/backup-verification.mdx:110` — The manual backup-verification example creates the CR in the operator namespace instead of the failover group's namespace.
- **[1×]** `internal/sidecar/server.go:243` — http.DefaultClient used for operator queries — no transport-level timeout, no connection pooling control
- **[1×]** `playground/rebuild.sh:148` — rebuild.sh and setup.sh use different k3d image import modes, creating inconsistent stale-image behavior
- **[1×]** `test/component/bootstrap_integration_test.go:153` — Negative assertion uses time.Sleep to detect erroneously-started goroutine
- **[1×]** `proposals/11-planned-failover.md:83` — Design-of-record proposals 08 and 11 have drifted from the shipped implementation of features that are already in production, misleading operators who
- **[1×]** `playground/counter-app/Dockerfile:1` — Playground sub-module Dockerfiles use Go 1.25 builder while main module uses Go 1.26
- **[1×]** `charts/bloodraven/values.yaml:1` — No values.schema.json, so value typos are silently ignored (e.g. a misspelled ServiceMonitor flag silently disables scraping)
- **[1×]** `api/v1alpha1/types.go:498` — `status.pitr` API documentation says the controller does not populate a field that current code does populate.
- **[1×]** `api/v1alpha1/backup_types.go:501` — BackupSchedule.ProfileName lacks the DNS-1035 Pattern enforced on BackupProfile.Name
- **[1×]** `api/v1alpha1/backup_types.go:21` — The default backup image is hard-coded in two places (the DefaultBackupImage const and the kubebuilder default marker) that must be hand-synced, and t
- **[1×]** `api/v1alpha1/backup_types.go:876` — RestoreStatus and RestoreInPlaceStatus duplicate 11 identical fields, creating maintenance burden
- **[1×]** `.github/workflows/ci.yml:95` — The generated-file drift gate uses `git diff --quiet`, which does not detect newly generated untracked files.
- **[1×]** `cmd/bloodraven/encrypt_upload.go:162` — The encryption manifest is written unencrypted beside the ciphertext and carries plaintext SHA-256 digests of every dump chunk.
- **[1×]** `internal/controller/topology.go:1328` — FenceSite transactional rollback path (unfence on GTID-read failure) has no test coverage
- **[1×]** `docs/docs/production-install-examples.mdx:164` — PrometheusRule examples violate the documented required alert annotation contract.
- **[1×]** `internal/controller/credentials.go:154` — Credentials packed via null-byte separator into a string field named secretName, fragile and confusing for security-critical code
- **[1×]** `test/component/safety_invariants_test.go:84` — TestSafetyInvariant_NeverAutoUnfence never exercises the "connectivity returns after fencing" path it claims to guard
- **[1×]** `test/component/safety_invariants_test.go:346` — TestSafetyInvariant_PrimaryServiceSelectorIsCorrect does not actually verify service selectors
- **[1×]** `examples/prometheusrule.yaml:11` — PrometheusRule example alert for operator-down uses overly broad job matcher and 5-minute delay
- **[1×]** `internal/controller/reconciler.go:920` — PreStop hook passes MySQL password via command-line argument in credentials mode
- **[1×]** `playground/setup.sh:185` — Kind image loading ignores the active kube context and imports into the first Kind cluster.
- **[1×]** `internal/controller/planned_failover_reconciler.go:650` — MySQL promotion failure during planned failover Promoting phase has no unit test
- **[1×]** `playground/manifests/rustfs.yaml:68` — Setup reports RustFS ready based only on Deployment rollout with no RustFS readiness probe.
- **[1×]** `proposals/08-backup-verification.md:3` — Both design proposals link their wishlist reference to a repo-root WISHLIST.md that does not exist (dangling internal links).
- **[1×]** `AGENTS.md:101` — AGENTS.md still describes the project as one custom resource and two binaries.
- **[1×]** `charts/bloodraven/templates/tests/test-connection.yaml:11` — Helm test pod runs without any security context, violating the chart's own hardened posture
- **[1×]** `charts/bloodraven/argocd/argocd-cm-patch.yaml:17` — ArgoCD Lua health-check scripts duplicated inline and as standalone files with no sync mechanism
- **[1×]** `internal/sidecar/binlog_archiver.go:294` — PITR archiver catch-up rewrites the full manifest once per missing binlog.
- **[1×]** `playground/chaos.sh:41` — chaos.sh `network-partition` help text describes a debug-pod/exec mechanism, but the command actually applies a persistent deny-all NetworkPolicy.
- **[1×]** `api/v1alpha1/site_helpers.go:27` — Dead code: exported spec methods PrimaryCandidates() and SiteNames() are unused anywhere in the repository.
- **[1×]** `internal/sidecar/fencing.go:278` — Internal HTTP clients decode JSON response bodies without io.LimitReader, allowing memory exhaustion from a compromised peer or operator endpoint
- **[1×]** `internal/sidecar/fencing.go:208` — checkBloodraven does not drain response body — prevents HTTP connection reuse
- **[1×]** `playground/rebuild.sh:93` — `rebuild.sh sidecar` can leave three-site MySQL pods on stale sidecar code while reporting success.
- **[1×]** `cmd/bloodraven/trigger.go:67` — Five committed files in cmd/ are not gofmt-clean, and the configured lint gate contains no formatter so the drift is unenforced.
- **[1×]** `playground/dns-webhook/main.go:144` — dns-webhook decodes unbounded JSON request bodies (OOM under large payload)
- **[1×]** `playground/manifests/dashboard.yaml:24` — Dashboard and external-dns containers have no readiness probe, and setup.sh gates their readiness with `|| true`, so setup reports success while they
- **[1×]** `charts/bloodraven/crds/shipstream.io_mysqlbackupverifications.yaml:130` — MysqlBackupVerification admits pointInTime.mode=timestamp without the required timestamp.
- **[1×]** `charts/bloodraven/crds/shipstream.io_mysqlfailovergroups.yaml:5972` — Shipped CRDs hard-code the sidecar default image tag instead of keeping it aligned with the chart/operator version.
- **[1×]** `internal/dragonfly/resp.go:98` — Dragonfly RESP parser readLine has no size limit, allowing unbounded memory allocation from a malicious or buggy server
- **[1×]** `internal/controller/verify_script.sh:178` — PITR verification replay-lag metric cannot be populated from current sentinel
- **[1×]** `internal/mysql/checker.go:185` — Dead Promote() method on Checker interface is an incomplete promotion sequence that could mislead future maintainers
- **[1×]** `charts/bloodraven/Chart.yaml:6` — Chart appVersion (0.1.0) has drifted behind the sidecar image the code/examples pin (0.1.6), producing operator/sidecar version skew from chart defaul
- **[1×]** `cmd/bloodraven/trigger_verification.go:100` — Scheduled-verification VerificationSpec copy logic is duplicated and its CronJob copy is entirely untested
- **[1×]** `cmd/kubectl-bloodraven/backup.go:181` — The kubectl-plugin --wait day-2 loops (backup, promote, verify) have no test coverage despite non-trivial retry/timeout logic.
- **[1×]** `cmd/kubectl-bloodraven/helpers.go:155` — Divergent humanBytes implementations across operator and kubectl plugin produce different formatting
- **[1×]** `docs/docs/intro.mdx:76` — Landing page claims "a single CRD" while the CRD reference documents four
- **[1×]** `internal/sidecar/binlog_storage.go:253` — PVC store GetFile creates downloaded files with world-readable permissions (0644)
- **[1×]** `internal/controller/safety_invariants_test.go:19` — The crown-jewel safety-invariant tests cite TESTING_2.0.md as the source of "release-blocking" / "non-negotiable" requirements, but that file has neve
- **[1×]** `internal/testutil/fakes.go:3` — The testutil package documents itself as the mandatory consolidated fake ("All test packages should import fakes from here"), but most controller test
- **[1×]** `internal/controller/updater_test.go:387` — Flaky test uses time.Sleep(2ms) to control mock state transition
- **[1×]** `internal/metrics/metrics_test.go:9` — Metrics registration test covers only 3 of 34 registered metrics
- **[1×]** `playground/chaos-scenarios.md:538` — Chaos scenario docs give stale taint-clearing instructions that contradict the reset-mysql.sh behavior documented in the same file and in AGENTS.md
- **[1×]** `test/envtest/suite_test.go:39` — envtest TestMain silently discards AddToScheme errors, masking scheme registration failures
- **[1×]** `test/component/bootstrap_test.go:12` — Dead code: testBootstrapLogger and testLogHelper defined but never called

## Weak-only / low-confidence surviving (50)
- **[1×]** `cmd/bloodraven/main.go:477` — A slow `/ws/status` client can block topology broadcasts while the topology lock is held.
- **[1×]** `cmd/bloodraven/encrypt_upload.go:737` — Encrypted backup metadata read errors are swallowed, allowing successful backups with missing PITR coordinates.
- **[1×]** `docs/docs/operations.mdx:118` — Split-brain manual recovery procedure omits divergence check before re-establishing replication
- **[1×]** `internal/controller/reconciler.go:961` — Legacy mode exposes MySQL credentials (including root password and full DSN) in container environment variables
- **[1×]** `internal/controller/reconciler.go:278` — Credential reconciliation swallows errors without requeue — broken users persist until Secret edit
- **[1×]** `config/crd/bases/shipstream.io_mysqlfailovergroups.yaml:692` — Backup group/profile identifiers accepted by the API can be invalid Kubernetes label values, causing scheduled backup creation to fail.
- **[1×]** `internal/controller/backup_verification_job.go:203` — Backup verification restore storage is unbounded emptyDir despite sizing logic
- **[1×]** `test/envtest/reconciler_test.go:421` — Missing core MySQL credentials only requeue and emit an Event, with no durable status condition for operators.
- **[1×]** `proposals/08-backup-verification.md:11` — Backup verification freshness metrics are not rehydrated after operator restart, so stale-backup alerts can lose their source series.
- **[1×]** `internal/controller/failover.go:24` — Failover fences old primary best-effort but proceeds to promote even when fencing fails, risking split-brain
- **[1×]** `internal/controller/backup_script.py:111` — Data-plane S3 scripts silently fall back to ambient AWS credentials when a configured credentials Secret is incomplete.
- **[1×]** `test/component/normal_test.go:73` — The tested topology readiness signal is not wired to the production readiness endpoints.
- **[1×]** `cmd/bloodraven/main.go:222` — Auxiliary HTTP server exposes topology state, GTID sets, and WebSocket without authentication or rate limiting
- **[1×]** `cmd/bloodraven/main.go:477` — WebSocket handler has no read deadline, message size limit, or ping/pong keepalive — connection-hold DoS
- **[1×]** `cmd/bloodraven/main.go:215` — Readyz probe uses healthz.Ping, reporting ready before informer caches sync
- **[1×]** `cmd/bloodraven/main.go:344` — Auxiliary HTTP server exposes always-OK /healthz and /readyz on :8082 that do not reflect operator state
- **[1×]** `api/v1alpha1/types.go:396` — spec.sidecar.bloodravenAddress lets a CR replace the unauthenticated authority used to clear MySQL fencing.
- **[1×]** `api/v1alpha1/backup_types.go:722` — RestoreInPlaceSpec.LoadOptions.IncludeSchemas length contract enforced in code, not at CRD
- **[1×]** `api/v1alpha1/backup_types.go:514` — BackupSchedule.Schedule and VerificationSpec.Schedule lack cron format validation
- **[1×]** `api/v1alpha1/backup_types.go:143` — PITRSpec.MaxBinlogSize and DumpOptions.BytesPerChunk lack format validation for MySQL-style size strings
- **[1×]** `.github/workflows/release.yml:170` — Release pushes mutable `latest` tag; Helm default pullPolicy is IfNotPresent
- **[1×]** `.github/workflows/release.yml:230` — Release workflow mutates Chart.yaml with sed instead of templated version
- **[1×]** `.github/workflows/ci.yml:119` — CI and the release gate build the docs with Node 20, but the production docs build (ReadTheDocs) uses Node 22, so CI does not reproduce the environmen
- **[1×]** `test/envtest/backup_verification_test.go:47` — Verification jobs reuse the backup profile S3 credential instead of a read-only verification credential.
- **[1×]** `test/envtest/backup_verification_test.go:114` — The backup verification sanity query can be used as a root-SQL data exfiltration channel through CR status.
- **[1×]** `test/envtest/backup_verification_test.go:382` — Envtest verification tests never assert finalizer removal on terminal states — stuck finalizer risk is untested
- **[1×]** `test/envtest/backup_verification_test.go:46` — Backup verification can be provisioned with a missing S3 credentials Secret instead of failing fast with an actionable condition.
- **[1×]** `test/envtest/backup_verification_test.go:382` — Backup verification terminal envtest does not cover the metrics and cleanup contracts operators rely on.
- **[1×]** `internal/controller/topology.go:1104` — DNS-flip failure during failover is logged only — no metric and no Kubernetes Event — so operators cannot alert on the most common cause of post-failo
- **[1×]** `internal/controller/topology.go:769` — PITR CR status only refreshes on topology changes, not on the archiver poll cadence.
- **[1×]** `docs/docs/backup-verification.mdx:253` — Backup and verification staleness alerts do not handle missing timestamp series.
- **[1×]** `internal/controller/credentials.go:207` — Credential change-detection hash truncated to 64 bits, risking silent collision
- **[1×]** `internal/platform/websocket.go:82` — WebSocket endpoint defaults to allowing all origins and has no authentication
- **[1×]** `test/component/failover_test.go:201` — Component tests assert on only 3 of 35+ Prometheus metrics, leaving metric emission regressions undetectable
- **[1×]** `charts/bloodraven/templates/deployment.yaml:34` — terminationGracePeriodSeconds (10s) is too short for in-flight failover operations and the auxiliary server's own 10s shutdown timeout
- **[1×]** `config/crd/bases/shipstream.io_mysqlfailovergroups.yaml:667` — Backup encryption accepts weak passphrases despite using a fast offline-checkable KDF.
- **[1×]** `playground/setup.sh:397` — Replication-user setup can leave MySQL sites writable after a failed or interrupted SQL attempt.
- **[1×]** `playground/setup.sh:204` — setup.sh applies DNSEndpoint CRD fetched from raw.githubusercontent.com with no integrity check
- **[1×]** `.github/dependabot.yml:7` — Dependabot quarterly cadence leaves up to 90-day window for critical security patches
- **[1×]** `test/envtest/reconciler_test.go:91` — envtest suite has no MysqlBackup CRD schema validation test
- **[1×]** `playground/counter-app/main.go:42` — Counter-app hardcodes MySQL DSN with credentials as fallback default
- **[1×]** `playground/dashboard/main.go:224` — Dashboard /healthz always returns 200 OK regardless of backend proxy target availability
- **[1×]** `cmd/playground-chaos/reset.go:727` — SQL string escaping incomplete — does not handle NUL bytes or control characters
- **[1×]** `internal/sidecar/mysql.go:46` — Sidecar MySQL connection pool has no ConnMaxLifetime or ConnMaxIdleTime, risking stale connections
- **[1×]** `playground/manifests/dashboard.yaml:53` — Dashboard and counter services are exposed as unauthenticated NodePorts.
- **[1×]** `api/v1alpha1/mysqlstandbycluster_types.go:58` — MysqlStandbyCluster admits the unimplemented Network transport.
- **[1×]** `config/crd/bases/shipstream.io_mysqlbackupverifications.yaml:109` — Failed backup verifications retain restored database pods and PVCs by default.
- **[1×]** `internal/mysql/replication.go:55` — Failed KILL operations during failover and self-fencing are swallowed.
- **[1×]** `charts/bloodraven/templates/serviceaccount.yaml:13` — automountServiceAccountToken is explicitly set to true, exposing the API token to all containers including CronJob pods that reuse the same ServiceAcc
- **[1×]** `charts/bloodraven/dashboards/archiver.json:75` — Archiver dashboard uses `increase()` on a Gauge metric — counter-reset heuristic loses data across sidecar restarts

## Model & Role Comparison

### Per-agent (aggregated across roles)
| Agent | Found | Validated | FalsePos | Unique | Shared | Accuracy | Composite |
|---|---|---|---|---|---|---|---|
| GPT-5.5 | 178 | 178 | 0 | 73 | 56 | 100% | 202 |
| Qwen 3.7 Max | 131 | 127 | 4 | 49 | 53 | 97% | 143 |
| GLM 5.2 | 94 | 92 | 2 | 22 | 52 | 98% | 92 |
| Opus 4.8 | 84 | 84 | 0 | 19 | 50 | 100% | 88 |

Best composite: **GPT-5.5 (202)** on raw volume + uniques. Lowest: Opus 4.8 (88) — but Opus's uniques were the deepest core-correctness bugs (the data race H17, divergent-reclone H16, planned-failover rollback context). Accuracy was ~100% across all models: only 6/487 raw findings were factually wrong.

### Per-role (aggregated across models)
| Role | Found | Validated | Unique-to-role | Accuracy |
|---|---|---|---|---|
| operability | 175 | 174 | 55 | 99% |
| stewardship | 161 | 159 | 53 | 99% |
| hardening | 149 | 146 | 62 | 98% |

All three roles produced strong, well-differentiated signal (hardening had the most unique-to-role findings; operability the highest volume). None was redundant.

## Rejected (INVALID) during validation
- **G466**: False: teardown deletes the whole namespace (line 40), cascading RustFS/NetworkPolicies/backup CRs; and set -e makes a 60s-timeout non-zero exit fail the script, so "reports success" is wrong.
- **G28**: Unauthenticated aux endpoints are a documented trade-off (security-model.mdx public endpoints); also falsely claims "no connection limits" — maxClients=100 cap exists.
- **G97**: Invalid: bash expansion of ${VERSION} into sed/heredoc does not re-evaluate command substitution; no injection sink. Tag-push is privileged regardless.
- **G157**: "5 unbounded MySQL ops" is false: RecoverOldPrimary runs on checker.db which carries statusNetTimeout=4s ReadTimeout/WriteTimeout; each op is driver-bounded, poll block is ~20s not indefinite.
- **G164**: SetupReplication ops run on checker.db with statusNetTimeout=4s driver deadline (bounded), and bootstrap runs in a goroutine off the poll loop; "stall indefinitely" premise false.
- **G278**: Unauthenticated aux/sidecar endpoints are a documented trade-off (known-limitations.mdx:106-107 + NetworkPolicy guidance); fencing "manipulation" needs MITM of the fixed operator address, not "any pod".
