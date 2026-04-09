# TODO

This file captures the highest-value improvements for the root `bloodraven` module. The focus is on closing functional gaps first, then tightening schema, testability, and operator ergonomics.

## Priority 0: Runtime correctness

### 1. Start the topology manager in the production controller
Problem:
- The root binary starts controller-runtime, the reconciler, metrics, probes, and the auxiliary HTTP server.
- The poll loop, failover orchestration, DNS updates, websocket broadcasts, and readiness behavior are implemented in `internal/controller/topology.go` but appear to be exercised only by tests.

Why this matters:
- The current runtime may reconcile Kubernetes objects without actually performing the failover/control-plane behavior described in `README.md`.
- This is the largest gap between documented behavior and live behavior.

Recommended work:
- Add a manager runnable that watches `MysqlReplicaPair` resources and starts one topology manager per active pair.
- Decide ownership clearly:
  - Reconciler owns desired resources.
  - Topology manager owns polling, state transitions, failover, DNS, taints, and status updates.
- Define lifecycle rules for creating, updating, and stopping per-pair topology managers.
- Ensure leader election semantics are correct so only the elected manager executes polling/failover.

Acceptance criteria:
- A live controller process actually polls both DCs.
- Failover logic runs without test-only wiring.
- `/status` reflects real topology state rather than a static `{status: "ok"}` response.

### 2. Persist real status to the CR status subresource
Problem:
- `MysqlReplicaPairStatus` exposes `primaryDC`, `dc1`, `dc2`, `conditions`, `lastFailover`, `lastFailoverTarget`, and `websocketClients`.
- The reconciler depends on status for sidecar env injection and pod label sync.
- I did not find a root-module path that updates status from the topology manager.

Why this matters:
- Pod role labels and sidecar startup logic can drift if status is stale or empty.
- Operators need accurate status to debug failovers safely.

Recommended work:
- Define a status projection from topology manager state to CR status.
- Update status only on meaningful state changes to avoid excessive API churn.
- Populate:
  - `status.primaryDC`
  - `status.dc1.state`, `status.dc2.state`
  - `status.dc1.lastSeen`, `status.dc2.lastSeen`
  - `status.lastFailover`
  - `status.lastFailoverTarget`
  - `status.conditions` for split brain, no primary, total loss, ready
- Decide whether `websocketClients` belongs in CR status. If not operationally useful, remove it from the API instead of leaving it half-wired.

Acceptance criteria:
- `kubectl get mysqlreplicapairs -o yaml` shows current topology state.
- Reconciler decisions based on status are backed by live status updates.

## Priority 1: API and schema quality

### 3. Add kubebuilder validation and default markers
Problem:
- Defaults are described in comments but not enforced by schema generation.
- Fields that should be constrained can currently accept invalid or ambiguous values.

Recommended work:
- Add kubebuilder markers for:
  - default values for `pollInterval`, `failureThreshold`, `recoveryThreshold`, `failoverCooldown`, `leaseTimeout`, `peerCheckInterval`
  - minimum values for thresholds and durations
  - enums for state/status fields where appropriate
  - optional/required boundaries for `secretName`, `zoneID`, DC names, and storage settings
- Regenerate CRDs after adding markers.

Acceptance criteria:
- Generated CRDs include defaults and validation.
- Invalid specs fail fast at admission time rather than during reconciliation.

### 4. Remove or finish partially wired API fields
Problem:
- A few API surfaces look broader than the active runtime integration.

Recommended work:
- Review each status/spec field and classify it as:
  - required and implemented
  - planned but not implemented
  - unnecessary
- Remove fields that do not carry operational value or are unlikely to be maintained accurately.

Suggested candidates to review:
- `status.websocketClients`
- `status.lastFailoverTarget`
- any conditions not actually emitted

Acceptance criteria:
- The API surface matches the real operator behavior.
- No field exists solely because it seemed useful once.

## Priority 2: Build, manifests, and local workflow

### 5. Fix the manifest generation workflow
Problem:
- `make manifests` writes to `config/crd/bases` and `config/rbac`, but the root `config/` directory is not present.

Recommended work:
- Decide whether `config/` should exist in the root module.
- If yes, commit the expected structure and generated artifacts.
- If no, change `Makefile` targets to the correct output path or remove the target until the layout is ready.

Acceptance criteria:
- `make manifests` succeeds in a clean checkout.
- Generated paths are documented and committed or intentionally ignored.

### 6. Expand the root Makefile
Problem:
- The current Makefile is thin and only builds the main controller binary.

Recommended work:
- Add explicit targets for:
  - `build-bloodraven`
  - `build-sidecar`
  - `test-unit`
  - `test-integration`
  - `fmt`
  - `lint` or `vet`
  - `manifests`
  - `docker-build`
- Consider setting a writable local `GOCACHE` or documenting the expectation for constrained environments.

Acceptance criteria:
- A contributor can discover the main workflows from `make help` or the README.
- Sidecar and operator builds are both first-class.

## Priority 3: Test reliability

### 7. Split pure unit tests from listener/network tests
Problem:
- Several tests depend on `httptest` listeners and real timing.
- These fail in restricted sandboxes and will be more brittle in constrained CI.

Recommended work:
- Separate tests into:
  - pure unit tests with no sockets or sleeps
  - integration tests that require listeners
- Use build tags or naming conventions for integration cases if needed.
- Favor injected transports or interfaces over local HTTP servers where practical.

Acceptance criteria:
- `go test ./...` is reliable in standard CI.
- Integration tests remain available but are intentionally scoped.

### 8. Reduce `time.Sleep`-driven test logic
Problem:
- Some tests wait for background loops using fixed sleeps.

Recommended work:
- Replace passive sleeps with deterministic synchronization:
  - channels
  - hooks/callbacks
  - fake clocks where practical
- Keep one or two high-level time-based tests, not many.

Acceptance criteria:
- Tests fail because behavior is wrong, not because timing shifted by 50ms.

## Priority 4: Operational hardening

### 9. Tighten defaults around images and configuration
Problem:
- The reconciler defaults the MySQL image to `mysql:9.6`, and the sidecar image falls back to the MySQL image if unspecified.

Recommended work:
- Make default image behavior explicit and documented.
- Consider requiring `sidecarImage` rather than silently reusing the MySQL image unless the image is truly multi-purpose.
- Validate that secrets include the exact keys the sidecar and MySQL containers require.

Acceptance criteria:
- Deployments fail early and clearly when configuration is incomplete.
- There is no ambiguous image selection at runtime.

### 10. Revisit websocket exposure assumptions, but keep this low priority
Current conclusion:
- If `/ws/status` is only reachable as an in-cluster `ClusterIP` service, with no ingress and no untrusted workloads in the cluster, strict origin checks or a token are not necessary right now.

Why this is acceptable:
- Same-cluster traffic is already inside the trust boundary.
- Browser-origin protections matter most when a browser can reach the endpoint from outside or across trust zones.
- A token adds complexity without much value if the service is not exposed beyond trusted workloads.

Caveats:
- This changes if any of the following become true:
  - the websocket endpoint is exposed through ingress or a load balancer
  - multi-tenant workloads share the cluster
  - the hub begins carrying higher-sensitivity data or control actions

Recommended work:
- Keep the current permissive websocket behavior for now.
- Document the assumption in `README.md` or deployment docs:
  - internal-only service
  - no ingress
  - trusted cluster boundary
- If exposure expands later, add either:
  - strict origin checks for browser clients, or
  - service-to-service auth for non-browser clients

Acceptance criteria:
- The security posture matches the actual deployment model.
- Future exposure changes have an obvious place to hook in auth/origin restrictions.

## Priority 5: Documentation alignment

### 11. Reconcile docs with actual implementation
Problem:
- The README is detailed and confident. That is good, but only if it precisely matches the live root module.

Recommended work:
- After wiring the topology manager and status updates, audit:
  - `README.md`
  - `BLOODRAVEN_UPGRADE.md`
  - generated manifests
  - sample CRs
- Mark aspirational features clearly if they are not yet live.

Acceptance criteria:
- A new contributor can trust the docs without reverse-engineering the code.

## Suggested sequence

1. Start topology managers from the production binary.
2. Write CR status from live topology state.
3. Fix manifest generation and expand the Makefile.
4. Add schema defaults/validation.
5. Refactor tests for deterministic execution.
6. Tighten configuration validation and document internal-only websocket assumptions.
