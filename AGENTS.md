# Repository Guidelines

## Project Structure & Module Organization
Primary code lives in the root Go module. `cmd/bloodraven` is the Kubernetes operator entrypoint; `cmd/sidecar` is the per-MySQL sidecar. API types live in `api/v1alpha1`, controller logic in `internal/controller`, and supporting packages in `internal/mysql`, `internal/platform`, `internal/sidecar`, `internal/state`, and `internal/metrics`. End-to-end and scenario-style tests live in `test/e2e`. Treat `bitpoke/` and `orchestrator/` as bundled upstream references, not the default place for new feature work.

## Build, Test, and Development Commands
Run commands from the repository root:

- `make build` builds the operator binary at `bin/bloodraven`.
- `go build ./cmd/sidecar` builds the sidecar binary.
- `make test` runs `go test ./...` across unit and e2e-style packages.
- `make vet` runs `go vet ./...`.
- `make lint` runs `golangci-lint run ./...`. `golangci-lint` is not vendored; install it with `go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest` (it lands in `$(go env GOPATH)/bin`). CI installs the same tool with the same command in `.github/workflows/ci.yml`, so local and CI output match when you run this.
- `make generate` refreshes API deep-copy code in `api/v1alpha1`.
- `make manifests` generates CRD and RBAC output under `config/`.
- `docker build --target bloodraven -t bloodraven .` and `docker build --target sidecar -t bloodraven-sidecar .` build container images. Podman works too (substitute `podman` for `docker`), but docker is preferred because k3d's podman support is experimental.

## Coding Style & Naming Conventions
Use standard Go formatting: run `gofmt` on changed files and keep imports organized. Follow existing package boundaries and keep Kubernetes-facing types explicit and stable. Exported names use `CamelCase`; unexported helpers use `camelCase`; tests follow `TestXxx`. Prefer descriptive file names like `failover.go`, `fencing.go`, and `matrix_test.go` that match one responsibility.

Structured-log `msg` strings and field names listed in `docs/docs/log-schema.mdx` are a public stability contract — downstream log pipelines filter on them. When you touch a log call site whose `msg` appears in that doc's Event reference, either preserve the `msg` string and the documented field set exactly, or update `docs/docs/log-schema.mdx` in the same PR and call out the break in the PR description. The same applies to field naming: log keys are `camelCase` (per the contract), not `snake_case`.

## Testing Guidelines
Add table-driven unit tests beside the code they cover, using the existing `*_test.go` layout under `internal/`. Put cross-component behavior tests in `test/e2e`. Some tests create local HTTP listeners with `httptest`, so restricted sandboxes may fail even when local developer runs pass.

### Pre-PR gate (required, do not skip)
Before pushing a branch that opens or updates a PR, run all of the following from the repo root and fix anything they report. Do **not** push expecting CI to find problems you could have caught locally — CI failures on lint or generate drift are round-trip latency and reviewer noise.

1. `make generate && make manifests` — regenerates deep-copy code, CRD YAML under `config/crd/bases`, and RBAC under `config/rbac`. If this produces a diff, commit it. The `Generate Check` CI job fails PRs that leave generated files stale.
2. `make vet`
3. `make lint` — install `golangci-lint` if missing (see Build section). Staticcheck findings like `SA9003` (empty branches), `SA1019` (deprecation), and `errcheck` are all enforced here and will block the Lint CI job.
4. `make test` (unit + component). Run `make test-envtest` too when you touch anything that interacts with the API server (CRD validation, Owns/Watches wiring, status subresource writes).
5. When you modify kubebuilder RBAC markers on a reconciler, also update `charts/bloodraven/templates/clusterrole.yaml` to mirror them — the Helm chart RBAC is hand-maintained and drifts silently from `config/rbac/role.yaml`.
6. When you add or modify a CRD, copy the regenerated file(s) from `config/crd/bases/` to `charts/bloodraven/crds/` so Helm installs ship the same schema.

## Commit & Pull Request Guidelines
Recent history uses short, imperative subjects such as `Upgrade mysql-watcher to Bloodraven MySQL operator`. Keep commit titles concise and action-oriented. PRs should explain the operational impact, note any CRD, failover, or sidecar behavior changes, link the relevant issue, and include logs, manifests, or screenshots when changing observable cluster behavior.

## Playground
The `playground/` directory deploys a complete Bloodraven environment into a local multi-node Kubernetes cluster (k3d recommended). It includes a two-site MySQL cluster, real-time dashboard, counter app, and chaos tools for triggering failovers. All scripts auto-detect docker or podman (preferring docker, since k3d's podman support is experimental) and the cluster tool (k3d, kind, or minikube). Set `BLOODRAVEN_CONTAINER_RUNTIME=podman` to force podman if both are installed.

Key scripts (run from repo root):
- `./playground/setup.sh` — builds images, installs CRDs, deploys everything. Takes ~2 minutes.
- `./playground/rebuild.sh [component ...]` — rebuilds images and restarts deployments after code changes. Components: `operator`, `sidecar`, `counter`, `dashboard`, `dns-webhook`.
- `./playground/chaos.sh <action> [site]` — triggers failover scenarios: `kill-site iad`, `cordon pdx`, `network-partition iad`, `recover`.
- `./playground/teardown.sh` — removes all playground resources, leaves the cluster intact.
- `./playground/reset-mysql.sh` — wipes MySQL data and PVCs without full teardown.

After creating a cluster, dump the config so it can be monitored remotely:
```
k3d kubeconfig get bloodraven
```

Access apps after setup:
```
kubectl -n bloodraven-playground port-forward svc/dashboard 8091:8091
kubectl -n bloodraven-playground port-forward svc/counter-app 8090:8090
```

Full documentation: `docs/docs/playground.mdx`.

## Chaos & Integration Testing in the Playground

Lessons from running chaos scenarios against a live k3d cluster:

### Environment setup
- k3d cluster needs `--k3s-arg '--tls-san=<hostname>@server:0'` if you want remote kubectl access (e.g. over Tailscale).
- Podman + k3d: images get a `localhost/` prefix after import. `setup.sh` handles this automatically via `IMG_PREFIX`, but if you apply manifests manually, use `localhost/bloodraven:playground` etc.
- After any MySQL data wipe (`reset-mysql.sh` or PVC deletion), the `replicator` user must be recreated — it's not part of the Docker entrypoint. The reset and setup scripts do this automatically, but manual resets require `CREATE USER IF NOT EXISTS 'replicator'@'%' ...` with REPLICATION SLAVE, BACKUP_ADMIN, CLONE_ADMIN grants.

### Running chaos scenarios
- **`kill-site` vs `scale --replicas=0`**: `chaos.sh kill-site` deletes the pod, but the Deployment controller recreates it in <5s. To truly hold a site down (e.g. for self-fencing or anti-flap tests), use `kubectl scale deployment --replicas=0`. The pod kill simulates a crash; the scale-down simulates a sustained outage.
- **Network partition doesn't work via iptables on host netns**: `chaos.sh network-partition` blocks port 3306 on the k3d node's host network, but kube-proxy DNAT operates in different iptables chains. The operator still reaches MySQL via ClusterIP. True partition testing needs pod-level network manipulation or NetworkPolicy.
- **Relay log drain takes 30s on dead primary**: Failover won't complete until the 30s relay log drain timeout expires when the old primary is unreachable. Plan for ~37s total failover time in tests, not ~6s.
- **Sandbox blocks kubectl**: If running inside a sandboxed environment (e.g. Claude Code), kubectl needs network access to the k3d API server port. Use `dangerouslyDisableSandbox` or whitelist the port.

### Known operator behaviors during testing
- After operator restart, `lastFailoverTarget` is lost (volatile). Old primary recovery won't trigger until the next failover within the new operator lifecycle. This is a known gap (see `playground/chaos-results.md`).
- Fresh-deploy bootstrap (`isFreshDeploy`) checks `SHOW REPLICA STATUS` — if a previous failed clone left replication metadata, bootstrap won't trigger. Fix: `STOP REPLICA; RESET REPLICA ALL;` on the stuck site, then restart operator.
- CLONE INSTANCE in Docker returns Error 3707 ("Restart server failed") because there's no mysqld supervisor. This is handled as an expected connection drop — Kubernetes restarts the container.

### Rebuilding after code changes
`./playground/rebuild.sh operator` builds, imports to k3d, and restarts the operator deployment. For sidecar changes, use `./playground/rebuild.sh sidecar` (restarts MySQL pods). Both can be combined: `./playground/rebuild.sh operator sidecar`.

### Automated chaos runner
A subset of `playground/chaos-scenarios.md` is automated by `cmd/playground-chaos` and exposed as Make targets: `make chaos-list`, `make chaos-check`, `make chaos-run SCENARIO=<id>`, `make chaos-run-all`. The runner refuses to mutate any kubectl context outside the `_guard.sh` allowlist; on assertion failure it captures cluster YAML + pods + events + operator/sidecar logs + raw `/metrics` under `playground/chaos-results/<timestamp>/<scenario-id>/` for triage. Use `--no-cleanup` to keep injected state in place for forensics.

The runner stamps an in-progress marker on the MFG (`chaos.playground.bloodraven.io/in-progress`) after Precheck and clears it on cleanup. A subsequent run that finds a leftover marker refuses to start with a specific reason (live owner / abandoned / different host). Override with `--force` (delete the marker before preflight) or `--auto-reset` (on Precheck failure, shell out to `reset-mysql.sh + setup.sh` and retry once; 3s pause unless `CI=1`). `chaos-check` runs the same structural baseline scenarios use — stuck scale-to-0 deployments, bogus `lastFailoverTarget`, anti-flap cooldown still ticking, `NoPrimary` (both-sites-read-only), replication off on a non-active candidate — each with the exact remediation command in the error.

`./playground/reset-mysql.sh` now scales the operator to 0 before stripping taints (no taint/operator race), restarts `local-path-provisioner` to reset its 15-failure threshold, JSON-patches `/status` to clear `lastFailover{,Target}`/`promotionGtidExecuted`/`plannedFailover` (otherwise `isFreshDeploy` skips bootstrap and the cluster stalls on the matrix.go `NoPrimary` guard), and on wait-loop timeout dumps full forensics to `playground/chaos-results/reset-<timestamp>/` and exits non-zero.

## Architecture & Configuration Notes
This project is a Go 1.26 Kubernetes operator built around a single custom resource and two binaries. When making material changes to reconciliation, failover, CRD types, sidecar behavior, or deployment model, update the relevant documentation in `docs/` to keep code and docs aligned.
