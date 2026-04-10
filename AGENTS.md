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
- `docker build --target bloodraven -t bloodraven .` and `docker build --target sidecar -t bloodraven-sidecar .` build container images.

## Coding Style & Naming Conventions
Use standard Go formatting: run `gofmt` on changed files and keep imports organized. Follow existing package boundaries and keep Kubernetes-facing types explicit and stable. Exported names use `CamelCase`; unexported helpers use `camelCase`; tests follow `TestXxx`. Prefer descriptive file names like `failover.go`, `fencing.go`, and `matrix_test.go` that match one responsibility.

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

## Architecture & Configuration Notes
This project is a Go 1.25 Kubernetes operator built around a single custom resource and two binaries. When making material changes to reconciliation, failover, CRD types, sidecar behavior, or deployment model, update the relevant documentation in `docs/` to keep code and docs aligned.
