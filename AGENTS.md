# Repository Guidelines

## Project Structure & Module Organization
Primary code lives in the root Go module. `cmd/bloodraven` is the Kubernetes operator entrypoint; `cmd/sidecar` is the per-MySQL sidecar. API types live in `api/v1alpha1`, controller logic in `internal/controller`, and supporting packages in `internal/mysql`, `internal/platform`, `internal/sidecar`, `internal/state`, and `internal/metrics`. End-to-end and scenario-style tests live in `test/e2e`. Treat `bitpoke/` and `orchestrator/` as bundled upstream references, not the default place for new feature work.

## Build, Test, and Development Commands
Run commands from the repository root:

- `make build` builds the operator binary at `bin/bloodraven`.
- `go build ./cmd/sidecar` builds the sidecar binary.
- `make test` runs `go test ./...` across unit and e2e-style packages.
- `make vet` runs `go vet ./...`.
- `make generate` refreshes API deep-copy code in `api/v1alpha1`.
- `make manifests` generates CRD and RBAC output under `config/`.
- `docker build --target bloodraven -t bloodraven .` and `docker build --target sidecar -t bloodraven-sidecar .` build container images.

## Coding Style & Naming Conventions
Use standard Go formatting: run `gofmt` on changed files and keep imports organized. Follow existing package boundaries and keep Kubernetes-facing types explicit and stable. Exported names use `CamelCase`; unexported helpers use `camelCase`; tests follow `TestXxx`. Prefer descriptive file names like `failover.go`, `fencing.go`, and `matrix_test.go` that match one responsibility.

## Testing Guidelines
Add table-driven unit tests beside the code they cover, using the existing `*_test.go` layout under `internal/`. Put cross-component behavior tests in `test/e2e`. Before opening a PR, run `make test` and `make vet`. Some tests create local HTTP listeners with `httptest`, so restricted sandboxes may fail even when local developer runs pass.

## Commit & Pull Request Guidelines
Recent history uses short, imperative subjects such as `Upgrade mysql-watcher to Bloodraven MySQL operator`. Keep commit titles concise and action-oriented. PRs should explain the operational impact, note any CRD, failover, or sidecar behavior changes, link the relevant issue, and include logs, manifests, or screenshots when changing observable cluster behavior.

## Architecture & Configuration Notes
This project is a Go 1.25 Kubernetes operator built around a single custom resource and two binaries. When making material changes to reconciliation, failover, CRD types, sidecar behavior, or deployment model, update the relevant documentation in `docs/` to keep code and docs aligned.
