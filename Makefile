CONTROLLER_GEN ?= go run sigs.k8s.io/controller-tools/cmd/controller-gen

.PHONY: help generate manifests build build-bloodraven build-sidecar build-playground-chaos test test-unit test-component test-envtest test-e2e test-integration fmt vet lint docker-build chaos-list chaos-check chaos-run chaos-run-all

##@ General

help: ## Show this help
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) }' $(MAKEFILE_LIST)

##@ Build

build: build-bloodraven build-sidecar ## Build both operator and sidecar binaries

build-bloodraven: ## Build the operator binary
	go build -o bin/bloodraven ./cmd/bloodraven

build-sidecar: ## Build the sidecar binary
	go build -o bin/sidecar ./cmd/sidecar

build-playground-chaos: ## Build the playground chaos test runner
	go build -o bin/playground-chaos ./cmd/playground-chaos

docker-build: ## Build Docker images for both operator and sidecar
	docker build --target bloodraven -t bloodraven .
	docker build --target sidecar -t bloodraven-sidecar .

##@ Code Generation

generate: ## Regenerate deep copy code
	$(CONTROLLER_GEN) object paths=./api/...

manifests: ## Generate CRD and RBAC manifests
	$(CONTROLLER_GEN) crd paths=./api/... output:crd:dir=config/crd/bases
	$(CONTROLLER_GEN) rbac:roleName=bloodraven-role paths=./internal/controller/... output:rbac:dir=config/rbac

##@ Testing

test: test-unit test-component ## Run fast tests (unit + component) — default PR gate

test-all: ## Run all tests including integration (network listeners)
	go test -tags integration -race ./...

test-unit: ## Run unit tests only (no network, no listeners, fast)
	go test -race ./internal/...

test-component: ## Run component tests (cross-package with fakes, no real cluster)
	go test -race ./test/component/

test-envtest: ## Run envtest controller tests (real API server, no cluster)
	go test -race -tags envtest ./test/envtest/

test-e2e: ## Run real cluster end-to-end tests (requires kind/k3d — Phase 4, not yet implemented)
	@echo "Real cluster e2e tests are not yet implemented (Testing 2.0 Phase 4)."
	@echo "See TESTING_2.0.md for the planned scenarios."
	@exit 1

test-integration: ## Run integration tests (network listener tests)
	go test -tags integration -race ./internal/platform/ ./test/component/

##@ Code Quality

fmt: ## Format Go source files
	gofmt -w .

vet: ## Run go vet
	go vet ./...

lint: ## Run golangci-lint (must be installed separately)
	golangci-lint run ./...

##@ Playground

chaos-list: build-playground-chaos ## List registered chaos scenarios
	./bin/playground-chaos list

chaos-check: build-playground-chaos ## Verify the playground baseline is healthy
	./bin/playground-chaos check

chaos-run: build-playground-chaos ## Run a single scenario (SCENARIO=<id>)
	@if [ -z "$(SCENARIO)" ]; then echo "usage: make chaos-run SCENARIO=01-clean-primary-kill"; exit 2; fi
	./bin/playground-chaos run $(SCENARIO)

chaos-run-all: build-playground-chaos ## Run every registered chaos scenario in order
	./bin/playground-chaos run-all
