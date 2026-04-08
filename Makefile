CONTROLLER_GEN ?= go run sigs.k8s.io/controller-tools/cmd/controller-gen

.PHONY: help generate manifests build build-bloodraven build-sidecar test test-unit test-integration fmt vet lint docker-build

##@ General

help: ## Show this help
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) }' $(MAKEFILE_LIST)

##@ Build

build: build-bloodraven build-sidecar ## Build both operator and sidecar binaries

build-bloodraven: ## Build the operator binary
	go build -o bin/bloodraven ./cmd/bloodraven

build-sidecar: ## Build the sidecar binary
	go build -o bin/sidecar ./cmd/sidecar

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

test: ## Run all tests
	go test ./...

test-unit: ## Run unit tests only (skip integration tests)
	go test -short ./...

test-integration: ## Run integration tests only
	go test -run Integration ./...

##@ Code Quality

fmt: ## Format Go source files
	gofmt -w .

vet: ## Run go vet
	go vet ./...

lint: ## Run golangci-lint (must be installed separately)
	golangci-lint run ./...
