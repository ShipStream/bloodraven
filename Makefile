CONTROLLER_GEN ?= go run sigs.k8s.io/controller-tools/cmd/controller-gen

.PHONY: help generate manifests build build-bloodraven build-sidecar build-playground-chaos build-kubectl-plugin install-kubectl-plugin test test-unit test-component test-envtest test-e2e test-e2e-smoke test-integration dst dst-repro fmt vet lint docker-build chaos-list chaos-check chaos-run chaos-run-all chaos-run-all-profile course course-build course-verify course-check

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

# KUBECTL_PLUGIN_VERSION stamps the version string visible to
# `kubectl bloodraven version`. Pass `make build-kubectl-plugin
# KUBECTL_PLUGIN_VERSION=v0.2.0` at release time.
KUBECTL_PLUGIN_VERSION ?= dev

build-kubectl-plugin: ## Build the kubectl-bloodraven plugin binary
	go build -ldflags "-X main.Version=$(KUBECTL_PLUGIN_VERSION)" -o bin/kubectl-bloodraven ./cmd/kubectl-bloodraven

# install-kubectl-plugin drops the binary into the first writable
# directory on $PATH that looks like a kubectl plugin location. We try
# ~/.local/bin first (creating it if needed), then walk $PATH for any
# user-writable directory, and only fall back to /usr/local/bin when
# nothing else works — printing a clear message if even that requires
# sudo. Override with `make install-kubectl-plugin DEST=/some/dir`.
install-kubectl-plugin: build-kubectl-plugin ## Install the kubectl-bloodraven plugin onto $PATH
	@dest="$(DEST)"; \
	if [ -z "$$dest" ]; then \
	  cand="$${HOME}/.local/bin"; \
	  mkdir -p "$$cand" 2>/dev/null || true; \
	  if [ -d "$$cand" ] && [ -w "$$cand" ]; then dest="$$cand"; fi; \
	fi; \
	if [ -z "$$dest" ]; then \
	  IFS=:; for p in $$PATH; do \
	    [ -z "$$p" ] && continue; \
	    case "$$p" in /sbin|/usr/sbin|/bin|/usr/bin) continue;; esac; \
	    if [ -d "$$p" ] && [ -w "$$p" ]; then dest="$$p"; break; fi; \
	  done; unset IFS; \
	fi; \
	if [ -z "$$dest" ]; then \
	  if [ -w /usr/local/bin ]; then dest=/usr/local/bin; \
	  else \
	    echo "no writable directory found on \$$PATH; pick one and re-run, e.g.:"; \
	    echo "  sudo install -m 0755 bin/kubectl-bloodraven /usr/local/bin/kubectl-bloodraven"; \
	    echo "  make install-kubectl-plugin DEST=\$$HOME/bin"; \
	    exit 1; \
	  fi; \
	fi; \
	echo "installing bin/kubectl-bloodraven into $$dest"; \
	install -m 0755 bin/kubectl-bloodraven "$$dest/kubectl-bloodraven"; \
	echo "run 'kubectl bloodraven version' to verify"

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

E2E_PROFILE ?= release
E2E_JUNIT_OUT ?= playground/chaos-results/e2e-$(E2E_PROFILE)-junit.xml
E2E_ARGS ?=

test-e2e: build-playground-chaos ## Run real-cluster E2E tests (E2E_PROFILE=release|smoke|full; requires kind/k3d)
	./bin/playground-chaos run-all --profile=$(E2E_PROFILE) --auto-reset --continue-on-failure --junit-out=$(E2E_JUNIT_OUT) $(E2E_ARGS)

test-e2e-smoke: build-playground-chaos ## Run real-cluster E2E smoke (smoke profile — requires kind/k3d)
	$(MAKE) test-e2e E2E_PROFILE=smoke E2E_JUNIT_OUT=playground/chaos-results/e2e-smoke-junit.xml

test-integration: ## Run integration tests (network listener tests)
	go test -tags integration -race ./internal/platform/ ./test/component/

DST_ARGS ?=
dst: ## Run the DST saturation campaign (deterministic simulation of the failover loop)
	DST_FULL=1 DST_REPORT=$${DST_REPORT:-$(CURDIR)/playground/chaos-results/dst-report.txt} go test ./internal/dst/ -run TestDST_Campaign -v -count=1 -timeout 45m $(DST_ARGS)

dst-repro: ## Replay one DST seed with events + operator logs (DST_SEED=<n> [DST_SKIP=i,j] make dst-repro)
	go test ./internal/dst/ -run TestDST_Repro -v -count=1

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

chaos-run-all-profile: build-playground-chaos ## Run chaos scenarios filtered by profile (PROFILE=smoke|release|full)
	@if [ -z "$(PROFILE)" ]; then echo "usage: make chaos-run-all-profile PROFILE=smoke"; exit 2; fi
	./bin/playground-chaos run-all --profile=$(PROFILE)

##@ Course

COURSE_DIR ?= course
COURSE_SLUG ?= course-bloodraven-in-production
COURSE_STATIC ?= docs/static/course

course-build: ## Build the "Bloodraven in Production" course site
	@# npm ci when the lockfile is present (CI, and any clean checkout) so the
	@# committed site is reproducible; npm install only as a fallback.
	cd $(COURSE_DIR) && \
		if [ -f package-lock.json ]; then npm ci --no-audit --no-fund; \
		else npm install --no-audit --no-fund; fi && \
		npm run build

course: course-build ## Build the course and sync it into docs/static/course
	rm -rf $(COURSE_STATIC)
	mkdir -p $(COURSE_STATIC)
	cp -r $(COURSE_DIR)/$(COURSE_SLUG)/dist/. $(COURSE_STATIC)/
	@echo "synced $$(find $(COURSE_STATIC) -type f | wc -l) file(s) into $(COURSE_STATIC)"

course-verify: course-build ## Run the course content gates and runtime tests
	@# Both run against dist/, so the build above is a real dependency, not
	@# just ordering: `verify` reads the built pages and `test` drives them in
	@# jsdom.
	cd $(COURSE_DIR) && npm run verify && npm run test

course-check: course ## Fail if docs/static/course is stale relative to the course source
	@# git status --porcelain, not git diff: diff only reports tracked files, so
	@# an untracked or newly added page under docs/static/course would slip past.
	@status="$$(git status --porcelain -- $(COURSE_STATIC))"; \
	if [ -n "$$status" ]; then \
		if git ls-files --error-unmatch $(COURSE_STATIC) >/dev/null 2>&1; then \
			echo "ERROR: $(COURSE_STATIC) is stale."; \
			echo "The committed course site no longer matches the course source."; \
			echo "Run 'make course' and commit the result."; \
		else \
			echo "ERROR: $(COURSE_STATIC) is not committed yet."; \
			echo "It is generated output that ships with the docs site, like charts/bloodraven/crds."; \
			echo "Run 'make course' and commit $(COURSE_STATIC)."; \
		fi; \
		echo "$$status" | head -20; \
		exit 1; \
	fi
	@echo "$(COURSE_STATIC) is up to date."
