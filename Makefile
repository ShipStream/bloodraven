CONTROLLER_GEN ?= go run sigs.k8s.io/controller-tools/cmd/controller-gen

.PHONY: generate manifests build test vet

generate:
	$(CONTROLLER_GEN) object paths=./api/...

manifests:
	$(CONTROLLER_GEN) crd paths=./api/... output:crd:dir=config/crd/bases
	$(CONTROLLER_GEN) rbac:roleName=bloodraven-role paths=./internal/controller/... output:rbac:dir=config/rbac

build:
	go build -o bin/bloodraven ./cmd/bloodraven

test:
	go test ./...

vet:
	go vet ./...
