ROOT_DIR:=$(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))

proto: internal/proto/asynq.proto
	protoc -I=$(ROOT_DIR)/internal/proto \
				 --go_out=$(ROOT_DIR)/internal/proto \
				 --go_opt=module=github.com/pars-aria-labs/asynq/internal/proto \
				 $(ROOT_DIR)/internal/proto/asynq.proto

.PHONY: lint
lint:
	golangci-lint run

# Test the sibling monitor against this checkout, including the local exporter.
.PHONY: test-asynqmon
test-asynqmon:
	@set -eu; \
	workspace_dir="$$(mktemp -d)"; \
	trap 'rm -r -- "$$workspace_dir"' EXIT; \
	cd "$$workspace_dir"; \
	GOWORK=off go work init "$(ROOT_DIR)" "$(ROOT_DIR)/x" "$(ROOT_DIR)/../asynqmon"; \
	root_version="$$(awk '$$1 == "github.com/pars-aria-labs/asynq" { print $$2 }' "$(ROOT_DIR)/x/go.mod")"; \
	x_version="$$(awk '$$1 == "github.com/pars-aria-labs/asynq/x" { print $$2 }' "$(ROOT_DIR)/../asynqmon/go.mod")"; \
	test -n "$$root_version"; \
	test -n "$$x_version"; \
	GOWORK="$$workspace_dir/go.work" go work edit \
		"-replace=github.com/pars-aria-labs/asynq@$${root_version}=$(ROOT_DIR)" \
		"-replace=github.com/pars-aria-labs/asynq/x@$${x_version}=$(ROOT_DIR)/x"; \
	cd "$(ROOT_DIR)/../asynqmon"; \
	GOWORK="$$workspace_dir/go.work" go test ./...

.PHONY: smoke-asynqmon
smoke-asynqmon:
	@set -eu; \
	workspace_dir="$$(mktemp -d)"; \
	trap 'rm -r -- "$$workspace_dir"' EXIT; \
	cd "$$workspace_dir"; \
	GOWORK=off go work init "$(ROOT_DIR)" "$(ROOT_DIR)/x" "$(ROOT_DIR)/../asynqmon"; \
	root_version="$$(awk '$$1 == "github.com/pars-aria-labs/asynq" { print $$2 }' "$(ROOT_DIR)/x/go.mod")"; \
	x_version="$$(awk '$$1 == "github.com/pars-aria-labs/asynq/x" { print $$2 }' "$(ROOT_DIR)/../asynqmon/go.mod")"; \
	test -n "$$root_version"; \
	test -n "$$x_version"; \
	GOWORK="$$workspace_dir/go.work" go work edit \
		"-replace=github.com/pars-aria-labs/asynq@$${root_version}=$(ROOT_DIR)" \
		"-replace=github.com/pars-aria-labs/asynq/x@$${x_version}=$(ROOT_DIR)/x"; \
	cd "$(ROOT_DIR)"; \
	GOWORK="$$workspace_dir/go.work" go run "$(ROOT_DIR)/.github/scripts/asynqmon_smoke.go"

# Opt-in concurrent load test. This runs SCRIPT FLUSH against the configured
# disposable Redis server; read the safety note in README.md before using it.
.PHONY: soak-inspector-batch
soak-inspector-batch:
	go run $(ROOT_DIR)/.github/scripts/inspector_batch_soak.go
