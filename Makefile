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
	cd $(ROOT_DIR)/../asynqmon && GOWORK=$(ROOT_DIR)/dev/asynqmon.work go test ./...

.PHONY: smoke-asynqmon
smoke-asynqmon:
	GOWORK=$(ROOT_DIR)/dev/asynqmon.work go run $(ROOT_DIR)/dev/asynqmon_smoke.go

# Opt-in concurrent load test. This runs SCRIPT FLUSH against the configured
# disposable Redis server; see docs/inspector-soak.md before using it.
.PHONY: soak-inspector-batch
soak-inspector-batch:
	go run $(ROOT_DIR)/dev/inspector_batch_soak.go
