SHELL := /usr/bin/env bash
GO ?= go
BUF ?= buf
PROTOC_GEN_GO_VERSION := v1.36.6
PROTOC_GEN_GO_GRPC_VERSION := v1.5.1
SYFT_VERSION := v1.50.0
BIN_DIR := $(CURDIR)/bin
COMMANDS := orchigram orchigram-plugin-agent-command orchigram-plugin-exec orchigram-plugin-http orchigram-plugin-github

.PHONY: tools release-tools generate generate-check fmt vet test race lint buf-check secret-scan history-scan dependency-licenses plugin-sboms build cross-build check release-check clean

tools:
	GOBIN=$(BIN_DIR) $(GO) install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GEN_GO_VERSION)
	GOBIN=$(BIN_DIR) $(GO) install google.golang.org/grpc/cmd/protoc-gen-go-grpc@$(PROTOC_GEN_GO_GRPC_VERSION)

release-tools:
	GOBIN=$(BIN_DIR) $(GO) install github.com/anchore/syft/cmd/syft@$(SYFT_VERSION)

generate: tools
	PATH="$(BIN_DIR):$$PATH" $(BUF) generate

generate-check: generate
	git diff --exit-code -- gen

fmt:
	@test -z "$$($(GO) fmt ./...)"

vet:
	$(GO) vet ./...

test:
	$(GO) test ./...

race:
	$(GO) test -race ./...

lint:
	$(GO) run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.4.0 run

buf-check:
	$(BUF) lint
	$(BUF) build

secret-scan:
	$(GO) run github.com/zricethezav/gitleaks/v8@v8.28.0 detect --no-git --source . --redact --no-banner

history-scan:
	$(GO) run github.com/zricethezav/gitleaks/v8@v8.28.0 git --redact --no-banner

dependency-licenses:
	./scripts/dependency-licenses.sh .release/dependency-licenses.csv

plugin-sboms:
	PATH="$(BIN_DIR):$$PATH" ./scripts/plugin-sboms.sh .release/plugin-bundles

build:
	@mkdir -p $(BIN_DIR)
	@for command in $(COMMANDS); do $(GO) build -trimpath -o "$(BIN_DIR)/$$command" "./cmd/$$command"; done

cross-build:
	@mkdir -p $(BIN_DIR)/cross
	@for target in darwin/amd64 darwin/arm64 linux/amd64 linux/arm64; do \
		os="$${target%/*}"; arch="$${target#*/}"; \
		for command in $(COMMANDS); do \
			CGO_ENABLED=0 GOOS="$$os" GOARCH="$$arch" $(GO) build -trimpath -o "$(BIN_DIR)/cross/$${command}_$${os}_$${arch}" "./cmd/$$command"; \
		done; \
	done

check: generate-check fmt vet test race lint buf-check secret-scan build

release-check: check history-scan cross-build release-tools
	PATH="$(BIN_DIR):$$PATH" $(GO) run github.com/goreleaser/goreleaser/v2@v2.12.7 release --snapshot --clean

clean:
	rm -rf $(BIN_DIR) dist .release coverage.out
