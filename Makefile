# Makefile for the go-raft project

# --- Variables ---
# Get all packages that contain at least one test file. This is more robust.
PKGS_TO_TEST := $(shell go list -f '{{if .TestGoFiles}}{{.ImportPath}}{{end}}' ./...)

# Define the output binary names
SERVER_BINARY=raft-server
CLIENT_BINARY=raft-client
SERVER_CMD_PATH=./cmd/server
CLIENT_CMD_PATH=./cmd/client

# go import format
GO_FILES := $(shell find . -type f -name '*.go' -not -path "./vendor/*")
GOIMPORTS_REVISER := goimports-reviser
COMPANY_PREFIXES := "github.com/xmh1011"
PROJECT_NAME := "github.com/xmh1011/go-raft"
IMPORTS_ORDER := "std,general,company,project"

# --- Targets ---

.PHONY: all deps build test integration-test cover install-mockgen mockgen clean help cluster stop-cluster proto install-protoc-gen install-go-imports-reviser format

.DEFAULT_GOAL := help

## help: Shows this help message.
help:
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

## all: Run all tests.
all: test

## deps: Tidy and download dependencies.
deps:
	@echo " tidy and downloading dependencies..."
	@go mod tidy

## build: Build both raft-server and raft-client binaries.
build: deps
	@echo " building $(SERVER_BINARY)..."
	@go build -o $(SERVER_BINARY) $(SERVER_CMD_PATH)
	@echo " building $(CLIENT_BINARY)..."
	@go build -o $(CLIENT_BINARY) $(CLIENT_CMD_PATH)

## test: Run all unit tests with race detector and coverage for ALL packages.
test: deps
	@echo " running unit tests..."
	@go test -race -timeout=20m -v -cover -coverprofile=coverage.txt -coverpkg=./... $(PKGS_TO_TEST)

## integration-test: Run integration tests.
integration-test: deps
	@echo " running integration tests..."
	@go test -race -v ./tests/...

## cover: Open the HTML coverage report in your browser.
cover: test
	@echo " opening coverage report..."
	@go tool cover -html=coverage.txt

install-mockgen:
	@echo "Installing mockgen..."
	@command -v mockgen >/dev/null 2>&1 || go install github.com/golang/mock/mockgen@latest

mockgen:
	mockgen -source=storage/storage.go -destination=storage/storage_mock.go -package=storage
	mockgen -source=transport/transport.go -destination=transport/transport_mock.go -package=transport

install-protoc-gen:
	@echo "Installing protoc-gen-go and protoc-gen-go-grpc..."
	@go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	@go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

## proto: Generate gRPC code from proto files.
proto: install-protoc-gen
	@echo " generating gRPC code..."
	@protoc --go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		transport/grpc/pb/raft.proto

## cluster: Start a 3-node local cluster.
cluster: build
	@echo " starting 3-node cluster..."
	@mkdir -p raft-data
	@nohup ./$(SERVER_BINARY) --id 1 --peers "1=127.0.0.1:8001,2=127.0.0.1:8002,3=127.0.0.1:8003" --data raft-data > raft-node-1.log 2>&1 & echo $$! > raft-node-1.pid
	@nohup ./$(SERVER_BINARY) --id 2 --peers "1=127.0.0.1:8001,2=127.0.0.1:8002,3=127.0.0.1:8003" --data raft-data > raft-node-2.log 2>&1 & echo $$! > raft-node-2.pid
	@nohup ./$(SERVER_BINARY) --id 3 --peers "1=127.0.0.1:8001,2=127.0.0.1:8002,3=127.0.0.1:8003" --data raft-data > raft-node-3.log 2>&1 & echo $$! > raft-node-3.pid
	@echo " cluster started. Logs in raft-node-*.log"

## stop-cluster: Stop the local cluster.
stop-cluster:
	@echo " stopping cluster..."
	@-if [ -f raft-node-1.pid ]; then kill `cat raft-node-1.pid` && rm raft-node-1.pid; fi
	@-if [ -f raft-node-2.pid ]; then kill `cat raft-node-2.pid` && rm raft-node-2.pid; fi
	@-if [ -f raft-node-3.pid ]; then kill `cat raft-node-3.pid` && rm raft-node-3.pid; fi
	@echo " cluster stopped."

# Code style checks
install-go-imports-reviser:
	@echo "Installing go-imports-reviser..."
	@command -v goimports-reviser >/dev/null 2>&1 || go install github.com/incu6us/goimports-reviser/v3@latest

format: install-go-imports-reviser
	@echo "Fixing import order for all Go files"
	@$(GOIMPORTS_REVISER) \
		-format \
		-company-prefixes "$(COMPANY_PREFIXES)" \
		-project-name "$(PROJECT_NAME)" \
		-imports-order "$(IMPORTS_ORDER)" \
		$(GO_FILES)
	for file in $(GO_FILES); do \
		gofmt -w "$$file"; \
	done

## clean: Remove generated files and clear Go test cache.
clean:
	@echo " cleaning up..."
	@go clean -testcache
	@rm -f coverage.txt unittest.txt $(SERVER_BINARY) $(CLIENT_BINARY) raft-node-*.log raft-node-*.pid
	@rm -rf raft-data
