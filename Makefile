# Makefile for the go-raft project

# --- Variables ---
# Get all packages that contain at least one test file. This is more robust.
PKGS_TO_TEST := $(shell go list -f '{{if .TestGoFiles}}{{.ImportPath}}{{end}}' ./...)

# Define the output binary name (if you have a build target)
BINARY_NAME=go-raft

# --- Targets ---

.PHONY: all deps test cover clean help

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

## test: Run all unit tests with race detector and coverage for ALL packages.
test: deps
	@echo " running tests..."
	@go test -race -timeout=2m -v -cover -coverprofile=coverage.txt -coverpkg=./... $(PKGS_TO_TEST)

## cover: Open the HTML coverage report in your browser.
cover: test
	@echo " opening coverage report..."
	@go tool cover -html=coverage.txt

## clean: Remove generated files and clear Go test cache.
clean:
	@echo " cleaning up..."
	@go clean -testcache
	@rm -f coverage.txt unittest.txt $(BINARY_NAME)