# Makefile for the go-raft project

# --- Variables ---
# Get all packages in the project, excluding vendor, example, and mock directories.
# The result is a multi-line string of package paths.
PKGS_TO_TEST := $(shell go list ./... | grep -vE "vendor|example|mock")

# Create a comma-separated list of packages for the -coverpkg flag.
# `paste -sd, -` is a clean way to join lines with a comma.
COVER_PKGS := $(shell echo '$(PKGS_TO_TEST)' | paste -sd, -)

# Define the output binary name (if you have a build target)
BINARY_NAME=go-raft

# --- Targets ---

# Phony targets are ones that don't represent an actual file.
.PHONY: all deps test cover clean help

# The default command, executed when you just run `make`.
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

## test: Run all unit tests with race detector and coverage.
test: deps
	@echo " running tests..."
	@# This command correctly passes the package list to both -coverpkg (comma-separated)
	@# and to the go test command itself (space/newline-separated).
	@go test -race -timeout=2m -v -cover -coverprofile=coverage.txt -coverpkg=$(COVER_PKGS) $(PKGS_TO_TEST)

## cover: Open the HTML coverage report in your browser.
cover: test
	@echo " opening coverage report..."
	@go tool cover -html=coverage.txt

## clean: Remove generated files.
clean:
	@echo " cleaning up..."
	@rm -f coverage.txt unittest.txt $(BINARY_NAME)