.PHONY: build test install clean proto-gen

# Regenerate Go bindings from proto/github/v1/github.proto.
# Requires: protoc + protoc-gen-go
#   brew install protobuf
#   go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
proto-gen:
	protoc \
		--proto_path=proto/github/v1 \
		--go_out=gen \
		--go_opt=paths=source_relative \
		proto/github/v1/github.proto

BINARY_NAME = workflow-plugin-github
PROVIDER_BINARY_NAME = github-runner-provider
INSTALL_DIR ?= data/plugins/$(BINARY_NAME)

build:
	GOWORK=off GOPRIVATE=github.com/GoCodeAlone/* go build -o bin/$(BINARY_NAME) ./cmd/$(BINARY_NAME)
	GOWORK=off GOPRIVATE=github.com/GoCodeAlone/* go build -o bin/$(PROVIDER_BINARY_NAME) ./cmd/$(PROVIDER_BINARY_NAME)

test:
	GOWORK=off GOPRIVATE=github.com/GoCodeAlone/* go test ./... -v -race

install: build
	mkdir -p $(DESTDIR)/$(INSTALL_DIR)
	cp bin/$(BINARY_NAME) $(DESTDIR)/$(INSTALL_DIR)/
	cp plugin.json $(DESTDIR)/$(INSTALL_DIR)/

clean:
	rm -rf bin/
