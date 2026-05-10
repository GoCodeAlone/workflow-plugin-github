.PHONY: build test install clean

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
