APP     := tickguard
BIN     := bin/$(APP)
IMAGE   ?= $(APP):dev
GOFLAGS ?=

.PHONY: build test test-integration lint check run docker clean

build: ## build bin/tickguard
	CGO_ENABLED=0 go build $(GOFLAGS) -trimpath -o $(BIN) ./cmd/$(APP)

test: ## unit and adapter tests with the race detector
	go test -race ./...

test-integration: ## tests behind the integration build tag
	go test -race -tags integration ./...

lint: ## go vet and gofmt (fails on unformatted files)
	go vet ./...
	@out="$$(gofmt -l .)"; if [ -n "$$out" ]; then echo "gofmt needed:"; echo "$$out"; exit 1; fi

check: lint test ## must pass before any change is done

run: build ## run locally with .env
	@if [ -f .env ]; then set -a; . ./.env; set +a; fi; ./$(BIN)

docker: ## build the container image
	docker build -f deploy/Dockerfile -t $(IMAGE) .

clean:
	rm -rf bin
