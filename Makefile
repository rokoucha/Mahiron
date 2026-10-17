.PHONY: build generate test test-race verify web-build web-test

TEST_ENV := $(if $(CI),,GOCACHE=/private/tmp/mahiron-gocache)

build:
	go build ./cmd/mahiron
	go build ./cmd/mahiron-agent
	go build ./cmd/mahiron-tools

web-build:
	npm --prefix web install
	npm --prefix web run build

web-test:
	npm --prefix web install
	npm --prefix web test

generate:
	go generate ./internal/web/api
	go tool sqlc generate

test:
	$(TEST_ENV) go test ./...

test-race:
	$(TEST_ENV) go test -race ./internal/agent/... ./internal/job/... ./internal/stream/... ./internal/tuner/... ./internal/util/...

verify: test test-race
