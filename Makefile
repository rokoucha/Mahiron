.PHONY: build generate test test-race verify web-build

build:
	go build ./cmd/mahiron

web-build:
	npm --prefix web install
	npm --prefix web run build

generate:
	go generate ./internal/web/api
	go tool sqlc generate

test:
	GOCACHE=/private/tmp/mahiron-gocache go test ./...

test-race:
	GOCACHE=/private/tmp/mahiron-gocache go test -race ./internal/job ./internal/stream ./internal/tuner ./internal/util

verify: test test-race
