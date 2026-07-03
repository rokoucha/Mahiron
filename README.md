# mahiron

Mahiron written in Go.

## Development

```sh
go run ./cmd/mahiron
go build ./cmd/mahiron
go generate ./internal/web/api
go tool sqlc generate
GOCACHE=/private/tmp/mahiron-gocache go test ./...
make test-race
make verify
```

`make verify`, `golangci-lint run`, `web/` tests and build, and a Docker build run in CI on every pull request and push to `main` (see [.github/workflows/ci.yml](.github/workflows/ci.yml)).
