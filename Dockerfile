# syntax=docker/dockerfile:1

FROM --platform=$BUILDPLATFORM node:24-bookworm-slim AS web
WORKDIR /src/web
RUN --mount=type=bind,source=web,target=/src/web,rw \
    --mount=type=cache,target=/root/.npm \
    npm ci && npm run build

FROM --platform=$BUILDPLATFORM golang:1.26-bookworm AS build
WORKDIR /src
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=bind,source=go.mod,target=go.mod \
    --mount=type=bind,source=go.sum,target=go.sum \
    go mod download
ARG TARGETOS TARGETARCH
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=bind,target=.,rw \
    --mount=type=bind,from=web,source=/src/internal/web/ui/dist/app,target=internal/web/ui/dist/app \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags="-s -w" -o /out/mahiron ./cmd/mahiron

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates tzdata curl \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/mahiron /usr/local/bin/mahiron
WORKDIR /app
RUN mkdir -p /app/config /app/db
VOLUME ["/app/config", "/app/db"]
EXPOSE 40772
ENTRYPOINT ["mahiron"]
CMD ["-config-dir", "/app/config"]
