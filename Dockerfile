# syntax=docker/dockerfile:1.7

FROM oven/bun:1.2.23 AS ui-builder
WORKDIR /src/ui

COPY ui/package.json ui/bun.lock ./
RUN bun install --frozen-lockfile

COPY ui/ ./
ENV PATOLAKE_VITE_MINIFY=true \
    PATOLAKE_VITE_REPORT_COMPRESSED=false
RUN bun run build

FROM golang:1.25-bookworm AS go-builder
WORKDIR /src

RUN apt-get update && apt-get install -y --no-install-recommends g++ && rm -rf /var/lib/apt/lists/*

ARG VERSION=dev
ARG COMMIT=none
ARG BUILD_DATE=unknown
ARG TARGETOS=linux
ARG TARGETARCH=amd64

COPY go.mod go.sum ./
RUN go mod download

COPY . .
COPY --from=ui-builder /src/ui/dist ./ui/dist

RUN CGO_ENABLED=1 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags "-s -w -linkmode external -extldflags '-static' -X main.Version=${VERSION} -X main.Commit=${COMMIT} -X main.BuildDate=${BUILD_DATE}" -o /out/patolake .

FROM alpine:3.20 AS runtime
RUN addgroup -S patolake && adduser -S -G patolake patolake \
    && apk add --no-cache ca-certificates tzdata \
    && mkdir -p /app/data \
    && chown -R patolake:patolake /app

WORKDIR /app
COPY --from=go-builder /out/patolake /usr/local/bin/patolake

ENV DATABASE_PATH=/app/data/patolake.db

EXPOSE 3488
VOLUME ["/app/data"]

USER patolake
ENTRYPOINT ["patolake", "server"]
