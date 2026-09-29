# syntax=docker/dockerfile:1

# ---- build ------------------------------------------------------------------
FROM golang:1.24-bookworm AS build
WORKDIR /src
ENV CGO_ENABLED=0 GOTOOLCHAIN=local
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -trimpath -ldflags="-s -w" -o /out/tickguard ./cmd/tickguard \
 && mkdir -p /out/var/lib/tickguard/data

# ---- run --------------------------------------------------------------------
# distroless/static: no shell, no package manager, CA certificates for wss://,
# and a non-root user (uid 65532).
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/tickguard /usr/local/bin/tickguard
COPY --from=build --chown=65532:65532 /out/var/lib/tickguard /var/lib/tickguard
ENV TICKGUARD_HTTP_ADDR=:8080 \
    TICKGUARD_DATA_DIR=/var/lib/tickguard/data \
    TICKGUARD_DB_PATH=/var/lib/tickguard/tickguard.db
VOLUME ["/var/lib/tickguard"]
EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/tickguard"]
