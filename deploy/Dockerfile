# syntax=docker/dockerfile:1

# ---- build ------------------------------------------------------------------
FROM golang:1.24-bookworm AS build
WORKDIR /src
ENV CGO_ENABLED=0 GOTOOLCHAIN=local
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -trimpath -ldflags="-s -w" -o /out/tekel ./cmd/tekel \
 && mkdir -p /out/var/lib/tekel/data

# ---- run --------------------------------------------------------------------
# distroless/static: no shell, no package manager, CA certificates for wss://,
# and a non-root user (uid 65532).
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/tekel /usr/local/bin/tekel
COPY --from=build --chown=65532:65532 /out/var/lib/tekel /var/lib/tekel
# The binary listens on 127.0.0.1 by default; in a container that would be
# unreachable, so the image listens on every interface.
ENV TEKEL_HTTP_ADDR=:8080 \
    TEKEL_DATA_DIR=/var/lib/tekel/data \
    TEKEL_DB_PATH=/var/lib/tekel/tekel.db
VOLUME ["/var/lib/tekel"]
EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/tekel"]
