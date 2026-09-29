# Engineering Standards

Shared by `tickguard`, `rpcgate`, `chainwatch` and `costbasis-br`. Each repository
carries a copy; this file is the source of truth.

## Principles

1. **Solve a named problem.** Every README opens with the problem, who has it and
   what it costs them. Features that do not serve that problem wait.
2. **Boring, explicit code.** Standard library first. No framework, no DI container,
   no generic repository layer. An interface exists only where there are two
   implementations or a test needs a fake.
3. **Failure is a feature.** Timeouts on every network call, bounded retries with
   jittered backoff, graceful shutdown, and errors that say what failed and why.
4. **Observable by default.** Structured logs and Prometheus metrics ship with the
   first version, not after the first incident.
5. **Decisions are written down.** Anything a reviewer might question gets an ADR.

## Layout

```
cmd/<app>/main.go        wiring only: config, dependencies, start, shutdown
internal/domain/         pure types and rules; no I/O, no imports from other layers
internal/app/            use cases; defines the ports (interfaces) it needs
internal/adapter/<name>/ implementations of ports: http, postgres, sqlite, exchange, rpc…
internal/platform/       config, logging, metrics, shutdown helpers
deploy/                  Dockerfile, docker-compose.yml (k8s/helm arrive in phase 2)
docs/ARCHITECTURE.md     problem, scope, diagram, package map, failure modes
docs/adr/NNNN-title.md   one decision per file
```

Dependency rule: `domain` ← `app` ← `adapter` ← `cmd`. Never the other way.

## Go conventions

- Go 1.24.x (`go.mod` declares `go 1.24` or `go 1.24.0` plus `toolchain go1.24.7`).
  Pick dependency versions that still support Go 1.24; upgrading Go is a deliberate ADR.
- Module path: `github.com/Amadeus-22/<repo>`.
- `context.Context` is the first parameter of anything that does I/O.
- Errors: wrap with `fmt.Errorf("verb noun: %w", err)`; sentinel errors in `domain`;
  never log and return the same error.
- Logging: `log/slog`, JSON handler, one logger passed down; fields in snake_case.
- Config: process settings from environment variables (12-factor); structured data that
  env vars cannot express (e.g. an upstream list) from a YAML file decoded strictly
  (unknown keys are errors). Parsed once in `internal/platform/config`, validated at
  startup, documented in the README table.
- Money and quantities: never `float64` where exactness matters (use `math/big.Rat`
  or scaled integers). `float64` is fine for statistics and latency.
- Concurrency: every goroutine has an owner that waits for it on shutdown
  (`errgroup` or `sync.WaitGroup`). No goroutine leaks in tests.

## Allowed dependencies

`github.com/prometheus/client_golang`, `github.com/gorilla/websocket`,
`github.com/jackc/pgx/v5`, `modernc.org/sqlite` (pure Go, no cgo),
`golang.org/x/sync`, `gopkg.in/yaml.v3`. Anything else needs an ADR.

## HTTP APIs

- Versioned under `/v1`. JSON in and out. Errors as
  `{"error":{"code":"snake_case","message":"..."}}` with a correct status code.
- Every service exposes `GET /healthz` (process alive), `GET /readyz`
  (dependencies reachable) and `GET /metrics`.
- Server timeouts set explicitly (read header, read, write, idle).

## Testing

- `domain`: table-driven unit tests, no mocks needed. Aim for full coverage here.
- `app`: tests with hand-written fakes of the ports.
- `adapter`: `httptest` servers, real SQLite in a temp dir; Postgres tests behind
  the `integration` build tag, run with `make test-integration`.
- Race detector on in CI (`go test -race ./...`).
- A bug fix lands with the test that would have caught it.

## Tooling

Every repository has the same `Makefile` targets:

| Target | Does |
|---|---|
| `make build` | builds `bin/<app>` |
| `make test` | `go test -race ./...` |
| `make test-integration` | tests with `-tags integration` (needs `docker compose up`) |
| `make lint` | `go vet ./...` and `gofmt -l` (fails on output) |
| `make check` | lint + test; must pass before any change is done |
| `make run` | runs locally with `.env` |
| `make docker` | builds the image |

CI (`.github/workflows/ci.yml`): `make check` on push and pull request.

## Documentation

README order: **Problem → What it does → Architecture (Mermaid diagram) → Quick start →
Configuration → API → Metrics → Design decisions (links to ADRs) → Roadmap**.
No badges that are not backed by a real check.

## Git

Conventional Commits (`feat:`, `fix:`, `docs:`, `test:`, `refactor:`, `chore:`).
One logical change per commit.
