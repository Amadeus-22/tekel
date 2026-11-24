# Contributing

Thanks for looking. Issues labelled `good first issue` are scoped so you can finish
them without knowing the whole codebase.

## Run it

```sh
make check    # go vet, gofmt and every test with the race detector; must pass
make run      # needs a .env, copy .env.example
docker compose -f deploy/docker-compose.yml up --build
docker compose -f deploy/docker-compose.yml --profile observability up --build  # + Prometheus, Grafana
```

Go 1.24 or newer. No exchange account or API key is needed: every feed is public.

## Where things go

`internal/domain` holds the rules, `internal/app` the use cases and their ports,
`internal/adapter/<name>` one implementation each (exchanges, SQLite, HTTP, LEAN
files).

### Adding an exchange or a chain

1. A new package next to `internal/adapter/kraken`: `Decode` for one message, and
   `Run` calling `wsfeed.Run` with your subscribe message and a `Handle` (the
   connection, reconnects and malformed messages are already handled).
2. Tests with recorded messages, using `internal/adapter/feedtest` and
   `wsconn/wstest`. Never connect to the real exchange in tests.
3. One row in `cmd/tekel/sources.go`: its environment variables and how to build it.
4. Document the variables in the README table and `.env.example`.

Nothing else changes: configuration, the pipeline, metrics, storage and the HTTP
API pick the new source up from that row.

The full layout and conventions are in
[ENGINEERING.md](ENGINEERING.md).

### End-to-end tests

`make test-integration` runs the whole service in-process (config, a fake exchange
on `wsconn/wstest`, pipeline, SQLite, HTTP API) and checks `/v1/incidents`.
To add a scenario, copy a test in `cmd/tekel/e2e_test.go`: script what the fake
exchange sends, call `startTekel`, then `waitIncident` for the kind you expect.

## Pull requests

- Open an issue first for anything larger than a fix, so we agree on the approach.
- One change per pull request, with a test that fails without it.
- Tests never connect to a real exchange; use the recorded messages and fakes the
  existing adapters use.
- Conventional Commits (`feat(kraken): ...`, `fix(audit): ...`).
- Say in the description how you verified the change.
