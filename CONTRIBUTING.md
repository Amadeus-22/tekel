# Contributing

Thanks for looking. Issues labelled `good first issue` are scoped so you can finish
them without knowing the whole codebase.

## Run it

```sh
make check    # go vet, gofmt and every test with the race detector; must pass
make run      # needs a .env, copy .env.example
docker compose -f deploy/docker-compose.yml up --build
```

Go 1.24 or newer. No exchange account or API key is needed: every feed is public.

## Where things go

`internal/domain` holds the rules, `internal/app` the use cases and their ports,
`internal/adapter/<name>` one implementation each (exchanges, SQLite, HTTP, LEAN
files). A new exchange is a new package next to `internal/adapter/kraken`, wired in
`cmd/tickguard/main.go`. The full layout and conventions are in
[ENGINEERING.md](ENGINEERING.md).

## Pull requests

- Open an issue first for anything larger than a fix, so we agree on the approach.
- One change per pull request, with a test that fails without it.
- Tests never connect to a real exchange; use the recorded messages and fakes the
  existing adapters use.
- Conventional Commits (`feat(kraken): ...`, `fix(audit): ...`).
- Say in the description how you verified the change.
