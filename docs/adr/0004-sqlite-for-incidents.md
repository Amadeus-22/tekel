# 0004 — SQLite for incidents

Status: accepted · 2026-09-29

> Written when the project was named tickguard. [ADR 0008](0008-rename-to-tekel.md)
> renamed it to Tekel: `TICKGUARD_*` variables are now `TEKEL_*`, `tickguard_*`
> metrics `tekel_*`.

## Context

Incidents are small (a few hundred bytes), written by one process, queried by
exchange, symbol, kind and time. Volume is low in normal operation and bursty
during a bad feed. The MVP runs on one machine (storage beyond one machine is a
non-goal).

## Decision

Store incidents in SQLite through `modernc.org/sqlite`, the pure-Go driver on
the allowed-dependency list:

- no cgo, so the binary is static and the image is `distroless/static`;
- WAL journal mode so API reads do not block pipeline writes;
- one open connection (`SetMaxOpenConns(1)`): SQLite has a single writer anyway;
- times stored as Unix microseconds (`INTEGER`) for cheap range queries;
- indexes on `detected_at` and `(exchange, symbol, kind, detected_at)`, matching
  the `GET /v1/incidents` filters.

## Consequences

- Zero-ops: the database is a file next to the LEAN data, on the same volume.
- Not shared between replicas. Phase 2 (one Deployment per exchange) keeps one
  writer per database; a shared store would move to Postgres (`pgx`, also on
  the allowed list) behind the same `IncidentStore` port.
- The pure-Go driver is slower than the cgo one; at incident volumes this does
  not matter.
