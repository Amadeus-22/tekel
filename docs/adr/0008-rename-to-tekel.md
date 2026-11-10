# 0008 — Rename tickguard to Tekel

Status: accepted · 2026-11-10

## Context

"tickguard" described the first version: a guard over exchange ticks. Since
0.2.0 the same checks also run over KleverChain blocks, and the offline audit
reads LEAN data nobody streamed. What the project does in every mode is find
what is missing from a sequence and say why. The name also collided with
unrelated projects in search results.

## Decision

Rename the project to **Tekel**, from Daniel 5:27, "thou art weighed in the
balances, and art found wanting": it weighs a sequence and reports what is
missing. Everything that carries the name changes at once, in v0.2.0:

| What | Before | After |
|---|---|---|
| Go module | `github.com/Amadeus-22/tickguard` | `github.com/Amadeus-22/tekel` |
| Command and binary | `cmd/tickguard`, `tickguard` | `cmd/tekel`, `tekel` |
| Environment variables | `TICKGUARD_*` | `TEKEL_*` (same suffixes) |
| Prometheus metrics | `tickguard_*` | `tekel_*` (same suffixes and labels) |
| Default database file | `./tickguard.db` | `./tekel.db` |
| Container image | `ghcr.io/amadeus-22/tickguard` | `ghcr.io/amadeus-22/tekel`, data in `/var/lib/tekel` |

The old variable names are not read as aliases. The project is pre-1.0 with a
single published release; two names for every setting would double the
configuration surface and its tests for a migration that is one `sed` line.

## Consequences

- Upgrading needs the variables renamed, dashboards and alerts pointed at the
  `tekel_` metrics, and an existing `tickguard.db` either renamed or kept with
  `TEKEL_DB_PATH`. README "Upgrading from tickguard" has the one-line table.
- The data layout is unchanged: LEAN files written by tickguard are continued
  by Tekel in the same `TEKEL_DATA_DIR`.
- ADRs 0001–0007 keep the old name: they record decisions as they were made.
- GitHub redirects the old repository URL after the rename; the old image
  `ghcr.io/amadeus-22/tickguard:0.1.0` stays published but gets no updates.
