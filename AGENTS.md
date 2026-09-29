# Agent Guide

Instructions for AI coding agents working in this repository. Goal: correct changes
with the least context and tokens spent.

## Read order (stop as soon as you know enough)

1. This file.
2. `docs/ARCHITECTURE.md` → **Package map** section only.
3. The specific files your task touches.

Do not read the README, ADRs or other packages unless the task is about them.

## Finding code

- Search before reading: `grep -rn "Symbol" internal/` or `rg`.
- Read line ranges, not whole files, when a file is over ~150 lines.
- Never open: `go.sum`, `bin/`, `vendor/`, `contracts/lib/`, `contracts/out/`,
  `testdata/` fixtures larger than a screen.
- Do not re-read a file you just edited to "check" it; run the tests instead.

## Making changes

- Smallest diff that solves the task. No drive-by refactors or renames.
- Follow the layer rule: `domain` ← `app` ← `adapter` ← `cmd`.
- No new dependency outside the allowed list in `ENGINEERING.md` without an ADR.
- Adding or removing a package → update the Package map in `docs/ARCHITECTURE.md`
  in the same change. That map is what the next agent reads instead of exploring.

## Verifying

- While iterating: `go test ./internal/<pkg>/...` for the package you touched.
- Before finishing: `make check`. Report failures verbatim; never claim a pass you did not see.

## Reporting back

- List changed files as `path:line — what changed`, one line each.
- Do not paste code that is already in the files.
- State what was not done and why, in one line each.
