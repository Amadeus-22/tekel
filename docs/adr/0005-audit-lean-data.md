# 0005 — `tickguard audit`: quality checks over existing LEAN data

Status: accepted · 2026-09-29

> Written when the project was named tickguard. [ADR 0008](0008-rename-to-tekel.md)
> renamed it to Tekel: `TICKGUARD_*` variables are now `TEKEL_*`, `tickguard_*`
> metrics `tekel_*`.

## Context

Every QuantConnect LEAN user backtests on files they did not write: downloaded,
bought, converted or produced by a ToolBox downloader. The live validators only
see data tickguard collects itself. The same checks, run over LEAN's on-disk
format, help the whole LEAN community, and need no exchange connection.

A wrong reading of the layout would produce false findings, so every layout was
checked against LEAN's repository (local clone at commit `a1470d35`, 2026-09-29)
rather than memory.

## Sources checked (2026-09-29)

1. `Data/crypto/readme.md`
   (<https://github.com/QuantConnect/Lean/blob/master/Data/crypto/readme.md>):
   - second/minute: `crypto/<market>/<resolution>/<symbol>/YYYYMMDD_tickType.zip`,
     entry `YYYYMMDD_symbol_resolution_tickType.csv`, rows
     `Time(ms since midnight),Open,High,Low,Close,Volume`;
   - hour/daily: `crypto/<market>/<resolution>/symbol_tickType.zip`, entry
     `symbol.csv`, Time formatted `YYYYMMDD HH:mm`;
   - tick: `crypto/<market>/tick/<symbol>/YYYYMMDD_trade.zip`, rows
     `Time(ms since midnight),Last Price,Quantity`;
   - "All times are in UTC"; "compressed zip files, each containing a single CSV file".
2. `Data/equity/readme.md`
   (<https://github.com/QuantConnect/Lean/blob/master/Data/equity/readme.md>):
   minute/second `equity/usa/<resolution>/<ticker>/YYYYMMDD_trade.zip`, same
   six columns, prices in **deci-cents** ("Divide prices by 10,000"), times in
   the New York time zone. Hour/daily equity files are `<ticker>.zip`.
3. `Common/Util/LeanData.cs` (`GenerateZipFileName`, `GenerateZipEntryName`,
   `GenerateRelativeZipFileDirectory`) — confirms the crypto paths above and that
   crypto tick entries are `YYYYMMDD_symbol_tick_trade.csv`.
4. `Common/Data/Market/Tick.cs` (crypto branch of `Reader`): trade ticks are
   `time, price, quantity` with an optional fourth `suspicious` flag; time is
   decimal milliseconds (`10000 * value` ticks), so fractions are legal.
5. The sample files themselves (`tickguard audit` over `Data/`): 27 crypto and
   41 equity trade zips parse without a single row error. The Bitfinex hour file
   uses CRLF, the others LF. LEAN ships **no crypto tick sample**, so the tick
   parser is tested with rows derived from the second-resolution sample.

## Decision

`tickguard audit <path>` walks a directory (any level of a LEAN data folder) or
one zip, recognises each file by the end of its path, streams the single CSV
entry row by row and reports findings. Audited: crypto tick, second, minute,
hour and daily trade files; equity second and minute trade files. Skipped with a
reason: quote and open-interest files, equity ticks (another column set), equity
hour/daily, other security types.

| Kind | Severity | Rule |
|---|---|---|
| `bad_file` | error | zip unreadable or not exactly one entry |
| `unparseable_row` | error | wrong field count, non-numeric value, bad time text |
| `bad_time` | error | time outside the file's day, or not aligned to the resolution |
| `duplicate_timestamp` | error | a bar time repeats (ticks may share a millisecond) |
| `out_of_order` | error | a time earlier than a previous row |
| `ohlc_inconsistent` | error | high < low, or open/close outside [low, high] |
| `non_positive_price` | error | any price ≤ 0 |
| `negative_volume` | error | volume/quantity < 0 (zero is allowed; LEAN's samples have it) |
| `price_spike` | warn | median/MAD rule of the live check, on a centred window (below) |
| `missing_bars` | info / warn | crypto only: run without rows inside a file (and from midnight / to the end of the day) — info from 1 minute, warn from 1 hour |
| `missing_days` | warn | crypto only: calendar days without a file between the first and last file of a series |

Choices a reviewer might question:

- **Gaps are info until an hour.** LEAN writes no row for an interval without
  trades (ADR 0001: the 2016 Coinbase minute file has 20 such jumps). On a 24/7
  market a missing minute on an illiquid pair is normal, so it is `info`; an hour
  without a single trade on a listed pair is more likely lost data, so `warn`.
  Equities get no gap or day check: sessions, weekends and holidays would need
  LEAN's market-hours database, which is out of scope.
- **Centred spike window.** The live check can only look back. Offline, a row
  is compared with up to 50 rows before and 50 after it (a Hampel filter),
  reusing `quality.SpikeRule.Evaluate`. A look-back window flagged 48 minutes of
  the genuine 2022-12-13 CPI move in LEAN's Binance sample; the centred window
  flags none, because after a level shift half the window sits at the new level.
- **2% deviation floor** instead of the live 0.5%: on LEAN's 2018-04-06 Coinbase
  samples a 0.5% floor flagged ordinary 1% dips while MAD was a few dollars.
  With 2% the crypto samples are spike-free, and the two equity spikes left are
  real: AAPL's first bar on its 2014-06-09 7:1 split day opens at the pre-split
  645.57, and BAC prints 14.12 amid 13.80 after hours on 2013-10-07.
- **High and low are tested too**, against the window of closes, because a bad
  print often shows only as a wick (the AAPL case above).
- Exit status: 0 clean, 1 when an issue is at or above `--fail-on` (default
  `error`), 2 for usage or I/O errors — so CI can gate on it.

## Consequences

- Memory is bounded by the number of files and 101 buffered rows, not rows:
  LEAN's whole `Data/` folder (310 903 audited rows) takes about 7 s.
- The checks live in `internal/domain/audit` (pure), orchestration in
  `app.Audit`, file access in `leanfs.Source`, rendering in `adapter/auditreport`.
- Findings on hour/daily files are placed by line in the one history file.
