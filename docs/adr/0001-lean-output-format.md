# 0001 — LEAN minute trade bars as the output format

Status: accepted · 2026-09-29

## Context

tickguard's clean output must be usable without a conversion step. The target
user backtests on QuantConnect LEAN, which reads market data from zipped CSV
files in a fixed directory layout. Getting one detail wrong (time unit, header,
file or entry name) makes LEAN silently skip the data, so the format was
verified against LEAN's own repository before any code was written, not taken
from memory.

## Sources checked (2026-09-29)

1. LEAN crypto data documentation, `Data/crypto/readme.md`:
   <https://github.com/QuantConnect/Lean/blob/master/Data/crypto/readme.md>
   - "Second/Minute files are located in the crypto / market / resolution / symbol folder."
   - "The zip files have the filename: `YYYYMMDD_tickType.zip`. The CSV file contained
     within has the filename: `YYYYMMDD_symbol_resolution_tickType.csv`"
   - Minute trade columns: `Time, Open, High, Low, Close, Volume`, where Time is
     "Milliseconds since midnight"; "All times are in UTC unless noted otherwise."
2. LEAN's sample files, downloaded and unzipped:
   - `Data/crypto/binance/minute/btcbusd/20221213_trade.zip` →
     one entry `20221213_btcbusd_minute_trade.csv`, first line
     `0,17212.12,17216.55,17208.88,17208.88,110.66042`
   - `Data/crypto/coinbase/minute/btcusd/20161007_trade.zip` →
     one entry `20161007_btcusd_minute_trade.csv`, first line
     `0,611.78,611.91,611.78,611.9,1.13872`
3. Market identifiers, `Common/Market.cs`:
   <https://github.com/QuantConnect/Lean/blob/master/Common/Market.cs>
   — `Binance = "binance"`, `Kraken = "kraken"`.

Observed in the samples, beyond the readme:

- no header row; Time is the **start** of the minute (the first bar is `0`);
- numbers are plain decimals with trailing zeros trimmed (`17241.1`, not `17241.10`);
- minutes without trades are **absent** (the 2016 Coinbase file has 1418 lines
  and 20 jumps larger than 60 000 ms), so LEAN expects sparse files;
- the 2022 Binance file uses LF line endings, the 2016 Coinbase file CRLF;
  LEAN reads both.

## Decision

`internal/adapter/leanfs` writes

```
<TICKGUARD_DATA_DIR>/crypto/<market>/minute/<ticker>/<yyyymmdd>_trade.zip
  └── <yyyymmdd>_<ticker>_minute_trade.csv
```

with `market` = the exchange id (`binance`, `kraken`, identical to LEAN's
`Market` constants), `ticker` = the symbol lowercased with non-alphanumerics
removed (`BTC/USD` → `btcusd`), one line per minute that had trades,
`ms-since-midnight-UTC,open,high,low,close,volume`, shortest exact decimal,
LF line endings, no header.

Prices and volumes are exact (`math/big.Rat`) from the exchange's decimal
strings to the CSV; no float64 is involved.

A test (`leanfs_test.go`) writes three bars and compares the CSV byte for byte
with the first three lines of LEAN's Binance sample, kept in `testdata/`.

## Consequences

- A LEAN data folder can point at `TICKGUARD_DATA_DIR` directly.
- Each write rewrites that day's zip via temp file + fsync + rename, so readers
  and crashes never see a partial file. A day is at most 1440 lines, so this
  costs ~100 KB of I/O per symbol per minute.
- Only trade bars are produced. Quote bars need order-book data (a non-goal).
- Kraken tickers follow the `BTC/USD` → `btcusd` rule; LEAN's symbol-properties
  database must know the pair for a backtest to use it, as for any custom data.
