# Audit existing LEAN data

`tekel audit` checks LEAN trade data already on disk, whoever wrote it. It
walks a LEAN data folder (or any sub-folder, or a single zip), recognises each
file from LEAN's path layout, streams the rows and reports what is wrong, with
`file:line`.

```sh
tekel audit <path> [--format table|json] [--fail-on warn|error]
```

- Audited: crypto trade files at tick, second, minute, hour and daily
  resolution; US equity second and minute trade bars (deci-cent prices are
  converted). Quote, open-interest and other files are listed as skipped.
- Checks: `bad_file`, `unparseable_row`, `bad_time` (outside the file's day or
  not aligned to the resolution), `duplicate_timestamp`, `out_of_order`,
  `ohlc_inconsistent`, `non_positive_price`, `negative_volume`, `price_spike`
  (the live median/MAD rule on a centred window, 2% floor), `missing_bars` and
  `missing_days` (crypto only, since it trades 24/7).
- LEAN writes no row for an interval without trades, so a missing minute is
  `info`; an hour or more without a row, and a missing day file, are `warn`.
  Equity gaps are not reported (sessions and holidays are out of scope).
- Exit status: `0` clean, `1` if an issue is at or above `--fail-on`
  (default `error`), `2` for usage or I/O errors. `--format json` gives the full
  report (series and file summaries, every issue, skipped files) for CI.

Layouts, sources and every threshold: [ADR 0005](adr/0005-audit-lean-data.md).

Real output on the sample data shipped in LEAN's repository (`Data/`, commit
`a1470d35`), trimmed:

```text
$ tekel audit Lean/Data/crypto
SERIES                         TZ   FILES  ROWS   FIRST                LAST                 ERROR  WARN  INFO
crypto/binance/hour/btcusdt    UTC  1      120    2018-05-01T00:00:00  2018-05-05T23:00:00  0      0     0
crypto/binance/minute/btcbusd  UTC  1      1440   2022-12-13T00:00:00  2022-12-13T23:59:00  0      0     0
crypto/bitfinex/hour/btcusd    UTC  1      733    2013-10-01T00:00:00  2013-10-31T21:00:00  0      7     0
crypto/coinbase/daily/btcusd   UTC  1      1318   2014-12-01T00:00:00  2018-08-13T00:00:00  0      8     0
crypto/coinbase/minute/btcusd  UTC  9      12423  2016-10-07T00:00:00  2018-04-06T23:59:00  0      4     95
crypto/coinbase/second/btcusd  UTC  3      11520  2016-10-07T00:00:04  2016-10-09T23:59:58  0      0     366
...
ISSUES (error and warn: 23, showing 23)
bitfinex/hour/btcusd_trade.zip:119         warn  missing_bars  2 hour bar(s) missing from 2013-10-06 00:00:00 to 2013-10-06 02:00:00 (LEAN writes no row for an interval without trades)
coinbase/daily/btcusd_trade.zip:10         warn  missing_bars  20 daily bar(s) missing from 2014-12-19 00:00:00 to 2015-01-08 00:00:00 (LEAN writes no row for an interval without trades)
coinbase/minute/btcusd/20180404_trade.zip  warn  missing_bars  432 minute bar(s) missing from 2018-04-04 16:48:00 to 2018-04-05 00:00:00 (LEAN writes no row for an interval without trades)
coinbase/minute/btcusd                     warn  missing_days  328 day(s) without a file: 2016-10-10 to 2017-09-02
...
SKIPPED 17 file(s): 17 × quote data (only trade data is audited)

TOTAL files=27 rows=41873 error=0 warn=23 info=620

$ tekel audit Lean/Data/equity/usa/minute
...
ISSUES (error and warn: 2, showing 2)
aapl/20140609_trade.zip:1   warn  price_spike  price 645.57 deviates 553.07 from the median 92.5 of 50 neighbouring rows (allowed 1.85, MAD 0.06)
bac/20131007_trade.zip:536  warn  price_spike  price 14.12 deviates 0.32 from the median 13.8 of 100 neighbouring rows (allowed 0.276, MAD 0)
```

The samples parse without a single row error. The AAPL finding is real: the
first bar on the day of Apple's 7:1 split still carries the pre-split price as
its open and high. The 2018-04-04 Coinbase files stop at 16:48 UTC. The genuine
2022-12-13 CPI move in the Binance file is *not* flagged: a centred window tells
a level shift from a spike.
