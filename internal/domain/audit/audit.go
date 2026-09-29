// Package audit checks the quality of market data already stored in QuantConnect
// LEAN's on-disk format. It is pure: it knows LEAN's path layout and CSV rows,
// and turns rows into findings, but reads no files. See
// docs/adr/0005-audit-lean-data.md for the layouts and their sources.
package audit

import (
	"errors"
	"fmt"
	"time"

	"github.com/Amadeus-22/tickguard/internal/domain/quality"
)

// ErrUnsupported marks a path that is not a LEAN trade file this package audits.
var ErrUnsupported = errors.New("unsupported LEAN file")

// Security types audited.
const (
	Crypto = "crypto"
	Equity = "equity"
)

// Resolution is a LEAN data resolution folder name.
type Resolution string

// Resolutions audited.
const (
	Tick   Resolution = "tick"
	Second Resolution = "second"
	Minute Resolution = "minute"
	Hour   Resolution = "hour"
	Daily  Resolution = "daily"
)

// Step is the bar length, or zero for ticks.
func (r Resolution) Step() time.Duration {
	switch r {
	case Second:
		return time.Second
	case Minute:
		return time.Minute
	case Hour:
		return time.Hour
	case Daily:
		return 24 * time.Hour
	}
	return 0
}

// Dated reports whether files hold one day each (tick, second, minute) rather
// than the whole history of a symbol (hour, daily).
func (r Resolution) Dated() bool { return r == Tick || r == Second || r == Minute }

// Series is every file of one symbol at one resolution in one market.
type Series struct {
	SecurityType string
	Market       string
	Resolution   Resolution
	Symbol       string
}

func (s Series) String() string {
	return s.SecurityType + "/" + s.Market + "/" + string(s.Resolution) + "/" + s.Symbol
}

// TimeZone is the zone LEAN stores the series' times in: UTC for crypto,
// New York for US equities (Data/crypto/readme.md, Data/equity/readme.md).
func (s Series) TimeZone() string {
	if s.SecurityType == Equity {
		return "America/New_York"
	}
	return "UTC"
}

// File is one LEAN trade data file, identified by its path.
type File struct {
	Series
	// Date is the day a dated file covers (midnight, data time zone); zero for
	// hour and daily files, which hold a symbol's whole history.
	Date time.Time
}

// Severity ranks a finding.
type Severity string

// Severities, from least to most serious.
const (
	Info  Severity = "info"
	Warn  Severity = "warn"
	Error Severity = "error"
)

// Rank orders severities: info 1, warn 2, error 3; unknown 0.
func (s Severity) Rank() int {
	switch s {
	case Info:
		return 1
	case Warn:
		return 2
	case Error:
		return 3
	}
	return 0
}

// ParseSeverity validates s.
func ParseSeverity(s string) (Severity, error) {
	if v := Severity(s); v.Rank() > 0 {
		return v, nil
	}
	return "", fmt.Errorf("invalid severity %q (want info, warn or error)", s)
}

// Kind names an audit check. Values are stable: they appear in JSON output.
type Kind string

// Audit checks. See docs/adr/0005-audit-lean-data.md for each rule.
const (
	KindBadFile        Kind = "bad_file"
	KindUnparseableRow Kind = "unparseable_row"
	KindBadTime        Kind = "bad_time"
	KindDuplicateTime  Kind = "duplicate_timestamp"
	KindOutOfOrder     Kind = "out_of_order"
	KindOHLC           Kind = "ohlc_inconsistent"
	KindNonPositive    Kind = "non_positive_price"
	KindNegativeVolume Kind = "negative_volume"
	KindPriceSpike     Kind = "price_spike"
	KindMissingBars    Kind = "missing_bars"
	KindMissingDays    Kind = "missing_days"
)

// Finding is one problem found in a file. Line is 1-based, or 0 when the
// finding is about the file or series rather than a row.
type Finding struct {
	Line     int
	Time     time.Time
	Kind     Kind
	Severity Severity
	Detail   string
}

// Config holds the audit thresholds.
type Config struct {
	// Spike is the rolling median/MAD rule shared with the live validators.
	Spike quality.SpikeRule
	// GapMin is the shortest run without data reported as missing_bars (info).
	GapMin time.Duration
	// GapWarn is the run without data from which missing_bars is a warning.
	GapWarn time.Duration
}

// DefaultConfig uses the live spike thresholds with a 2% deviation floor instead
// of 0.5%: a minute bar sums a minute of trades, and on LEAN's own samples a 0.5%
// floor flags ordinary 1% dips in calm markets (MAD of a few dollars). Every
// missing minute is reported as info, a run of an hour or more as a warning.
func DefaultConfig() Config {
	spike := quality.DefaultConfig().SpikeRule()
	spike.MinDeviation = 0.02
	return Config{Spike: spike, GapMin: time.Minute, GapWarn: time.Hour}
}
