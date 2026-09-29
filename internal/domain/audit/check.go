package audit

import (
	"fmt"
	"math/big"
	"time"

	"github.com/Amadeus-22/tickguard/internal/domain"
	"github.com/Amadeus-22/tickguard/internal/domain/quality"
)

const day = 24 * time.Hour

// CheckTime reports a row time outside the file's day or not aligned to the
// resolution (a minute bar at 00:00:30, an hour bar at 10:15).
func CheckTime(f File, line int, t time.Time) (Finding, bool) {
	if f.Resolution.Dated() && (t.Before(f.Date) || !t.Before(f.Date.Add(day))) {
		return Finding{Line: line, Time: t, Kind: KindBadTime, Severity: Error,
			Detail: fmt.Sprintf("time %s is outside the file's day %s", stamp(t), f.Date.Format("2006-01-02"))}, true
	}
	if step := f.Resolution.Step(); step > 0 && t.Sub(t.Truncate(step)) != 0 {
		return Finding{Line: line, Time: t, Kind: KindBadTime, Severity: Error,
			Detail: fmt.Sprintf("time %s is not aligned to the %s resolution", stamp(t), f.Resolution)}, true
	}
	return Finding{}, false
}

// CheckOHLC reports a bar whose prices contradict each other: high < low, or
// open or close outside [low, high].
func CheckOHLC(b Bar) (Finding, bool) {
	var problem string
	switch {
	case b.High.Cmp(b.Low) < 0:
		problem = "high < low"
	case b.Open.Cmp(b.Low) < 0 || b.Open.Cmp(b.High) > 0:
		problem = "open outside [low, high]"
	case b.Close.Cmp(b.Low) < 0 || b.Close.Cmp(b.High) > 0:
		problem = "close outside [low, high]"
	default:
		return Finding{}, false
	}
	return Finding{Line: b.Line, Time: b.Time, Kind: KindOHLC, Severity: Error, Detail: fmt.Sprintf(
		"%s (o=%s h=%s l=%s c=%s)", problem, num(b.Open), num(b.High), num(b.Low), num(b.Close))}, true
}

// CheckPrices reports any price that is zero or negative.
func CheckPrices(line int, t time.Time, prices ...*big.Rat) (Finding, bool) {
	for _, p := range prices {
		if p.Sign() <= 0 {
			return Finding{Line: line, Time: t, Kind: KindNonPositive, Severity: Error,
				Detail: fmt.Sprintf("price %s is not positive", num(p))}, true
		}
	}
	return Finding{}, false
}

// CheckVolume reports a negative volume or quantity. Zero is allowed: LEAN's
// own samples contain zero-volume bars.
func CheckVolume(line int, t time.Time, v *big.Rat) (Finding, bool) {
	if v.Sign() < 0 {
		return Finding{Line: line, Time: t, Kind: KindNegativeVolume, Severity: Error,
			Detail: fmt.Sprintf("volume %s is negative", num(v))}, true
	}
	return Finding{}, false
}

// Checker runs the row-by-row checks of one file. Rows are fed in file order;
// only a rolling price window and the previous row are kept, so memory does not
// grow with the file. Not safe for concurrent use.
type Checker struct {
	file   File
	cfg    Config
	spikes centredSpikes

	have     bool
	prevTime time.Time // latest row time seen (never moves backwards)
	prevLine int
}

// NewChecker returns a checker for f. cfg.Spike must be valid.
func NewChecker(f File, cfg Config) *Checker {
	return &Checker{file: f, cfg: cfg, spikes: centredSpikes{rule: cfg.Spike, half: cfg.Spike.Window / 2}}
}

// Bar checks one trade bar.
func (c *Checker) Bar(b Bar) []Finding {
	out := c.sequence(b.Line, b.Time)
	ohlc, badOHLC := CheckOHLC(b)
	out = appendIf(out)(ohlc, badOHLC)
	price, badPrice := CheckPrices(b.Line, b.Time, b.Open, b.High, b.Low, b.Close)
	out = appendIf(out)(price, badPrice)
	out = appendIf(out)(CheckVolume(b.Line, b.Time, b.Volume))
	// A bar that is already an error is kept out of the spike window.
	if !badOHLC && !badPrice {
		out = append(out, c.spikes.push(spikeRow{line: b.Line, t: b.Time, prices: [3]*big.Rat{b.Close, b.High, b.Low}})...)
	}
	return out
}

// Tick checks one trade tick. Equal timestamps are normal for ticks (several
// fills in one millisecond), so only a time going backwards is reported.
func (c *Checker) Tick(t TradeTick) []Finding {
	out := c.sequence(t.Line, t.Time)
	out = appendIf(out)(CheckPrices(t.Line, t.Time, t.Price))
	out = appendIf(out)(CheckVolume(t.Line, t.Time, t.Quantity))
	if _, bad := CheckPrices(t.Line, t.Time, t.Price); !bad {
		out = append(out, c.spikes.push(spikeRow{line: t.Line, t: t.Time, prices: [3]*big.Rat{t.Price}})...)
	}
	return out
}

// Finish reports the spikes still waiting for right-hand neighbours, then the
// run without data from the last row to the end of a dated file's day (the
// whole day when the file had no rows).
func (c *Checker) Finish() []Finding {
	out := c.spikes.flush()
	if !c.file.Resolution.Dated() || !c.gapsApply() {
		return out
	}
	from := c.file.Date
	if c.have {
		from = c.prevTime.Add(c.file.Resolution.Step())
	}
	return appendIf(out)(c.gap(0, from, c.file.Date.Add(day)))
}

// sequence checks the time column and the gap since the previous row.
func (c *Checker) sequence(line int, t time.Time) []Finding {
	var out []Finding
	if f, bad := CheckTime(c.file, line, t); bad {
		return append(out, f) // no ordering or gap verdict on a time that is wrong anyway
	}
	switch {
	case !c.have:
		if c.file.Resolution.Dated() && c.gapsApply() {
			out = appendIf(out)(c.gap(line, c.file.Date, t))
		}
	case t.Equal(c.prevTime) && c.file.Resolution != Tick:
		out = append(out, Finding{Line: line, Time: t, Kind: KindDuplicateTime, Severity: Error,
			Detail: fmt.Sprintf("time %s repeats line %d", stamp(t), c.prevLine)})
	case t.Before(c.prevTime):
		out = append(out, Finding{Line: line, Time: t, Kind: KindOutOfOrder, Severity: Error,
			Detail: fmt.Sprintf("time %s is before line %d (%s)", stamp(t), c.prevLine, stamp(c.prevTime))})
	case c.gapsApply():
		out = appendIf(out)(c.gap(line, c.prevTime.Add(c.file.Resolution.Step()), t))
	}
	if !c.have || t.After(c.prevTime) {
		c.prevTime, c.prevLine = t, line
	}
	c.have = true
	return out
}

// gapsApply: crypto trades around the clock, so time without rows is
// meaningful. US equities have sessions, weekends and holidays that this
// package does not model, so no gap is reported for them.
func (c *Checker) gapsApply() bool { return c.file.SecurityType == Crypto }

// gap reports [from, to) as missing_bars when it is at least cfg.GapMin long.
// LEAN writes no bar for an interval without trades, so a short gap on an
// illiquid pair is normal (info); a long one on a 24/7 market is suspicious (warn).
func (c *Checker) gap(line int, from, to time.Time) (Finding, bool) {
	missing := to.Sub(from)
	if missing <= 0 || missing < c.cfg.GapMin {
		return Finding{}, false
	}
	sev := Info
	if missing >= c.cfg.GapWarn {
		sev = Warn
	}
	what := "no trades"
	if step := c.file.Resolution.Step(); step > 0 {
		what = fmt.Sprintf("%d %s bar(s) missing", missing/step, c.file.Resolution)
	}
	return Finding{Line: line, Time: from, Kind: KindMissingBars, Severity: sev, Detail: fmt.Sprintf(
		"%s from %s to %s (LEAN writes no row for an interval without trades)", what, stamp(from), stamp(to))}, true
}

// spikeRow is a row waiting for its spike verdict. prices[0] (close, or the
// tick price) is the row's reference price; high and low are tested as well.
type spikeRow struct {
	line   int
	t      time.Time
	prices [3]*big.Rat
}

// centredSpikes applies the live price_spike rule (quality.SpikeRule) to a
// centred window: each row is compared with the reference prices of up to half
// rows before and half rows after it (a Hampel filter). Offline, the future is
// known, and using it is what separates a spike (isolated, reverts) from a level
// shift (a news move): after a shift, half the window sits at the new level,
// which widens the MAD instead of flagging every following row. A row's verdict
// is therefore delayed by half rows; memory stays at 2 × half + 1 rows.
type centredSpikes struct {
	rule    quality.SpikeRule
	half    int
	buf     []spikeRow // rows [cursor-half, cursor+half] at most
	cursor  int        // index in buf of the next row to judge
	window  []float64
	scratch []float64
}

func (s *centredSpikes) push(r spikeRow) []Finding {
	s.buf = append(s.buf, r)
	var out []Finding
	for s.cursor < len(s.buf) && len(s.buf)-1-s.cursor >= s.half {
		out = appendIf(out)(s.judge(s.cursor))
		s.cursor++
	}
	if drop := s.cursor - s.half; drop > 0 {
		s.buf = append(s.buf[:0], s.buf[drop:]...)
		s.cursor -= drop
	}
	return out
}

func (s *centredSpikes) flush() []Finding {
	var out []Finding
	for ; s.cursor < len(s.buf); s.cursor++ {
		out = appendIf(out)(s.judge(s.cursor))
	}
	return out
}

func (s *centredSpikes) judge(i int) (Finding, bool) {
	s.window = s.window[:0]
	for j := max(0, i-s.half); j < len(s.buf) && j <= i+s.half; j++ {
		if j != i {
			s.window = append(s.window, ratFloat(s.buf[j].prices[0]))
		}
	}
	if len(s.window) < s.rule.MinSamples {
		return Finding{}, false
	}
	row := s.buf[i]
	for _, p := range row.prices {
		if p == nil {
			continue
		}
		if res, bad := s.rule.Evaluate(ratFloat(p), s.window, s.scratch); bad {
			return Finding{Line: row.line, Time: row.t, Kind: KindPriceSpike, Severity: Warn, Detail: fmt.Sprintf(
				"price %s deviates %.6g from the median %.6g of %d neighbouring rows (allowed %.6g, MAD %.6g)",
				num(p), res.Deviation, res.Median, res.N, res.Allowed, res.MAD)}, true
		}
	}
	return Finding{}, false
}

// CheckDays reports runs of calendar days without a file in a dated crypto
// series. dates must be sorted ascending. Equities are skipped: weekends and
// holidays have no file by design.
func CheckDays(s Series, dates []time.Time) []Finding {
	if s.SecurityType != Crypto || !s.Resolution.Dated() {
		return nil
	}
	var out []Finding
	for i := 1; i < len(dates); i++ {
		missing := int(dates[i].Sub(dates[i-1])/day) - 1
		if missing <= 0 {
			continue
		}
		first, last := dates[i-1].Add(day), dates[i].Add(-day)
		out = append(out, Finding{Time: first, Kind: KindMissingDays, Severity: Warn, Detail: fmt.Sprintf(
			"%d day(s) without a file: %s to %s", missing, first.Format("2006-01-02"), last.Format("2006-01-02"))})
	}
	return out
}

func appendIf(out []Finding) func(Finding, bool) []Finding {
	return func(f Finding, ok bool) []Finding {
		if ok {
			return append(out, f)
		}
		return out
	}
}

func num(r *big.Rat) string { return domain.FormatRat(r) }

func ratFloat(r *big.Rat) float64 {
	f, _ := r.Float64()
	return f
}

// stamp formats t in the series' own time zone, without a zone suffix.
func stamp(t time.Time) string { return t.Format("2006-01-02 15:04:05.999") }
