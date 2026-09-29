package quality

import "math"

// SpikeRule holds the thresholds of the price_spike check. It is shared by the
// live validators (Stream) and the offline audit of LEAN files.
type SpikeRule struct {
	// Window is how many recent prices feed the rolling median.
	Window int
	// MinSamples is how many prices must be in the window before spikes are checked.
	MinSamples int
	// K is the number of (scaled) MADs a price may deviate from the median.
	K float64
	// MinDeviation is a relative floor (0.01 = 1%) on the allowed deviation.
	MinDeviation float64
}

// SpikeRule extracts the spike thresholds from c.
func (c Config) SpikeRule() SpikeRule {
	return SpikeRule{Window: c.SpikeWindow, MinSamples: c.SpikeMinSamples, K: c.SpikeK, MinDeviation: c.SpikeMinDeviation}
}

// SpikeResult explains one spike check with the numbers that decided it.
type SpikeResult struct {
	Median, MAD, Allowed, Deviation float64
	// N is how many prices were in the window.
	N int
}

// SpikeWindow is a rolling window of prices that flags a price deviating from
// the window's median by more than max(K × 1.4826 × MAD, MinDeviation × median).
// Median and MAD are robust: one spike cannot drag them the way it would drag a
// mean and standard deviation. Not safe for concurrent use.
type SpikeWindow struct {
	rule    SpikeRule
	prices  []float64 // ring buffer, oldest at at once full
	at      int
	scratch []float64
}

// NewSpikeWindow returns an empty window. rule must be valid (see Config.Validate).
func NewSpikeWindow(rule SpikeRule) *SpikeWindow {
	return &SpikeWindow{
		rule:    rule,
		prices:  make([]float64, 0, rule.Window),
		scratch: make([]float64, 0, rule.Window),
	}
}

// Check compares price with the window without adding it. It never flags
// before the window holds MinSamples prices.
func (w *SpikeWindow) Check(price float64) (SpikeResult, bool) {
	if len(w.prices) < w.rule.MinSamples {
		return SpikeResult{N: len(w.prices)}, false
	}
	return w.rule.Evaluate(price, w.prices, w.scratch)
}

// Evaluate applies the rule to price against the reference prices in window,
// which must be non-empty. scratch is reused to avoid allocating; it may be nil.
// Callers decide which prices form the window: the live check uses the previous
// trades, the offline audit the neighbours on both sides.
func (r SpikeRule) Evaluate(price float64, window, scratch []float64) (SpikeResult, bool) {
	median, mad := medianMAD(window, scratch[:0])
	res := SpikeResult{
		Median:    median,
		MAD:       mad,
		Allowed:   math.Max(r.K*madToSigma*mad, r.MinDeviation*median),
		Deviation: math.Abs(price - median),
		N:         len(window),
	}
	return res, res.Deviation > res.Allowed
}

// Add appends price to the window, evicting the oldest price when full.
func (w *SpikeWindow) Add(price float64) {
	if len(w.prices) < w.rule.Window {
		w.prices = append(w.prices, price)
		return
	}
	w.prices[w.at] = price
	w.at = (w.at + 1) % w.rule.Window
}
