package domain

import (
	"fmt"
	"time"
)

// IncidentKind names a data-quality check. Values are stable: they are stored in
// SQLite, exposed in the API and used as a metric label.
type IncidentKind string

// Incident kinds. See docs/ARCHITECTURE.md for the rule behind each one.
const (
	// KindSequenceGap is a silent gap: the trade ID jumped while connected.
	KindSequenceGap IncidentKind = "sequence_gap"
	KindDuplicate   IncidentKind = "duplicate"
	KindOutOfOrder  IncidentKind = "out_of_order"
	KindClockSkew   IncidentKind = "clock_skew"
	KindPriceSpike  IncidentKind = "price_spike"
	KindStaleStream IncidentKind = "stale_stream"
	// KindDisconnect is a known gap: the connection was down for [Start, End].
	KindDisconnect IncidentKind = "disconnect"
)

// IncidentKinds lists every kind, in documentation order.
var IncidentKinds = []IncidentKind{
	KindSequenceGap, KindDuplicate, KindOutOfOrder, KindClockSkew,
	KindPriceSpike, KindStaleStream, KindDisconnect,
}

// ParseIncidentKind validates s against the known kinds.
func ParseIncidentKind(s string) (IncidentKind, error) {
	for _, k := range IncidentKinds {
		if string(k) == s {
			return k, nil
		}
	}
	return "", fmt.Errorf("%w: %q", ErrInvalidKind, s)
}

// Incident is one detected data-quality problem on one stream.
type Incident struct {
	// ID is assigned by the store; zero until saved.
	ID       int64
	Exchange string
	Symbol   string
	Kind     IncidentKind
	// DetectedAt is when tickguard noticed the problem.
	DetectedAt time.Time
	// Start and End bound the affected window when there is one (gaps, staleness,
	// disconnects). Both are zero for point incidents such as a duplicate.
	Start time.Time
	End   time.Time
	// TradeID is the offending trade, or zero when the incident is not about a trade.
	TradeID int64
	// Detail is a short human-readable explanation with the numbers that matter.
	Detail string
}

// Key returns the stream this incident belongs to.
func (i Incident) Key() StreamKey { return StreamKey{Exchange: i.Exchange, Symbol: i.Symbol} }
