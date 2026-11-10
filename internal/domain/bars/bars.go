// Package bars aggregates trades into one-minute OHLCV bars. It is pure.
package bars

import (
	"fmt"
	"math/big"
	"time"

	"github.com/Amadeus-22/tekel/internal/domain"
)

// Aggregator builds minute bars for one stream. Trades must arrive roughly in
// exchange-time order: a trade for a minute that was already emitted is rejected
// with domain.ErrLateTrade rather than silently rewriting history. Minutes with
// no trades produce no bar, matching LEAN's sparse minute files.
//
// It is not safe for concurrent use.
type Aggregator struct {
	key     domain.StreamKey
	cur     *domain.Bar
	emitted time.Time // start of the newest emitted bar; zero if none
}

// NewAggregator returns an aggregator for one stream.
func NewAggregator(key domain.StreamKey) *Aggregator {
	return &Aggregator{key: key}
}

// Add folds t into the current bar. When t opens a new minute, the previous bar
// is complete and returned with ok = true.
func (a *Aggregator) Add(t domain.Trade) (done domain.Bar, ok bool, err error) {
	minute := t.ExchangeTime.UTC().Truncate(time.Minute)
	if !a.emitted.IsZero() && !minute.After(a.emitted) {
		return domain.Bar{}, false, fmt.Errorf("%w: trade %d at %s, bar %s already written",
			domain.ErrLateTrade, t.TradeID, t.ExchangeTime.UTC().Format(time.RFC3339Nano), minute.Format(time.RFC3339))
	}
	if a.cur != nil {
		switch {
		case minute.Equal(a.cur.Start):
			apply(a.cur, t)
			return domain.Bar{}, false, nil
		case minute.Before(a.cur.Start):
			return domain.Bar{}, false, fmt.Errorf("%w: trade %d at %s, current bar is %s",
				domain.ErrLateTrade, t.TradeID, t.ExchangeTime.UTC().Format(time.RFC3339Nano), a.cur.Start.Format(time.RFC3339))
		default:
			done, ok = a.take()
		}
	}
	a.cur = open(a.key, minute, t)
	return done, ok, nil
}

// FlushBefore emits the current bar if its minute ended at or before cutoff.
// The pipeline calls it on a timer so that a quiet market still gets its last
// bar written, with cutoff = now − grace to leave room for slightly late trades.
func (a *Aggregator) FlushBefore(cutoff time.Time) (domain.Bar, bool) {
	if a.cur == nil || a.cur.End().After(cutoff) {
		return domain.Bar{}, false
	}
	return a.take()
}

// Flush emits the current, possibly partial, bar. Used at shutdown; a restart
// within the same minute is reconciled by Merge.
func (a *Aggregator) Flush() (domain.Bar, bool) {
	if a.cur == nil {
		return domain.Bar{}, false
	}
	return a.take()
}

func (a *Aggregator) take() (domain.Bar, bool) {
	b := *a.cur
	a.emitted = b.Start
	a.cur = nil
	return b, true
}

func open(key domain.StreamKey, minute time.Time, t domain.Trade) *domain.Bar {
	return &domain.Bar{
		Exchange: key.Exchange, Symbol: key.Symbol, Start: minute,
		Open: t.Price.Rat(), High: t.Price.Rat(), Low: t.Price.Rat(), Close: t.Price.Rat(),
		Volume: t.Qty.Rat(), Trades: 1,
	}
}

func apply(b *domain.Bar, t domain.Trade) {
	// Every field gets its own copy so that no two fields of a bar alias.
	if t.Price.Rat().Cmp(b.High) > 0 {
		b.High = t.Price.Rat()
	}
	if t.Price.Rat().Cmp(b.Low) < 0 {
		b.Low = t.Price.Rat()
	}
	b.Close = t.Price.Rat()
	b.Volume = new(big.Rat).Add(b.Volume, t.Qty.Rat())
	b.Trades++
}

// Merge combines two bars of the same stream and minute, where earlier holds the
// trades that came first (e.g. a partial bar written before a restart). Open comes
// from earlier, Close from later; High, Low, Volume and Trades combine.
func Merge(earlier, later domain.Bar) (domain.Bar, error) {
	if earlier.Key() != later.Key() || !earlier.Start.Equal(later.Start) {
		return domain.Bar{}, fmt.Errorf("merge bars: %s@%s and %s@%s are different bars",
			earlier.Key(), earlier.Start.Format(time.RFC3339), later.Key(), later.Start.Format(time.RFC3339))
	}
	out := earlier
	out.Open = new(big.Rat).Set(earlier.Open)
	out.High = maxRat(earlier.High, later.High)
	out.Low = minRat(earlier.Low, later.Low)
	out.Close = new(big.Rat).Set(later.Close)
	out.Volume = new(big.Rat).Add(earlier.Volume, later.Volume)
	out.Trades = earlier.Trades + later.Trades
	return out, nil
}

func maxRat(a, b *big.Rat) *big.Rat {
	if a.Cmp(b) >= 0 {
		return new(big.Rat).Set(a)
	}
	return new(big.Rat).Set(b)
}

func minRat(a, b *big.Rat) *big.Rat {
	if a.Cmp(b) <= 0 {
		return new(big.Rat).Set(a)
	}
	return new(big.Rat).Set(b)
}
