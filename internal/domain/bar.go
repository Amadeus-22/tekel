package domain

import (
	"math/big"
	"time"
)

// Bar is a one-minute OHLCV trade bar. Prices and volume are exact.
type Bar struct {
	Exchange string
	Symbol   string
	// Start is the UTC minute the bar covers: [Start, Start+1m).
	Start  time.Time
	Open   *big.Rat
	High   *big.Rat
	Low    *big.Rat
	Close  *big.Rat
	Volume *big.Rat
	// Trades is the number of trades aggregated into the bar.
	Trades int
}

// Key returns the stream this bar belongs to.
func (b Bar) Key() StreamKey { return StreamKey{Exchange: b.Exchange, Symbol: b.Symbol} }

// End returns the exclusive end of the bar's minute.
func (b Bar) End() time.Time { return b.Start.Add(time.Minute) }
