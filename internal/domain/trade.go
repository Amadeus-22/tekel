// Package domain holds tickguard's core types: trades, quality incidents and bars.
// It is pure: no I/O and no imports from other tickguard packages.
package domain

import (
	"fmt"
	"time"
)

// Exchange identifiers. They double as LEAN market names, which is why they are
// lowercase and must match LEAN's Common/Market.cs constants.
const (
	ExchangeBinance = "binance"
	ExchangeKraken  = "kraken"
)

// Side is the side of the taker (aggressor) order.
type Side string

// Taker sides.
const (
	SideBuy  Side = "buy"
	SideSell Side = "sell"
)

// ParseSide converts "buy" or "sell" into a Side.
func ParseSide(s string) (Side, error) {
	switch Side(s) {
	case SideBuy, SideSell:
		return Side(s), nil
	default:
		return "", fmt.Errorf("%w: %q", ErrInvalidSide, s)
	}
}

// StreamKey identifies one trade stream: one symbol on one exchange.
type StreamKey struct {
	Exchange string
	Symbol   string
}

func (k StreamKey) String() string { return k.Exchange + ":" + k.Symbol }

// Trade is one public trade, normalised across exchanges.
type Trade struct {
	Exchange string
	// Symbol is the exchange's own spelling ("BTCUSDT" on Binance, "BTC/USD" on Kraken).
	Symbol string
	// TradeID is the exchange's per-symbol trade sequence number.
	TradeID      int64
	Price        Decimal
	Qty          Decimal
	Side         Side
	ExchangeTime time.Time
	// ReceivedTime is when tickguard read the message off the socket.
	ReceivedTime time.Time
}

// Key returns the stream this trade belongs to.
func (t Trade) Key() StreamKey { return StreamKey{Exchange: t.Exchange, Symbol: t.Symbol} }

// Validate checks the invariants every decoded trade must satisfy.
func (t Trade) Validate() error {
	switch {
	case t.Exchange == "":
		return fmt.Errorf("%w: missing exchange", ErrInvalidTrade)
	case t.Symbol == "":
		return fmt.Errorf("%w: missing symbol", ErrInvalidTrade)
	case t.TradeID < 0:
		return fmt.Errorf("%w: negative trade id %d", ErrInvalidTrade, t.TradeID)
	case t.Price.IsZero():
		return fmt.Errorf("%w: non-positive price %q", ErrInvalidTrade, t.Price)
	case t.Qty.IsZero():
		return fmt.Errorf("%w: non-positive quantity %q", ErrInvalidTrade, t.Qty)
	case t.Side != SideBuy && t.Side != SideSell:
		return fmt.Errorf("%w: side %q", ErrInvalidTrade, t.Side)
	case t.ExchangeTime.IsZero():
		return fmt.Errorf("%w: missing exchange time", ErrInvalidTrade)
	case t.ReceivedTime.IsZero():
		return fmt.Errorf("%w: missing received time", ErrInvalidTrade)
	}
	return nil
}
