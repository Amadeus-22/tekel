package bars

import (
	"errors"
	"math/big"
	"testing"
	"time"

	"github.com/Amadeus-22/tickguard/internal/domain"
)

var (
	key = domain.StreamKey{Exchange: domain.ExchangeKraken, Symbol: "BTC/USD"}
	day = time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
)

func trade(id int64, at time.Duration, price, qty string) domain.Trade {
	return domain.Trade{
		Exchange: key.Exchange, Symbol: key.Symbol, TradeID: id,
		Price: domain.MustDecimal(price), Qty: domain.MustDecimal(qty), Side: domain.SideSell,
		ExchangeTime: day.Add(at), ReceivedTime: day.Add(at),
	}
}

type ohlcv struct{ o, h, l, c, v string }

func assertBar(t *testing.T, b domain.Bar, start time.Duration, want ohlcv, trades int) {
	t.Helper()
	got := ohlcv{
		domain.FormatRat(b.Open), domain.FormatRat(b.High), domain.FormatRat(b.Low),
		domain.FormatRat(b.Close), domain.FormatRat(b.Volume),
	}
	if got != want || b.Trades != trades || !b.Start.Equal(day.Add(start)) || b.Key() != key {
		t.Errorf("bar = %v %+v trades=%d key=%v, want %v %+v trades=%d",
			b.Start, got, b.Trades, b.Key(), day.Add(start), want, trades)
	}
}

func TestAggregatorBuildsOHLCV(t *testing.T) {
	a := NewAggregator(key)
	steps := []domain.Trade{
		trade(1, 5*time.Second, "100.10", "0.5"),
		trade(2, 20*time.Second, "101", "0.25"),
		trade(3, 40*time.Second, "99.5", "1"),
		trade(4, 59*time.Second+999*time.Millisecond, "100.2", "0.125"),
	}
	for _, tr := range steps {
		if _, ok, err := a.Add(tr); ok || err != nil {
			t.Fatalf("Add(%d) = ok %v, err %v; want bar still open", tr.TradeID, ok, err)
		}
	}
	b, ok, err := a.Add(trade(5, time.Minute, "100", "1"))
	if err != nil || !ok {
		t.Fatalf("minute rollover: ok %v, err %v", ok, err)
	}
	assertBar(t, b, 0, ohlcv{"100.1", "101", "99.5", "100.2", "1.875"}, 4)
}

func TestAggregatorSkipsEmptyMinutes(t *testing.T) {
	a := NewAggregator(key)
	a.Add(trade(1, 10*time.Second, "1", "1"))
	b, ok, _ := a.Add(trade(2, 5*time.Minute, "2", "1"))
	if !ok {
		t.Fatal("expected the first bar")
	}
	assertBar(t, b, 0, ohlcv{"1", "1", "1", "1", "1"}, 1)
	b, ok = a.Flush()
	if !ok {
		t.Fatal("expected the second bar on Flush")
	}
	assertBar(t, b, 5*time.Minute, ohlcv{"2", "2", "2", "2", "1"}, 1)
}

func TestAggregatorRejectsLateTrades(t *testing.T) {
	tests := []struct {
		name string
		prep func(*Aggregator)
		late domain.Trade
	}{
		{
			name: "earlier than the open bar",
			prep: func(a *Aggregator) { a.Add(trade(1, 2*time.Minute, "1", "1")) },
			late: trade(2, time.Minute, "1", "1"),
		},
		{
			name: "minute already emitted",
			prep: func(a *Aggregator) {
				a.Add(trade(1, 0, "1", "1"))
				a.Add(trade(2, time.Minute, "1", "1")) // emits minute 0
			},
			late: trade(3, 30*time.Second, "1", "1"),
		},
		{
			name: "minute already flushed with no open bar",
			prep: func(a *Aggregator) {
				a.Add(trade(1, 0, "1", "1"))
				a.Flush()
			},
			late: trade(2, 59*time.Second, "1", "1"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := NewAggregator(key)
			tt.prep(a)
			if _, _, err := a.Add(tt.late); !errors.Is(err, domain.ErrLateTrade) {
				t.Errorf("Add(late) error = %v, want ErrLateTrade", err)
			}
		})
	}
}

func TestFlushBefore(t *testing.T) {
	a := NewAggregator(key)
	if _, ok := a.FlushBefore(day.Add(time.Hour)); ok {
		t.Fatal("empty aggregator flushed a bar")
	}
	a.Add(trade(1, 10*time.Second, "1", "1"))
	if _, ok := a.FlushBefore(day.Add(59 * time.Second)); ok {
		t.Error("bar flushed before its minute ended")
	}
	if _, ok := a.FlushBefore(day.Add(time.Minute)); !ok {
		t.Error("bar not flushed once its minute ended")
	}
	if _, ok := a.Flush(); ok {
		t.Error("Flush after FlushBefore should have nothing left")
	}
}

func TestAggregatorDoesNotAliasTradeValues(t *testing.T) {
	a := NewAggregator(key)
	a.Add(trade(1, 0, "5", "1"))
	a.Add(trade(2, time.Second, "7", "1")) // sets High and Close from one trade
	b, _ := a.Flush()
	b.Close.SetInt64(0)
	if domain.FormatRat(b.High) != "7" {
		t.Errorf("High = %s after mutating Close; fields alias", domain.FormatRat(b.High))
	}
}

func TestMerge(t *testing.T) {
	r := func(s string) *big.Rat { return domain.MustDecimal(s).Rat() }
	earlier := domain.Bar{Exchange: key.Exchange, Symbol: key.Symbol, Start: day,
		Open: r("10"), High: r("12"), Low: r("9"), Close: r("11"), Volume: r("1.5"), Trades: 3}
	later := domain.Bar{Exchange: key.Exchange, Symbol: key.Symbol, Start: day,
		Open: r("11"), High: r("11.5"), Low: r("8"), Close: r("10.5"), Volume: r("0.5"), Trades: 2}

	got, err := Merge(earlier, later)
	if err != nil {
		t.Fatal(err)
	}
	assertBar(t, got, 0, ohlcv{"10", "12", "8", "10.5", "2"}, 5)

	// Swapped roles exercise the other branch of max/min.
	swapped, _ := Merge(later, earlier)
	assertBar(t, swapped, 0, ohlcv{"11", "12", "8", "11", "2"}, 5)

	got.Open.SetInt64(0)
	if domain.FormatRat(earlier.Open) != "10" {
		t.Error("Merge aliased its input")
	}

	other := later
	other.Start = day.Add(time.Minute)
	if _, err := Merge(earlier, other); err == nil {
		t.Error("merging different minutes must fail")
	}
	other = later
	other.Symbol = "ETH/USD"
	if _, err := Merge(earlier, other); err == nil {
		t.Error("merging different streams must fail")
	}
}
