package domain

import (
	"errors"
	"math/big"
	"testing"
	"time"
)

func TestParseDecimal(t *testing.T) {
	tests := []struct {
		in      string
		want    string // exact value as FormatRat renders it
		wantErr bool
	}{
		{in: "0.00123000", want: "0.00123"},
		{in: "17212.12000000", want: "17212.12"},
		{in: "100", want: "100"},
		{in: "6.4e-05", want: "0.000064"},
		{in: "1E+2", want: "100"},
		{in: "0", want: "0"},
		{in: "", wantErr: true},
		{in: "-1", wantErr: true},
		{in: "+1", wantErr: true},
		{in: "1/3", wantErr: true},
		{in: "0x10", wantErr: true},
		{in: "NaN", wantErr: true},
		{in: "abc", wantErr: true},
		{in: "1e", wantErr: true},
		{in: "1.2.3", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			d, err := ParseDecimal(tt.in)
			if tt.wantErr {
				if !errors.Is(err, ErrInvalidDecimal) {
					t.Fatalf("ParseDecimal(%q) error = %v, want ErrInvalidDecimal", tt.in, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseDecimal(%q) unexpected error: %v", tt.in, err)
			}
			if got := FormatRat(d.Rat()); got != tt.want {
				t.Errorf("value = %s, want %s", got, tt.want)
			}
			if d.String() != tt.in {
				t.Errorf("String() = %q, want original %q", d.String(), tt.in)
			}
		})
	}
}

func TestDecimalAccessors(t *testing.T) {
	d := MustDecimal("2.5")
	if d.Float64() != 2.5 {
		t.Errorf("Float64() = %v, want 2.5", d.Float64())
	}
	if d.IsZero() {
		t.Error("IsZero() = true for 2.5")
	}
	r := d.Rat()
	r.SetInt64(9) // mutating the copy must not change d
	if d.Float64() != 2.5 {
		t.Error("Rat() returned an alias, not a copy")
	}

	var zero Decimal
	if !zero.IsZero() || zero.Float64() != 0 || zero.Rat().Sign() != 0 {
		t.Error("zero Decimal should behave as 0")
	}
	if !MustDecimal("0.000").IsZero() {
		t.Error("0.000 should be zero")
	}
}

func TestMustDecimalPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("MustDecimal(bad) did not panic")
		}
	}()
	MustDecimal("bad")
}

func TestFormatRat(t *testing.T) {
	tests := []struct {
		name string
		in   *big.Rat
		want string
	}{
		{"integer", big.NewRat(5, 1), "5"},
		{"zero", new(big.Rat), "0"},
		{"one decimal", big.NewRat(172411, 10), "17241.1"},
		{"small", big.NewRat(12, 100000), "0.00012"},
		{"sum of decimals", new(big.Rat).Add(big.NewRat(1, 10), big.NewRat(2, 10)), "0.3"},
		{"non-terminating rounds", big.NewRat(1, 3), "0.333333333333333333333333333333"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := FormatRat(tt.in); got != tt.want {
				t.Errorf("FormatRat(%v) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

func TestParseSide(t *testing.T) {
	tests := []struct {
		in      string
		want    Side
		wantErr bool
	}{
		{"buy", SideBuy, false},
		{"sell", SideSell, false},
		{"BUY", "", true},
		{"", "", true},
	}
	for _, tt := range tests {
		got, err := ParseSide(tt.in)
		if (err != nil) != tt.wantErr || got != tt.want {
			t.Errorf("ParseSide(%q) = %q, %v; want %q, err=%v", tt.in, got, err, tt.want, tt.wantErr)
		}
		if err != nil && !errors.Is(err, ErrInvalidSide) {
			t.Errorf("ParseSide(%q) error %v is not ErrInvalidSide", tt.in, err)
		}
	}
}

func TestParseIncidentKind(t *testing.T) {
	for _, k := range IncidentKinds {
		got, err := ParseIncidentKind(string(k))
		if err != nil || got != k {
			t.Errorf("ParseIncidentKind(%q) = %q, %v", k, got, err)
		}
	}
	if _, err := ParseIncidentKind("gap"); !errors.Is(err, ErrInvalidKind) {
		t.Errorf("ParseIncidentKind(gap) error = %v, want ErrInvalidKind", err)
	}
}

func validTrade() Trade {
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	return Trade{
		Exchange: ExchangeBinance, Symbol: "BTCUSDT", TradeID: 1,
		Price: MustDecimal("100"), Qty: MustDecimal("0.5"), Side: SideBuy,
		ExchangeTime: now, ReceivedTime: now,
	}
}

func TestTradeValidate(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Trade)
		ok     bool
	}{
		{"valid", func(*Trade) {}, true},
		{"no exchange", func(t *Trade) { t.Exchange = "" }, false},
		{"no symbol", func(t *Trade) { t.Symbol = "" }, false},
		{"negative id", func(t *Trade) { t.TradeID = -1 }, false},
		{"zero price", func(t *Trade) { t.Price = MustDecimal("0") }, false},
		{"zero qty", func(t *Trade) { t.Qty = Decimal{} }, false},
		{"bad side", func(t *Trade) { t.Side = "up" }, false},
		{"no exchange time", func(t *Trade) { t.ExchangeTime = time.Time{} }, false},
		{"no received time", func(t *Trade) { t.ReceivedTime = time.Time{} }, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := validTrade()
			tt.mutate(&tr)
			err := tr.Validate()
			if tt.ok && err != nil {
				t.Fatalf("Validate() = %v, want nil", err)
			}
			if !tt.ok && !errors.Is(err, ErrInvalidTrade) {
				t.Fatalf("Validate() = %v, want ErrInvalidTrade", err)
			}
		})
	}
}

func TestKeys(t *testing.T) {
	want := StreamKey{Exchange: ExchangeKraken, Symbol: "BTC/USD"}
	if got := (Trade{Exchange: ExchangeKraken, Symbol: "BTC/USD"}).Key(); got != want {
		t.Errorf("Trade.Key() = %v", got)
	}
	if got := (Incident{Exchange: ExchangeKraken, Symbol: "BTC/USD"}).Key(); got != want {
		t.Errorf("Incident.Key() = %v", got)
	}
	if got := (Bar{Exchange: ExchangeKraken, Symbol: "BTC/USD"}).Key(); got != want {
		t.Errorf("Bar.Key() = %v", got)
	}
	if want.String() != "kraken:BTC/USD" {
		t.Errorf("StreamKey.String() = %q", want.String())
	}
}

func TestBarEnd(t *testing.T) {
	start := time.Date(2026, 1, 2, 3, 4, 0, 0, time.UTC)
	if got := (Bar{Start: start}).End(); !got.Equal(start.Add(time.Minute)) {
		t.Errorf("End() = %v", got)
	}
}
