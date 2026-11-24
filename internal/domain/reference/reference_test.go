package reference

import (
	"math"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Amadeus-22/tekel/internal/domain"
)

func TestSplitPair(t *testing.T) {
	tests := []struct {
		symbol, base, quote string
		ok                  bool
	}{
		{"BTCUSDT", "BTC", "USDT", true},
		{"ethfdusd", "ETH", "FDUSD", true},
		{"BTC/USD", "BTC", "USD", true},
		{"BTC-BRL", "BTC", "BRL", true},
		{"ETHBTC", "ETH", "BTC", true},
		{"USDT", "", "", false},
		{"BTC/", "", "", false},
		{"XYZ", "", "", false},
	}
	for _, tt := range tests {
		b, q, ok := SplitPair(tt.symbol)
		if b != tt.base || q != tt.quote || ok != tt.ok {
			t.Errorf("SplitPair(%q) = %q, %q, %v; want %q, %q, %v", tt.symbol, b, q, ok, tt.base, tt.quote, tt.ok)
		}
	}
}

func TestAssetForMapsStablecoinsToUSD(t *testing.T) {
	for symbol, want := range map[string]Asset{
		"BTCUSDT": {"BTC", "USD"}, "BTC/USD": {"BTC", "USD"}, "BTC-BRL": {"BTC", "BRL"}, "ETHBTC": {"ETH", "BTC"},
	} {
		if got, ok := AssetFor(symbol); !ok || got != want {
			t.Errorf("AssetFor(%q) = %v, %v; want %v", symbol, got, ok, want)
		}
	}
}

func TestDeviationIsExact(t *testing.T) {
	got := Deviation(domain.MustDecimal("101"), domain.MustDecimal("100"))
	if math.Abs(got-0.01) > 1e-15 {
		t.Fatalf("Deviation = %v, want 0.01", got)
	}
	if got := Deviation(domain.MustDecimal("99.5"), domain.MustDecimal("100")); math.Abs(got+0.005) > 1e-15 {
		t.Fatalf("Deviation = %v, want -0.005", got)
	}
}

func TestConfigValidate(t *testing.T) {
	if err := DefaultConfig().Validate(); err != nil {
		t.Fatalf("default config: %v", err)
	}
	bad := Config{Threshold: 0, MaxQuoteAge: 0, MaxTradeAge: time.Minute}
	if err := bad.Validate(); err == nil {
		t.Fatal("want an error for zero threshold and zero quote age")
	}
}

var (
	t0  = time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)
	btc = domain.StreamKey{Exchange: "kraken", Symbol: "BTC/USD"} // a USD pair: no stablecoin cross
	usd = Asset{"BTC", "USD"}
)

func obs(price string, at time.Time) []Observation {
	return []Observation{{Key: btc, Price: domain.MustDecimal(price), ReceivedAt: at}}
}

func quotes(price string, at time.Time) map[Asset]Quote {
	return map[Asset]Quote{usd: {Price: domain.MustDecimal(price), UpdatedAt: at}}
}

func TestMonitorRaisesOnceUntilBackInRange(t *testing.T) {
	m, err := NewMonitor(DefaultConfig()) // threshold 1%
	if err != nil {
		t.Fatal(err)
	}
	steps := []struct {
		price     string
		incidents int
	}{
		{"100.5", 0}, // 0.5%: in range
		{"102", 1},   // 2%: raise
		{"103", 0},   // still out: no repeat
		{"100.7", 0}, // 0.7%: under threshold but above half of it, still breached
		{"102", 0},   // so this is not a new incident
		{"100.2", 0}, // 0.2%: back in range
		{"98", 1},    // -2%: a new incident
	}
	for i, s := range steps {
		res, inc := m.Check(t0, obs(s.price, t0), quotes("100", t0))
		if len(res) != 1 || len(inc) != s.incidents {
			t.Fatalf("step %d (%s): %d results, %d incidents; want 1, %d", i, s.price, len(res), len(inc), s.incidents)
		}
	}
}

func TestMonitorIncidentFields(t *testing.T) {
	m, _ := NewMonitor(DefaultConfig())
	_, inc := m.Check(t0, obs("61000", t0), quotes("61670.205039", t0))
	if len(inc) != 1 {
		t.Fatalf("want 1 incident, got %d", len(inc))
	}
	got := inc[0]
	if got.Kind != domain.KindPriceDeviation || got.Key() != btc || !got.DetectedAt.Equal(t0) {
		t.Fatalf("incident = %+v", got)
	}
	for _, want := range []string{"61000", "61670.205039 USD", "-1.09%", "threshold 1.00%"} {
		if !strings.Contains(got.Detail, want) {
			t.Errorf("detail %q does not contain %q", got.Detail, want)
		}
	}
}

func TestMonitorSkipsStaleOrMissingData(t *testing.T) {
	cfg := DefaultConfig()
	m, _ := NewMonitor(cfg)
	old := t0.Add(-cfg.MaxTradeAge - time.Second)
	cases := map[string]struct {
		o []Observation
		q map[Asset]Quote
	}{
		"old trade":      {obs("200", old), quotes("100", t0)},
		"old quote":      {obs("200", t0), quotes("100", t0.Add(-cfg.MaxQuoteAge-time.Second))},
		"no quote":       {obs("200", t0), nil},
		"unknown symbol": {[]Observation{{Key: domain.StreamKey{Exchange: "x", Symbol: "XYZ"}, Price: domain.MustDecimal("1"), ReceivedAt: t0}}, quotes("100", t0)},
		"no price":       {[]Observation{{Key: btc, ReceivedAt: t0}}, quotes("100", t0)},
	}
	for name, c := range cases {
		if res, inc := m.Check(t0, c.o, c.q); len(res) != 0 || len(inc) != 0 {
			t.Errorf("%s: %d results, %d incidents; want none", name, len(res), len(inc))
		}
	}
}

func TestAssetsForAddsTheStablecoinQuote(t *testing.T) {
	for symbol, want := range map[string][]Asset{
		"BTCUSDT":  {{"BTC", "USD"}, {"USDT", "USD"}},
		"ETH-USDC": {{"ETH", "USD"}, {"USDC", "USD"}},
		"BTC/USD":  {{"BTC", "USD"}},
		"BTC-BRL":  {{"BTC", "BRL"}},
		"XYZ":      nil,
	} {
		if got := AssetsFor(symbol); !slices.Equal(got, want) {
			t.Errorf("AssetsFor(%q) = %v, want %v", symbol, got, want)
		}
	}
}

// USDT at 0.999 USD made every Binance USDT pair read about 0.1% above
// CoinMarketCap's USD price; priced in USDT, the same trade is in line.
func TestMonitorPricesStablecoinPairsInTheStablecoin(t *testing.T) {
	m, _ := NewMonitor(DefaultConfig())
	pair := domain.StreamKey{Exchange: "binance", Symbol: "BTCUSDT"}
	trade := []Observation{{Key: pair, Price: domain.MustDecimal("100.1"), ReceivedAt: t0}}
	q := map[Asset]Quote{
		{"BTC", "USD"}:  {Price: domain.MustDecimal("100"), UpdatedAt: t0},
		{"USDT", "USD"}: {Price: domain.MustDecimal("0.999"), UpdatedAt: t0},
	}
	res, _ := m.Check(t0, trade, q)
	if len(res) != 1 {
		t.Fatalf("results = %v, want one", res)
	}
	if res[0].Asset != (Asset{"BTC", "USDT"}) {
		t.Errorf("asset = %v, want BTC/USDT", res[0].Asset)
	}
	// 100 / 0.999 = 100.1001…; the trade is 0.0001% below it.
	if d := res[0].Deviation; math.Abs(d) > 0.00001 {
		t.Errorf("deviation = %v, want about 0 once priced in USDT (it was +0.1%% against USD)", d)
	}

	delete(q, Asset{"USDT", "USD"})
	if res, _ := m.Check(t0, trade, q); len(res) != 0 {
		t.Errorf("without the USDT quote: %v, want no comparison rather than one against USD", res)
	}
	q[Asset{"USDT", "USD"}] = Quote{Price: domain.MustDecimal("0.999"), UpdatedAt: t0.Add(-time.Hour)}
	if res, _ := m.Check(t0, trade, q); len(res) != 0 {
		t.Errorf("with a stale USDT quote: %v, want no comparison", res)
	}
}
