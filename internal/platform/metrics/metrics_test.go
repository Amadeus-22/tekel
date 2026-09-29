package metrics

import (
	"io"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/Amadeus-22/tickguard/internal/app"
	"github.com/Amadeus-22/tickguard/internal/domain"
)

var _ app.Metrics = (*Metrics)(nil)

func TestMetricsRecordAndServe(t *testing.T) {
	m := New()
	m.TradeReceived("binance", "BTCUSDT", 40*time.Millisecond)
	m.TradeReceived("binance", "BTCUSDT", -time.Second) // clamps to 0
	m.IncidentRecorded("binance", "BTCUSDT", domain.KindSequenceGap)
	m.LateTrade("binance", "BTCUSDT")
	m.SetConnected("binance", true)
	m.SetConnected("kraken", false)
	m.Backpressure("binance", 1500*time.Millisecond)
	m.SinkError("bars")
	m.DecodeError("kraken")

	checks := []struct {
		got  float64
		want float64
		name string
	}{
		{testutil.ToFloat64(m.trades.WithLabelValues("binance", "BTCUSDT")), 2, "trades"},
		{testutil.ToFloat64(m.incidents.WithLabelValues("binance", "BTCUSDT", "sequence_gap")), 1, "incidents"},
		{testutil.ToFloat64(m.lateTrades.WithLabelValues("binance", "BTCUSDT")), 1, "late"},
		{testutil.ToFloat64(m.connected.WithLabelValues("binance")), 1, "connected binance"},
		{testutil.ToFloat64(m.connected.WithLabelValues("kraken")), 0, "connected kraken"},
		{testutil.ToFloat64(m.backpressure.WithLabelValues("binance")), 1.5, "backpressure"},
		{testutil.ToFloat64(m.sinkErrors.WithLabelValues("bars")), 1, "sink errors"},
		{testutil.ToFloat64(m.decodeErrors.WithLabelValues("kraken")), 1, "decode errors"},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %v, want %v", c.name, c.got, c.want)
		}
	}

	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	body, _ := io.ReadAll(rec.Body)
	for _, name := range []string{
		"tickguard_trades_total", "tickguard_incidents_total", "tickguard_feed_lag_seconds_bucket",
		"tickguard_connected", "tickguard_decode_errors_total", "tickguard_backpressure_seconds_total",
		"tickguard_late_trades_total", "tickguard_sink_errors_total", "go_goroutines",
	} {
		if !strings.Contains(string(body), name) {
			t.Errorf("/metrics lacks %s", name)
		}
	}
	if !strings.Contains(string(body), `tickguard_feed_lag_seconds_sum{exchange="binance",symbol="BTCUSDT"} 0.04`) {
		t.Error("lag sum should be 0.04 (the negative observation clamps to 0)")
	}
	if m.Registry() == nil {
		t.Error("Registry() = nil")
	}
}
