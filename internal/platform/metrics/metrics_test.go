package metrics

import (
	"io"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/Amadeus-22/tekel/internal/app"
	"github.com/Amadeus-22/tekel/internal/domain"
	"github.com/Amadeus-22/tekel/internal/domain/reference"
)

var _ app.Metrics = (*Metrics)(nil)

func TestMetricsRecordAndServe(t *testing.T) {
	m := New()
	m.TradeReceived("binance", "BTCUSDT", 40*time.Millisecond)
	m.TradeReceived("binance", "BTCUSDT", -time.Second) // clamps to 0
	m.BlockReceived("klever", "mainnet", 3, 2*time.Second)
	m.BlockReceived("klever", "mainnet", 0, 2*time.Second)
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
		{testutil.ToFloat64(m.blocks.WithLabelValues("klever", "mainnet")), 2, "blocks"},
		{testutil.ToFloat64(m.blockTxs.WithLabelValues("klever", "mainnet")), 3, "block transactions"},
		{float64(testutil.CollectAndCount(m.trades)), 1, "trade series (blocks must not add one)"},
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
		"tekel_trades_total", "tekel_blocks_total", "tekel_block_transactions_total", "tekel_incidents_total", "tekel_feed_lag_seconds_bucket",
		"tekel_connected", "tekel_decode_errors_total", "tekel_backpressure_seconds_total",
		"tekel_late_trades_total", "tekel_sink_errors_total", "go_goroutines",
	} {
		if !strings.Contains(string(body), name) {
			t.Errorf("/metrics lacks %s", name)
		}
	}
	if !strings.Contains(string(body), `tekel_feed_lag_seconds_sum{exchange="binance",symbol="BTCUSDT"} 0.04`) {
		t.Error("lag sum should be 0.04 (the negative observation clamps to 0)")
	}
	if m.Registry() == nil {
		t.Error("Registry() = nil")
	}
}

func TestReferenceDistanceCountsChecksByBand(t *testing.T) {
	m := New()
	m.ReferenceDeviation("binance", "BTCUSDT", 0.0004) // within 0.05%
	m.ReferenceDeviation("binance", "BTCUSDT", -0.002) // below the price: distance 0.2%
	m.ReferenceDeviation("binance", "BTCUSDT", 0.03)   // beyond every bound

	if got := testutil.ToFloat64(m.refDeviation.WithLabelValues("binance", "BTCUSDT")); got != 0.03 {
		t.Errorf("deviation gauge = %v, want the last check, 0.03", got)
	}
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	body, _ := io.ReadAll(rec.Body)
	for _, line := range []string{
		`tekel_reference_distance_ratio_bucket{exchange="binance",symbol="BTCUSDT",le="0.0005"} 1`,
		`tekel_reference_distance_ratio_bucket{exchange="binance",symbol="BTCUSDT",le="0.001"} 1`,
		`tekel_reference_distance_ratio_bucket{exchange="binance",symbol="BTCUSDT",le="0.0025"} 2`,
		`tekel_reference_distance_ratio_bucket{exchange="binance",symbol="BTCUSDT",le="0.025"} 2`,
		`tekel_reference_distance_ratio_bucket{exchange="binance",symbol="BTCUSDT",le="+Inf"} 3`,
		`tekel_reference_distance_ratio_count{exchange="binance",symbol="BTCUSDT"} 3`,
	} {
		if !strings.Contains(string(body), line) {
			t.Errorf("/metrics lacks %s", line)
		}
	}
}

func TestReferenceUsageSetsCreditGauges(t *testing.T) {
	m := New()
	m.ReferenceUsage(reference.Usage{UsedToday: 3, UsedMonth: 40, LeftMonth: 9960})
	for kind, want := range map[string]float64{"used_today": 3, "used_month": 40, "left_month": 9960} {
		if got := testutil.ToFloat64(m.refCredits.WithLabelValues(kind)); got != want {
			t.Errorf("credits %s = %v, want %v", kind, got, want)
		}
	}
}
