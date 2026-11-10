package leanfs

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Amadeus-22/tickguard/internal/domain/audit"
)

// TestWrittenFilesAuditRoundTrip: what the live writer produces, the offline
// audit must recognise and parse back unchanged. The two sides share no code
// on purpose (leanfs.parseCSV vs audit.ParseBar), so this test is what keeps
// their idea of the LEAN layout in step.
func TestWrittenFilesAuditRoundTrip(t *testing.T) {
	root := t.TempDir()
	w, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct{ exchange, symbol, ticker string }{
		{"binance", "BTCUSDT", "btcusdt"},
		{"kraken", "BTC/USD", "btcusd"},
		{"mercadobitcoin", "BTC-BRL", "btcbrl"},
	} {
		b := bar(tt.exchange, tt.symbol, 61, "100.5", "101", "99.25", "100", "0.125")
		if err := w.WriteBar(context.Background(), b); err != nil {
			t.Fatal(err)
		}
		path := ZipPath(root, tt.exchange, tt.symbol, day)
		rel, err := filepath.Rel(root, path)
		if err != nil {
			t.Fatal(err)
		}
		f, err := audit.ClassifyPath(filepath.ToSlash(rel))
		if err != nil {
			t.Fatalf("%s: ClassifyPath(%s) = %v", tt.exchange, rel, err)
		}
		want := audit.Series{SecurityType: audit.Crypto, Market: tt.exchange, Resolution: audit.Minute, Symbol: tt.ticker}
		if f.Series != want || !f.Date.Equal(day) {
			t.Errorf("%s: classified as %+v, want %+v on %s", tt.exchange, f, want, day.Format(time.DateOnly))
		}

		_, csv := readZip(t, path)
		row := strings.TrimSpace(csv)
		got, err := audit.ParseBar(f, 1, row)
		if err != nil {
			t.Fatalf("%s: ParseBar(%q) = %v", tt.exchange, row, err)
		}
		if !got.Time.Equal(b.Start) || got.Open.Cmp(b.Open) != 0 || got.High.Cmp(b.High) != 0 ||
			got.Low.Cmp(b.Low) != 0 || got.Close.Cmp(b.Close) != 0 || got.Volume.Cmp(b.Volume) != 0 {
			t.Errorf("%s: parsed %+v, want the bar written %+v", tt.exchange, got, b)
		}
	}
}
