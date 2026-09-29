package auditreport

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"testing"

	"github.com/Amadeus-22/tickguard/internal/adapter/leanfs"
	"github.com/Amadeus-22/tickguard/internal/app"
)

var update = flag.Bool("update", false, "rewrite the golden files")

// testdata/lean holds small zips trimmed from the sample data in LEAN's
// repository (Data/ at QuantConnect/Lean), in LEAN's own layout:
//
//   - coinbase minute btcusd 20161007 and 20161009: first 30 rows of each
//     sample (20161008 left out, so a missing day is reported; the trimmed
//     days end at 00:29, so the rest of each day is a missing_bars warning);
//   - coinbase daily btcusd, binance hour btcusdt: first 20 / 30 rows;
//   - coinbase tick btcusd 20161007: LEAN has no crypto tick sample, so the
//     first 40 rows of the second-resolution sample as time,close,volume;
//   - equity usa minute aapl 20140609: first 30 rows (the split-day bar with
//     the pre-split price 645.57 is a genuine spike in LEAN's sample);
//   - coinbase minute ethusd 20170903: 40 rows with defects injected at lines
//     5 (duplicate time), 8 (out of order), 12 (high < low), 15 (negative
//     volume), 18 (zero prices), 20 (garbage), 30 (3x spike), 33 (misaligned);
//   - a quote zip (skipped) and a two-entry zip (bad_file).
func auditFixtures(t *testing.T) app.AuditReport {
	t.Helper()
	src, err := leanfs.NewSource(filepath.Join("testdata", "lean"))
	if err != nil {
		t.Fatal(err)
	}
	r, err := app.Audit(context.Background(), src, app.DefaultAuditConfig())
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestGolden(t *testing.T) {
	r := auditFixtures(t)
	for _, format := range []string{FormatTable, FormatJSON} {
		t.Run(format, func(t *testing.T) {
			var buf bytes.Buffer
			if err := Write(&buf, format, r); err != nil {
				t.Fatal(err)
			}
			golden := filepath.Join("testdata", "golden."+format)
			if *update {
				if err := os.WriteFile(golden, buf.Bytes(), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			want, err := os.ReadFile(golden)
			if err != nil {
				t.Fatalf("%v (run go test -update to create it)", err)
			}
			if !bytes.Equal(buf.Bytes(), want) {
				t.Errorf("%s output differs from %s (go test ./internal/adapter/auditreport -update to accept):\n%s", format, golden, buf.String())
			}
		})
	}
}

func TestJSONIsValidAndComplete(t *testing.T) {
	r := auditFixtures(t)
	var buf bytes.Buffer
	if err := WriteJSON(&buf, r); err != nil {
		t.Fatal(err)
	}
	var back app.AuditReport
	if err := json.Unmarshal(buf.Bytes(), &back); err != nil {
		t.Fatal(err)
	}
	if len(back.Issues) != len(r.Issues) || back.Totals != r.Totals {
		t.Errorf("round trip lost data: %d issues, totals %+v", len(back.Issues), back.Totals)
	}
	kinds := map[string]bool{}
	for _, is := range back.Issues {
		kinds[string(is.Kind)] = true
	}
	for _, k := range []string{"bad_file", "unparseable_row", "bad_time", "duplicate_timestamp", "out_of_order",
		"ohlc_inconsistent", "non_positive_price", "negative_volume", "price_spike", "missing_bars", "missing_days"} {
		if !kinds[k] {
			t.Errorf("fixtures do not exercise %s", k)
		}
	}
}

func TestWriteTableCapsIssuesAndRejectsFormat(t *testing.T) {
	var r app.AuditReport
	for range MaxTableIssues + 5 {
		r.Issues = append(r.Issues, app.Issue{File: "f.zip", Line: 1, Severity: "error", Kind: "bad_time"})
	}
	r.Totals.Error = MaxTableIssues + 5
	r.IssuesOmitted = 3
	var buf bytes.Buffer
	if err := WriteTable(&buf, r); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	if !bytes.Contains(buf.Bytes(), []byte("error and warn: 105, showing 100")) || !bytes.Contains(buf.Bytes(), []byte("3 issues beyond the cap")) {
		t.Errorf("table = %s", out)
	}
	if err := Write(&buf, "xml", r); err == nil {
		t.Error("unknown format accepted")
	}
}
