package app

import (
	"context"
	"errors"
	"io"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/Amadeus-22/tickguard/internal/domain/audit"
)

// fakeSource serves in-memory CSV content keyed by path; a nil entry fails to open.
type fakeSource struct {
	files   map[string]*string
	listErr error
}

func (f fakeSource) Files(context.Context) ([]SourceFile, error) {
	var out []SourceFile
	for p := range f.files {
		out = append(out, SourceFile{Name: p, Path: "/data/" + p})
	}
	slices.SortFunc(out, func(a, b SourceFile) int { return strings.Compare(a.Name, b.Name) })
	return out, f.listErr
}

func (f fakeSource) Open(_ context.Context, sf SourceFile) (io.ReadCloser, error) {
	content := f.files[sf.Name]
	if content == nil {
		return nil, errors.New("zip: not a valid zip file")
	}
	return io.NopCloser(strings.NewReader(*content)), nil
}

func ptr(s string) *string { return &s }

// fullMinuteDay returns 1440 identical minute rows, optionally with CRLF endings.
func fullMinuteDay(crlf bool) string {
	var b strings.Builder
	for m := range 1440 {
		b.WriteString(strconv.Itoa(m * 60000))
		b.WriteString(",10,10,10,10,1")
		if crlf {
			b.WriteByte('\r')
		}
		b.WriteByte('\n')
	}
	return b.String()
}

func TestAuditCleanSeriesAndMissingDays(t *testing.T) {
	src := fakeSource{files: map[string]*string{
		"crypto/binance/minute/btcusdt/20240101_trade.zip": ptr(fullMinuteDay(false)),
		"crypto/binance/minute/btcusdt/20240102_trade.zip": ptr(fullMinuteDay(true)), // CRLF like LEAN's Bitfinex sample
		"crypto/binance/minute/btcusdt/20240105_trade.zip": ptr(fullMinuteDay(false)),
		"crypto/binance/minute/btcusdt/20240101_quote.zip": ptr("ignored"),
	}}
	r, err := Audit(context.Background(), src, DefaultAuditConfig())
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Files) != 3 || r.Rows != 3*1440 || len(r.Skipped) != 1 || !strings.Contains(r.Skipped[0].Reason, "quote") {
		t.Fatalf("files=%d rows=%d skipped=%+v", len(r.Files), r.Rows, r.Skipped)
	}
	if len(r.Issues) != 1 || r.Issues[0].Kind != audit.KindMissingDays ||
		r.Issues[0].File != "crypto/binance/minute/btcusdt" || !strings.Contains(r.Issues[0].Detail, "2024-01-03 to 2024-01-04") {
		t.Fatalf("issues = %+v", r.Issues)
	}
	s := r.Series[0]
	if s.Series != "crypto/binance/minute/btcusdt" || s.Files != 3 || s.Warn != 1 || s.TimeZone != "UTC" ||
		s.First != "2024-01-01T00:00:00" || s.Last != "2024-01-05T23:59:00" {
		t.Errorf("series = %+v", s)
	}
	if r.Totals != (Counts{Warn: 1}) || !r.Fails(audit.Warn) || r.Fails(audit.Error) {
		t.Errorf("totals = %+v", r.Totals)
	}
}

func TestAuditFindingsPlacedAndSorted(t *testing.T) {
	rows := strings.Split(strings.TrimSpace(fullMinuteDay(false)), "\n")
	rows[40] = "2400000,10,30,10,30,1" // spike at line 41: reported after the rows that follow it
	rows[45] = "garbage"               // line 46
	rows[50] = "3000000,10,10,10,10,-1"
	src := fakeSource{files: map[string]*string{
		"crypto/kraken/minute/xbtusd/20240101_trade.zip": ptr(strings.Join(rows, "\n") + "\n\n"),
		"crypto/kraken/minute/xbtusd/20240102_trade.zip": nil, // unreadable
		"crypto/kraken/tick/xbtusd/20240101_trade.zip":   ptr("1000,10,1\n500,10,1,1\n"),
	}}
	r, err := Audit(context.Background(), src, DefaultAuditConfig())
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, is := range r.Issues {
		got = append(got, strings.TrimPrefix(is.File, "crypto/kraken/")+":"+strconv.Itoa(is.Line)+":"+string(is.Kind))
	}
	want := []string{
		"minute/xbtusd/20240101_trade.zip:41:price_spike",
		"minute/xbtusd/20240101_trade.zip:46:unparseable_row",
		"minute/xbtusd/20240101_trade.zip:47:missing_bars", // the minute of the garbage row
		"minute/xbtusd/20240101_trade.zip:51:negative_volume",
		"minute/xbtusd/20240102_trade.zip:0:bad_file",
		"tick/xbtusd/20240101_trade.zip:2:out_of_order",
		"tick/xbtusd/20240101_trade.zip:0:missing_bars", // the rest of the day
	}
	if strings.Join(got, " ") != strings.Join(want, " ") {
		t.Fatalf("issues =\n%v\nwant\n%v", got, want)
	}
	if r.Files[1].Rows != 0 || r.Files[1].Error != 1 {
		t.Errorf("unreadable file summary = %+v", r.Files[1])
	}
	if r.Rows != 1440+2 {
		t.Errorf("rows = %d (blank lines must not count)", r.Rows)
	}
}

func TestAuditCapsIssuesButKeepsCounts(t *testing.T) {
	var b strings.Builder
	for range 50 {
		b.WriteString("x\n")
	}
	src := fakeSource{files: map[string]*string{"equity/usa/minute/spy/20240102_trade.zip": ptr(b.String())}}
	cfg := DefaultAuditConfig()
	cfg.MaxIssues = 10
	r, err := Audit(context.Background(), src, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Issues) != 10 || r.IssuesOmitted != 40 || r.Totals.Error != 50 {
		t.Errorf("issues=%d omitted=%d totals=%+v", len(r.Issues), r.IssuesOmitted, r.Totals)
	}
}

func TestAuditErrors(t *testing.T) {
	if _, err := Audit(context.Background(), fakeSource{listErr: errors.New("boom")}, DefaultAuditConfig()); err == nil {
		t.Error("list error swallowed")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	src := fakeSource{files: map[string]*string{"crypto/a/minute/b/20240101_trade.zip": ptr("")}}
	if _, err := Audit(ctx, src, DefaultAuditConfig()); !errors.Is(err, context.Canceled) {
		t.Errorf("cancelled audit err = %v", err)
	}
}

func TestReportFails(t *testing.T) {
	tests := []struct {
		totals    Counts
		threshold audit.Severity
		want      bool
	}{
		{Counts{}, audit.Warn, false},
		{Counts{Info: 3}, audit.Warn, false},
		{Counts{Warn: 1}, audit.Warn, true},
		{Counts{Warn: 1}, audit.Error, false},
		{Counts{Error: 1}, audit.Error, true},
		{Counts{Error: 1}, audit.Warn, true},
	}
	for _, tt := range tests {
		if got := (AuditReport{Totals: tt.totals}).Fails(tt.threshold); got != tt.want {
			t.Errorf("Fails(%+v, %s) = %v", tt.totals, tt.threshold, got)
		}
	}
}
