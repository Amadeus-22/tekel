package leanfs

import (
	"archive/zip"
	"bytes"
	"context"
	"io"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Amadeus-22/tickguard/internal/domain"
)

var day = time.Date(2022, 12, 13, 0, 0, 0, 0, time.UTC)

func r(s string) *big.Rat { return domain.MustDecimal(s).Rat() }

func bar(exchange, symbol string, minute int, o, h, l, c, v string) domain.Bar {
	return domain.Bar{
		Exchange: exchange, Symbol: symbol, Start: day.Add(time.Duration(minute) * time.Minute),
		Open: r(o), High: r(h), Low: r(l), Close: r(c), Volume: r(v), Trades: 1,
	}
}

// readZip returns the single entry name and content of a zip file.
func readZip(t *testing.T, path string) (string, string) {
	t.Helper()
	zr, err := zip.OpenReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer zr.Close()
	if len(zr.File) != 1 {
		t.Fatalf("%s has %d entries", path, len(zr.File))
	}
	rc, err := zr.File[0].Open()
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	b, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	return zr.File[0].Name, string(b)
}

func TestWritesLEANLayout(t *testing.T) {
	root := t.TempDir()
	w, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	// Written out of order on purpose: the file must still be sorted by time.
	for _, b := range []domain.Bar{
		bar("binance", "BTCBUSD", 1, "17209.49000000", "17227.24", "17208.84", "17226.99", "147.61285"),
		bar("binance", "BTCBUSD", 0, "17212.12", "17216.55", "17208.88", "17208.88", "110.66042000"),
		bar("binance", "BTCBUSD", 2, "17227.23", "17241.10", "17225.42", "17225.42", "227.11544"),
	} {
		if err := w.WriteBar(ctx, b); err != nil {
			t.Fatal(err)
		}
	}

	path := filepath.Join(root, "crypto", "binance", "minute", "btcbusd", "20221213_trade.zip")
	name, csv := readZip(t, path)
	if name != "20221213_btcbusd_minute_trade.csv" {
		t.Errorf("entry name = %q", name)
	}
	// Byte-for-byte the first three lines of LEAN's own sample file.
	want, err := os.ReadFile("testdata/lean_binance_btcbusd_20221213_head.csv")
	if err != nil {
		t.Fatal(err)
	}
	wantHead := strings.Join(strings.SplitAfter(string(want), "\n")[:3], "")
	if csv != wantHead {
		t.Errorf("csv =\n%s\nwant (LEAN sample) =\n%s", csv, wantHead)
	}

	entries, _ := os.ReadDir(filepath.Dir(path))
	if len(entries) != 1 {
		t.Errorf("directory has %d files, want only the zip (no temp leftovers)", len(entries))
	}
}

func TestLEANSampleRoundTrips(t *testing.T) {
	raw, err := os.ReadFile("testdata/lean_binance_btcbusd_20221213_head.csv")
	if err != nil {
		t.Fatal(err)
	}
	key := domain.StreamKey{Exchange: "binance", Symbol: "BTCBUSD"}
	parsed := map[int64]domain.Bar{}
	if err := parseCSV(bytes.NewReader(raw), key, day, parsed); err != nil {
		t.Fatal(err)
	}
	if got := string(encodeDay(parsed)); got != string(raw) {
		t.Errorf("re-encoded sample differs:\n%s\nwant\n%s", got, raw)
	}
	if b := parsed[240000]; !b.Start.Equal(day.Add(4 * time.Minute)) {
		t.Errorf("parsed start = %v", b.Start)
	}
}

func TestRestartMergesPartialBarAndKeepsDay(t *testing.T) {
	root := t.TempDir()
	ctx := context.Background()
	first, _ := New(root)
	first.WriteBar(ctx, bar("kraken", "BTC/USD", 0, "100", "101", "99", "100", "1"))
	first.WriteBar(ctx, bar("kraken", "BTC/USD", 1, "100", "105", "100", "104", "2")) // partial, at shutdown

	second, _ := New(root) // a new process
	if err := second.WriteBar(ctx, bar("kraken", "BTC/USD", 1, "104", "104.5", "98", "98.5", "0.5")); err != nil {
		t.Fatal(err)
	}
	_, csv := readZip(t, ZipPath(root, "kraken", "BTC/USD", day))
	want := "0,100,101,99,100,1\n60000,100,105,98,98.5,2.5\n"
	if csv != want {
		t.Errorf("csv =\n%s\nwant\n%s", csv, want)
	}
}

func TestNewDayGoesToNewFile(t *testing.T) {
	root := t.TempDir()
	w, _ := New(root)
	ctx := context.Background()
	w.WriteBar(ctx, bar("binance", "ETHUSDT", 1439, "1", "1", "1", "1", "1")) // 23:59
	w.WriteBar(ctx, bar("binance", "ETHUSDT", 1440, "2", "2", "2", "2", "1")) // next day 00:00

	_, d1 := readZip(t, ZipPath(root, "binance", "ETHUSDT", day))
	name, d2 := readZip(t, ZipPath(root, "binance", "ETHUSDT", day.Add(24*time.Hour)))
	if d1 != "86340000,1,1,1,1,1\n" || d2 != "0,2,2,2,2,1\n" || name != "20221214_ethusdt_minute_trade.csv" {
		t.Errorf("day1 = %q, day2 = %q (%s)", d1, d2, name)
	}
}

func TestCorruptExistingFileIsAnError(t *testing.T) {
	tests := map[string]string{
		"not a zip":     "garbage",
		"wrong columns": "zip:0,1,2\n",
		"bad time":      "zip:x,1,1,1,1,1\n",
		"time too big":  "zip:86400000,1,1,1,1,1\n",
		"bad price":     "zip:0,1,-1,1,1,1\n",
	}
	for name, content := range tests {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			path := ZipPath(root, "binance", "BTCUSDT", day)
			os.MkdirAll(filepath.Dir(path), 0o755)
			if csv, ok := strings.CutPrefix(content, "zip:"); ok {
				if err := writeAtomic(path, "x.csv", day, []byte(csv)); err != nil {
					t.Fatal(err)
				}
			} else {
				os.WriteFile(path, []byte(content), 0o644)
			}
			w, _ := New(root)
			if err := w.WriteBar(context.Background(), bar("binance", "BTCUSDT", 0, "1", "1", "1", "1", "1")); err == nil {
				t.Error("WriteBar over a corrupt file = nil error; it must not overwrite data it cannot read")
			}
		})
	}
}

func TestTwoEntryZipIsAnError(t *testing.T) {
	root := t.TempDir()
	path := ZipPath(root, "binance", "BTCUSDT", day)
	os.MkdirAll(filepath.Dir(path), 0o755)
	f, _ := os.Create(path)
	zw := zip.NewWriter(f)
	zw.Create("a.csv")
	zw.Create("b.csv")
	zw.Close()
	f.Close()
	w, _ := New(root)
	if err := w.WriteBar(context.Background(), bar("binance", "BTCUSDT", 0, "1", "1", "1", "1", "1")); err == nil {
		t.Error("WriteBar over a two-entry zip = nil error")
	}
}

func TestWriteFailureIsReported(t *testing.T) {
	root := t.TempDir()
	w, _ := New(root)
	// A file where the market directory should be makes MkdirAll fail.
	os.WriteFile(filepath.Join(root, "crypto"), []byte("x"), 0o644)
	if err := w.WriteBar(context.Background(), bar("binance", "BTCUSDT", 0, "1", "1", "1", "1", "1")); err == nil {
		t.Error("WriteBar = nil error when the directory cannot be created")
	}
}

func TestNewValidates(t *testing.T) {
	if _, err := New(""); err == nil {
		t.Error("New(\"\") = nil error")
	}
	file := filepath.Join(t.TempDir(), "file")
	os.WriteFile(file, nil, 0o644)
	if _, err := New(filepath.Join(file, "sub")); err == nil {
		t.Error("New under a file = nil error")
	}
}

func TestTickerAndPaths(t *testing.T) {
	tests := map[string]string{"BTC/USD": "btcusd", "BTCUSDT": "btcusdt", "1INCH/EUR": "1incheur"}
	for in, want := range tests {
		if got := Ticker(in); got != want {
			t.Errorf("Ticker(%q) = %q, want %q", in, got, want)
		}
	}
	got := ZipPath("/data", "kraken", "BTC/USD", day.Add(13*time.Hour))
	if got != "/data/crypto/kraken/minute/btcusd/20221213_trade.zip" {
		t.Errorf("ZipPath = %q", got)
	}
	if got := EntryName("BTC/USD", day); got != "20221213_btcusd_minute_trade.csv" {
		t.Errorf("EntryName = %q", got)
	}
}
