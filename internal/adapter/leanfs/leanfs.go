// Package leanfs writes minute trade bars in the on-disk format of the
// QuantConnect LEAN engine, so a backtest can read Tekel's output directly.
//
// Layout (verified against LEAN's Data/crypto/readme.md and its sample files,
// see docs/adr/0001-lean-output-format.md):
//
//	<root>/crypto/<market>/minute/<ticker>/<yyyymmdd>_trade.zip
//	  └── <yyyymmdd>_<ticker>_minute_trade.csv
//
// Each CSV line is "ms since midnight UTC,open,high,low,close,volume", no header,
// LF line endings, one line per minute that had trades.
package leanfs

import (
	"archive/zip"
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/big"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Amadeus-22/tekel/internal/domain"
	"github.com/Amadeus-22/tekel/internal/domain/bars"
)

// Writer implements app.BarSink. Each write rewrites that day's zip through a
// temporary file and an atomic rename, so a reader or a crash never sees a
// half-written file. A day holds at most 1440 bars (~100 KB), so rewriting it
// once per minute per symbol is cheap.
type Writer struct {
	root string

	mu   sync.Mutex
	days map[domain.StreamKey]*dayFile // the day currently being written, per stream
}

type dayFile struct {
	date string // yyyymmdd
	bars map[int64]domain.Bar
}

// New returns a Writer rooted at root, LEAN's data folder.
func New(root string) (*Writer, error) {
	if root == "" {
		return nil, errors.New("leanfs: root directory is required")
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, fmt.Errorf("leanfs: create root %s: %w", root, err)
	}
	return &Writer{root: root, days: make(map[domain.StreamKey]*dayFile)}, nil
}

// Ticker converts an exchange symbol to LEAN's ticker: lowercase letters and
// digits only ("BTC/USD" → "btcusd", "BTCUSDT" → "btcusdt").
func Ticker(symbol string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(symbol) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
		}
	}
	return b.String()
}

// ZipPath returns where the bars of market/symbol for date go.
func ZipPath(root, market, symbol string, date time.Time) string {
	return filepath.Join(root, "crypto", market, "minute", Ticker(symbol),
		date.UTC().Format("20060102")+"_trade.zip")
}

// EntryName returns the name of the CSV inside the zip.
func EntryName(symbol string, date time.Time) string {
	return date.UTC().Format("20060102") + "_" + Ticker(symbol) + "_minute_trade.csv"
}

// WriteBar adds b to its day file. If the file already has a bar for the same
// minute (a partial bar written before a restart), the two are merged.
func (w *Writer) WriteBar(_ context.Context, b domain.Bar) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	start := b.Start.UTC()
	date := start.Format("20060102")
	path := ZipPath(w.root, b.Exchange, b.Symbol, start)

	day := w.days[b.Key()]
	if day == nil || day.date != date {
		loaded, err := readDay(path, b.Key(), start)
		if err != nil {
			return err
		}
		day = &dayFile{date: date, bars: loaded}
		w.days[b.Key()] = day // the previous day is complete; drop it from memory
	}

	ms := msSinceMidnight(start)
	if prev, ok := day.bars[ms]; ok {
		merged, err := bars.Merge(prev, b)
		if err != nil {
			return fmt.Errorf("leanfs: %w", err)
		}
		b = merged
	}
	day.bars[ms] = b

	if err := writeAtomic(path, EntryName(b.Symbol, start), start, encodeDay(day.bars)); err != nil {
		delete(w.days, b.Key()) // reload from disk next time rather than trust memory
		return err
	}
	return nil
}

func msSinceMidnight(t time.Time) int64 {
	midnight := t.Truncate(24 * time.Hour)
	return t.Sub(midnight).Milliseconds()
}

func encodeDay(m map[int64]domain.Bar) []byte {
	keys := make([]int64, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	var buf bytes.Buffer
	for _, k := range keys {
		b := m[k]
		buf.WriteString(strconv.FormatInt(k, 10))
		for _, v := range []*big.Rat{b.Open, b.High, b.Low, b.Close, b.Volume} {
			buf.WriteByte(',')
			buf.WriteString(domain.FormatRat(v))
		}
		buf.WriteByte('\n')
	}
	return buf.Bytes()
}

// writeAtomic writes a single-entry zip to a temp file next to path, syncs it and
// renames it over path. Rename within one directory is atomic on POSIX systems.
func writeAtomic(path, entry string, modified time.Time, csv []byte) (err error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("leanfs: create %s: %w", dir, err)
	}
	tmp, err := os.CreateTemp(dir, ".tmp-*.zip")
	if err != nil {
		return fmt.Errorf("leanfs: create temp file in %s: %w", dir, err)
	}
	defer func() {
		if err != nil {
			tmp.Close()
			os.Remove(tmp.Name())
		}
	}()

	zw := zip.NewWriter(tmp)
	fw, err := zw.CreateHeader(&zip.FileHeader{Name: entry, Method: zip.Deflate, Modified: modified})
	if err != nil {
		return fmt.Errorf("leanfs: add %s to zip: %w", entry, err)
	}
	if _, err := fw.Write(csv); err != nil {
		return fmt.Errorf("leanfs: write %s: %w", entry, err)
	}
	if err := zw.Close(); err != nil {
		return fmt.Errorf("leanfs: finish zip: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		return fmt.Errorf("leanfs: sync %s: %w", tmp.Name(), err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("leanfs: close %s: %w", tmp.Name(), err)
	}
	if err := os.Chmod(tmp.Name(), 0o644); err != nil {
		return fmt.Errorf("leanfs: chmod %s: %w", tmp.Name(), err)
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		return fmt.Errorf("leanfs: rename to %s: %w", path, err)
	}
	return syncDir(dir)
}

// syncDir makes the rename itself durable.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("leanfs: open dir %s: %w", dir, err)
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("leanfs: sync dir %s: %w", dir, err)
	}
	return nil
}

// readDay loads an existing day file, or returns an empty day if there is none.
// This is how a restart continues a day instead of overwriting it.
func readDay(path string, key domain.StreamKey, date time.Time) (map[int64]domain.Bar, error) {
	out := make(map[int64]domain.Bar)
	zr, err := zip.OpenReader(path)
	if errors.Is(err, os.ErrNotExist) {
		return out, nil
	}
	if err != nil {
		return nil, fmt.Errorf("leanfs: open existing %s: %w", path, err)
	}
	defer zr.Close()
	if len(zr.File) != 1 {
		return nil, fmt.Errorf("leanfs: %s has %d entries, want 1", path, len(zr.File))
	}
	rc, err := zr.File[0].Open()
	if err != nil {
		return nil, fmt.Errorf("leanfs: open entry in %s: %w", path, err)
	}
	defer rc.Close()
	midnight := date.UTC().Truncate(24 * time.Hour)
	if err := parseCSV(rc, key, midnight, out); err != nil {
		return nil, fmt.Errorf("leanfs: parse %s: %w", path, err)
	}
	return out, nil
}

func parseCSV(r io.Reader, key domain.StreamKey, midnight time.Time, out map[int64]domain.Bar) error {
	sc := bufio.NewScanner(r)
	for line := 1; sc.Scan(); line++ {
		text := strings.TrimSpace(sc.Text())
		if text == "" {
			continue
		}
		f := strings.Split(text, ",")
		if len(f) != 6 {
			return fmt.Errorf("line %d: %d fields, want 6", line, len(f))
		}
		ms, err := strconv.ParseInt(f[0], 10, 64)
		if err != nil || ms < 0 || ms >= 24*60*60*1000 {
			return fmt.Errorf("line %d: bad time %q", line, f[0])
		}
		vals := make([]*big.Rat, 5)
		for i := range vals {
			d, err := domain.ParseDecimal(f[i+1])
			if err != nil {
				return fmt.Errorf("line %d field %d: %w", line, i+2, err)
			}
			vals[i] = d.Rat()
		}
		out[ms] = domain.Bar{
			Exchange: key.Exchange, Symbol: key.Symbol,
			Start: midnight.Add(time.Duration(ms) * time.Millisecond),
			Open:  vals[0], High: vals[1], Low: vals[2], Close: vals[3], Volume: vals[4],
		}
	}
	return sc.Err()
}
