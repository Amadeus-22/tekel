package leanfs

import (
	"archive/zip"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeZip(t *testing.T, path string, entries map[string]string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	zw := zip.NewWriter(f)
	for name, content := range entries {
		w, err := zw.Create(name)
		if err != nil {
			t.Fatal(err)
		}
		io.WriteString(w, content)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	f.Close()
}

func TestSourceWalksDirectory(t *testing.T) {
	root := t.TempDir()
	good := filepath.Join(root, "crypto", "binance", "minute", "btcusdt", "20240101_trade.zip")
	writeZip(t, good, map[string]string{"20240101_btcusdt_minute_trade.csv": "0,1,1,1,1,1\n"})
	writeZip(t, filepath.Join(root, "crypto", "binance", "hour", "btcusdt_trade.zip"), map[string]string{"a.csv": "", "b.csv": ""})
	os.WriteFile(filepath.Join(root, "readme.md"), []byte("not data"), 0o644)

	src, err := NewSource(root)
	if err != nil {
		t.Fatal(err)
	}
	files, err := src.Files(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 2 || files[0].Name != "crypto/binance/hour/btcusdt_trade.zip" ||
		files[1].Name != "crypto/binance/minute/btcusdt/20240101_trade.zip" ||
		files[1].Path != filepath.ToSlash(good) {
		t.Fatalf("files = %+v", files)
	}

	rc, err := src.Open(context.Background(), files[1])
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(rc)
	if err := rc.Close(); err != nil || string(b) != "0,1,1,1,1,1\n" {
		t.Errorf("content %q, close err %v", b, err)
	}
	if _, err := src.Open(context.Background(), files[0]); err == nil || !strings.Contains(err.Error(), "2 entries") {
		t.Errorf("two-entry zip err = %v", err)
	}
}

func TestSourceSingleFileAndErrors(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "20240101_trade.zip")
	writeZip(t, path, map[string]string{"x.csv": "0,1,1,1,1,1\n"})
	src, err := NewSource(path)
	if err != nil {
		t.Fatal(err)
	}
	files, err := src.Files(context.Background())
	if err != nil || len(files) != 1 || files[0].Name != "20240101_trade.zip" {
		t.Fatalf("files = %+v, %v", files, err)
	}

	notZip := filepath.Join(root, "data.csv")
	os.WriteFile(notZip, []byte("x"), 0o644)
	corrupt := filepath.Join(root, "bad.zip")
	os.WriteFile(corrupt, []byte("not a zip"), 0o644)
	if _, err := NewSource(notZip); err == nil {
		t.Error("csv accepted as a source")
	}
	if _, err := NewSource(filepath.Join(root, "missing")); err == nil {
		t.Error("missing path accepted")
	}
	bad, _ := NewSource(corrupt)
	bf, _ := bad.Files(context.Background())
	if _, err := bad.Open(context.Background(), bf[0]); err == nil || !strings.Contains(err.Error(), "open zip") {
		t.Errorf("corrupt zip err = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	dir, _ := NewSource(root)
	if _, err := dir.Files(ctx); err == nil {
		t.Error("cancelled walk returned no error")
	}
}
