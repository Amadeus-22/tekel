package leanfs

import (
	"archive/zip"
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/Amadeus-22/tekel/internal/app"
)

// Source implements app.LeanSource over a LEAN data directory or one zip file.
// It only reads; nothing under the path is modified.
type Source struct {
	root   string // absolute
	single bool
}

// NewSource returns a Source for path, which may be a directory (walked
// recursively) or a single .zip file.
func NewSource(path string) (*Source, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("leanfs: resolve %s: %w", path, err)
	}
	info, err := os.Stat(abs)
	if err != nil {
		return nil, fmt.Errorf("leanfs: %w", err)
	}
	if !info.IsDir() && !strings.HasSuffix(abs, ".zip") {
		return nil, fmt.Errorf("leanfs: %s is neither a directory nor a .zip file", path)
	}
	return &Source{root: abs, single: !info.IsDir()}, nil
}

// Files lists every .zip file, sorted by name. Other files (readmes, map and
// factor files) are ignored. Names are relative to the directory, or the base
// name for a single file.
func (s *Source) Files(ctx context.Context) ([]app.SourceFile, error) {
	if s.single {
		return []app.SourceFile{{Name: filepath.Base(s.root), Path: filepath.ToSlash(s.root)}}, nil
	}
	var out []app.SourceFile
	err := filepath.WalkDir(s.root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(d.Name(), ".zip") {
			return nil
		}
		rel, err := filepath.Rel(s.root, path)
		if err != nil {
			return err
		}
		out = append(out, app.SourceFile{Name: filepath.ToSlash(rel), Path: filepath.ToSlash(path)})
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("leanfs: walk %s: %w", s.root, err)
	}
	slices.SortFunc(out, func(a, b app.SourceFile) int { return strings.Compare(a.Name, b.Name) })
	return out, nil
}

// Open returns the single CSV entry of the zip. LEAN's data readme states each
// zip holds one CSV file; anything else is reported rather than guessed at.
func (s *Source) Open(_ context.Context, f app.SourceFile) (io.ReadCloser, error) {
	zr, err := zip.OpenReader(filepath.FromSlash(f.Path))
	if err != nil {
		return nil, fmt.Errorf("open zip: %w", err)
	}
	if len(zr.File) != 1 {
		zr.Close()
		return nil, fmt.Errorf("zip has %d entries, want 1", len(zr.File))
	}
	rc, err := zr.File[0].Open()
	if err != nil {
		zr.Close()
		return nil, fmt.Errorf("open entry %s: %w", zr.File[0].Name, err)
	}
	return &entryReader{ReadCloser: rc, zip: zr}, nil
}

type entryReader struct {
	io.ReadCloser
	zip *zip.ReadCloser
}

func (e *entryReader) Close() error {
	err := e.ReadCloser.Close()
	if zerr := e.zip.Close(); err == nil {
		err = zerr
	}
	return err
}
