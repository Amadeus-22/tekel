// Package logging builds the process logger: log/slog with a JSON handler.
package logging

import (
	"fmt"
	"io"
	"log/slog"
)

// New returns a JSON logger writing to w at level ("debug", "info", "warn" or
// "error"). The logger is created once in main and passed down.
func New(w io.Writer, level string) (*slog.Logger, error) {
	var l slog.Level
	if err := l.UnmarshalText([]byte(level)); err != nil {
		return nil, fmt.Errorf("parse log level %q: %w", level, err)
	}
	return slog.New(slog.NewJSONHandler(w, &slog.HandlerOptions{Level: l})), nil
}
