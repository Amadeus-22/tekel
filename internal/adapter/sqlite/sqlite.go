// Package sqlite stores quality incidents in a local SQLite database using the
// pure-Go modernc.org/sqlite driver (no cgo, so the binary stays static).
package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "modernc.org/sqlite" // registers the "sqlite" driver

	"github.com/Amadeus-22/tekel/internal/app"
	"github.com/Amadeus-22/tekel/internal/domain"
)

// MaxLimit caps one List call.
const MaxLimit = 1000

const schema = `
CREATE TABLE IF NOT EXISTS incidents (
	id          INTEGER PRIMARY KEY AUTOINCREMENT,
	exchange    TEXT    NOT NULL,
	symbol      TEXT    NOT NULL,
	kind        TEXT    NOT NULL,
	detected_at INTEGER NOT NULL, -- unix microseconds, UTC
	start_at    INTEGER,          -- unix microseconds, NULL for point incidents
	end_at      INTEGER,
	trade_id    INTEGER,          -- NULL when not about one trade
	detail      TEXT    NOT NULL
);
CREATE INDEX IF NOT EXISTS incidents_detected_at ON incidents (detected_at);
CREATE INDEX IF NOT EXISTS incidents_stream ON incidents (exchange, symbol, kind, detected_at);
`

// Store implements app.IncidentStore and the incident query used by the API.
type Store struct {
	db *sql.DB
}

// Open opens (creating if needed) the database at path and applies the schema.
func Open(ctx context.Context, path string) (*Store, error) {
	// WAL lets API reads proceed while the pipeline writes; busy_timeout makes
	// a brief lock contention wait instead of failing.
	dsn := "file:" + path + "?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=synchronous(NORMAL)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite %s: %w", path, err)
	}
	// SQLite has one writer; a single connection avoids SQLITE_BUSY between our
	// own connections and keeps the write order equal to the call order.
	db.SetMaxOpenConns(1)
	if _, err := db.ExecContext(ctx, schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("apply sqlite schema: %w", err)
	}
	return &Store{db: db}, nil
}

// Close releases the database.
func (s *Store) Close() error { return s.db.Close() }

// Ping checks that the database answers; used by /readyz.
func (s *Store) Ping(ctx context.Context) error {
	if err := s.db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping sqlite: %w", err)
	}
	return nil
}

// Save inserts one incident.
func (s *Store) Save(ctx context.Context, inc domain.Incident) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO incidents (exchange, symbol, kind, detected_at, start_at, end_at, trade_id, detail)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		inc.Exchange, inc.Symbol, string(inc.Kind), inc.DetectedAt.UnixMicro(),
		nullTime(inc.Start), nullTime(inc.End), nullID(inc.TradeID), inc.Detail)
	if err != nil {
		return fmt.Errorf("insert incident %s %s: %w", inc.Key(), inc.Kind, err)
	}
	return nil
}

// List returns incidents matching f, newest first. A zero or oversized Limit
// is clamped to MaxLimit.
func (s *Store) List(ctx context.Context, f app.IncidentFilter) ([]domain.Incident, error) {
	var where []string
	var args []any
	add := func(clause string, v any) {
		where = append(where, clause)
		args = append(args, v)
	}
	if f.Exchange != "" {
		add("exchange = ?", f.Exchange)
	}
	if f.Symbol != "" {
		add("symbol = ?", f.Symbol)
	}
	if f.Kind != "" {
		add("kind = ?", string(f.Kind))
	}
	if !f.Since.IsZero() {
		add("detected_at >= ?", f.Since.UnixMicro())
	}
	limit := f.Limit
	if limit <= 0 || limit > MaxLimit {
		limit = MaxLimit
	}
	q := "SELECT id, exchange, symbol, kind, detected_at, start_at, end_at, trade_id, detail FROM incidents"
	if len(where) > 0 {
		q += " WHERE " + strings.Join(where, " AND ")
	}
	q += " ORDER BY detected_at DESC, id DESC LIMIT ?"
	args = append(args, limit)

	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("query incidents: %w", err)
	}
	defer rows.Close()

	out := []domain.Incident{}
	for rows.Next() {
		var (
			inc               domain.Incident
			kind              string
			detected          int64
			start, end, trade sql.NullInt64
		)
		if err := rows.Scan(&inc.ID, &inc.Exchange, &inc.Symbol, &kind, &detected, &start, &end, &trade, &inc.Detail); err != nil {
			return nil, fmt.Errorf("scan incident: %w", err)
		}
		inc.Kind = domain.IncidentKind(kind)
		inc.DetectedAt = time.UnixMicro(detected).UTC()
		inc.Start = fromNull(start)
		inc.End = fromNull(end)
		inc.TradeID = trade.Int64
		out = append(out, inc)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate incidents: %w", err)
	}
	return out, nil
}

func nullTime(t time.Time) sql.NullInt64 {
	if t.IsZero() {
		return sql.NullInt64{}
	}
	return sql.NullInt64{Int64: t.UnixMicro(), Valid: true}
}

func nullID(id int64) sql.NullInt64 {
	return sql.NullInt64{Int64: id, Valid: id != 0}
}

func fromNull(v sql.NullInt64) time.Time {
	if !v.Valid {
		return time.Time{}
	}
	return time.UnixMicro(v.Int64).UTC()
}
