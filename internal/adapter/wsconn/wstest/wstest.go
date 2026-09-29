// Package wstest runs a local WebSocket server for adapter tests, the WebSocket
// counterpart of net/http/httptest. Tests never talk to real exchanges.
package wstest

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/gorilla/websocket"
)

// Server accepts WebSocket connections and hands each one to a script.
type Server struct {
	*httptest.Server
	// Conns counts accepted connections.
	Conns atomic.Int32

	header atomic.Pointer[http.Header]
}

// Header returns the handshake headers of the latest connection, or nil.
func (s *Server) Header() http.Header {
	if h := s.header.Load(); h != nil {
		return *h
	}
	return nil
}

// NewServer starts a server that runs script for every connection; n is the
// 1-based connection number. The connection is closed when script returns.
func NewServer(t testing.TB, script func(n int, conn *websocket.Conn)) *Server {
	t.Helper()
	s := &Server{}
	up := websocket.Upgrader{}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := r.Header.Clone()
		s.header.Store(&h)
		conn, err := up.Upgrade(w, r, nil)
		if err != nil {
			return // Upgrade already wrote the HTTP error
		}
		defer conn.Close()
		script(int(s.Conns.Add(1)), conn)
	}))
	t.Cleanup(s.Close)
	return s
}

// URL returns the ws:// URL of the server plus path.
func (s *Server) URL(path string) string {
	return "ws" + strings.TrimPrefix(s.Server.URL, "http") + path
}

// DrainUntilClosed reads and discards messages until the client goes away, so
// that control frames (ping, close) are processed.
func DrainUntilClosed(conn *websocket.Conn) {
	for {
		if _, _, err := conn.ReadMessage(); err != nil {
			return
		}
	}
}
