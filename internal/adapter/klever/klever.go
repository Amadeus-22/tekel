// Package klever is the KleverChain block feed: one WebSocket connection to a
// Klever node's /subscribe endpoint with the "blocks" subscription, re-sent
// after every reconnect. Blocks carry a sequential nonce, so tickguard's gap
// rules apply to them; they carry no price, so the feed is of kind blocks (no
// price check, no LEAN bars). See docs/adr/0007-klever-block-feed.md.
//
// Message formats follow the klever-go source (github.com/klever-io/klever-go,
// websocket/README.md, network/api/websocket/routes.go, indexer/events.go,
// indexer/dataParser.go) and messages recorded from the public testnet node on
// 2026-11-10 (testdata/):
//   - subscribe:  {"addresses":[],"subscribed_types":["blocks"]}, sent right
//     after the handshake; the server closes after 30 s without it
//   - block:      {"type":"blocks","address":"","hash":"","data":{"hash","nonce",
//     "timestamp" (Unix ms, whole seconds),"chainID","txCount",...}}
//   - error:      {"error":"invalid subscription type: bogus"}, then close
//   - keepalive:  the server pings every 15 s (websocket/config.go)
//
// The node drops events when its event queue or a client's output buffer is
// full (indexer/events.go trySendEvent, websocket/client.go): such a drop
// shows up here as a nonce jump while connected, which is a silent gap.
package klever

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"time"

	"github.com/gorilla/websocket"

	"github.com/Amadeus-22/tickguard/internal/adapter/wsconn"
	"github.com/Amadeus-22/tickguard/internal/app"
	"github.com/Amadeus-22/tickguard/internal/domain"
)

// Chain identifies the KleverChain block stream. It takes the place of the
// exchange in a StreamKey; the network ("mainnet", "testnet") takes the place
// of the symbol. See docs/adr/0007.
const Chain = "klever"

// TestnetURL is the public testnet node's subscription endpoint. The public
// mainnet node (node.mainnet.klever.org) answers 404 on /subscribe (checked
// 2026-11-10), so mainnet needs the URL of a node that serves it.
const TestnetURL = "wss://node.testnet.klever.org/subscribe"

const (
	// defaultReadTimeout: a block every 4 s and a server ping every 15 s, so
	// 30 s of silence means the connection is dead.
	defaultReadTimeout = 30 * time.Second
	// maxMessageBytes: a block lists every transaction hash (~70 bytes each),
	// so a full block is far larger than a trade message.
	maxMessageBytes = 8 << 20
)

// ChainIDs maps the networks tickguard knows to the chainID their blocks carry
// (GET /node/overview on node.mainnet.klever.org and node.testnet.klever.org,
// 2026-11-10). A block from another chain stops the feed rather than being
// recorded under the wrong network.
var ChainIDs = map[string]string{"mainnet": "108", "testnet": "109"}

// Config configures the feed.
type Config struct {
	// URL is the node's /subscribe endpoint; empty means TestnetURL on testnet.
	URL string
	// Network is "mainnet" or "testnet"; it is the stream's symbol.
	Network string
	// Conn holds timeouts and backoff; Name, URL and the read limit are set by New.
	Conn   wsconn.Config
	Logger *slog.Logger
	// DecodeErrors is called once per malformed message; nil is allowed.
	DecodeErrors func()
}

// Feed implements app.Feed for KleverChain blocks.
type Feed struct {
	cfg       Config
	chainID   string
	malformed *wsconn.Malformed
}

// New validates cfg and returns a feed.
func New(cfg Config) (*Feed, error) {
	chainID, ok := ChainIDs[cfg.Network]
	if !ok {
		return nil, fmt.Errorf("klever: network %q must be mainnet or testnet", cfg.Network)
	}
	if cfg.URL == "" {
		if cfg.Network != "testnet" {
			return nil, fmt.Errorf("klever: no public %s node serves /subscribe; set the URL of one that does", cfg.Network)
		}
		cfg.URL = TestnetURL
	}
	if cfg.Logger == nil {
		return nil, errors.New("klever: logger is required")
	}
	if cfg.Conn.ReadTimeout == 0 || cfg.Conn.ReadTimeout > defaultReadTimeout {
		cfg.Conn.ReadTimeout = defaultReadTimeout
	}
	cfg.Conn.MaxMessageBytes = max(cfg.Conn.MaxMessageBytes, maxMessageBytes)
	cfg.Conn.Name = Chain
	cfg.Conn.URL = cfg.URL
	cfg.Conn.Logger = cfg.Logger
	return &Feed{cfg: cfg, chainID: chainID,
		malformed: wsconn.NewMalformed(Chain, cfg.Logger, cfg.DecodeErrors)}, nil
}

// Exchange implements app.Feed.
func (f *Feed) Exchange() string { return Chain }

// Symbols implements app.Feed: the one stream is the network.
func (f *Feed) Symbols() []string { return []string{f.cfg.Network} }

// Kind implements app.Feed: the feed carries blocks.
func (f *Feed) Kind() domain.StreamKind { return domain.StreamBlocks }

// SubscribeRequest is the first message on every connection.
type SubscribeRequest struct {
	Addresses []string `json:"addresses"`
	Types     []string `json:"subscribed_types"`
}

// NewSubscribeRequest returns the global "blocks" subscription (no addresses).
func NewSubscribeRequest() SubscribeRequest {
	return SubscribeRequest{Addresses: []string{}, Types: []string{"blocks"}}
}

// Run implements app.Feed.
func (f *Feed) Run(ctx context.Context, emit app.Emit) error {
	return wsconn.Run(ctx, f.cfg.Conn, wsconn.Handlers{
		Subscribe: func(_ context.Context, conn *websocket.Conn) error {
			return conn.WriteJSON(NewSubscribeRequest())
		},
		Up: func(ctx context.Context, at time.Time) error {
			return emit(ctx, app.Event{Kind: app.EventConnected, At: at})
		},
		Down: func(ctx context.Context, at time.Time, cause error) error {
			return emit(ctx, app.Event{Kind: app.EventDisconnected, At: at, Err: cause})
		},
		Message: func(ctx context.Context, msg []byte, received time.Time) error {
			m, err := Decode(msg, received, f.cfg.Network)
			if err != nil {
				f.malformed.Report(msg, received, err)
				return nil
			}
			if m.Error != "" {
				// The node closes the connection after any handshake error, and
				// every one of them means our request is wrong.
				return wsconn.Permanent(fmt.Errorf("node error: %s (%s)", m.Error, f.cfg.URL))
			}
			if m.Block == nil {
				return nil
			}
			if m.ChainID != f.chainID {
				return wsconn.Permanent(fmt.Errorf("node %s serves chain %q, but %s is chain %q",
					f.cfg.URL, m.ChainID, f.cfg.Network, f.chainID))
			}
			return emit(ctx, app.Event{Kind: app.EventBlock, At: received, Block: *m.Block})
		},
	})
}

// Message is the part of one decoded message tickguard acts on.
type Message struct {
	// Block is set for a "blocks" event; ChainID is the chain it came from.
	Block   *domain.Block
	ChainID string
	// Error is the node's message for an {"error":...} reply.
	Error string
}

// envelope covers the push event, the handshake error and request replies:
//
//	{"type":"blocks","address":"","hash":"","data":{...}}
//	{"error":"invalid subscription type: bogus"}
//	{"id":"1","data":"subscribed"}
type envelope struct {
	Type  string          `json:"type"`
	Data  json.RawMessage `json:"data"`
	Error string          `json:"error"`
}

// blockData is the part of "data" that tickguard reads. timestamp is the block
// header's Unix seconds times 1000 (indexer/dataParser.go).
type blockData struct {
	Hash      string  `json:"hash"`
	Nonce     *uint64 `json:"nonce"`
	Timestamp *int64  `json:"timestamp"`
	ChainID   string  `json:"chainID"`
	TxCount   int     `json:"txCount"`
}

// Decode parses one message for network. Events of other types and replies
// to requests decode to an empty Message.
func Decode(msg []byte, received time.Time, network string) (Message, error) {
	var env envelope
	if err := json.Unmarshal(msg, &env); err != nil {
		return Message{}, fmt.Errorf("decode envelope: %w", err)
	}
	if env.Error != "" {
		return Message{Error: env.Error}, nil
	}
	if env.Type != "blocks" {
		return Message{}, nil
	}
	var d blockData
	if err := json.Unmarshal(env.Data, &d); err != nil {
		return Message{}, fmt.Errorf("decode block data: %w", err)
	}
	switch {
	case d.Nonce == nil:
		return Message{}, errors.New("decode block: missing nonce")
	case *d.Nonce > math.MaxInt64:
		return Message{}, fmt.Errorf("decode block: nonce %d out of range", *d.Nonce)
	case d.Timestamp == nil || *d.Timestamp <= 0:
		return Message{}, errors.New("decode block: missing timestamp")
	}
	b := domain.Block{
		Chain: Chain, Network: network, Nonce: int64(*d.Nonce), Hash: d.Hash, TxCount: d.TxCount,
		ChainTime: time.UnixMilli(*d.Timestamp).UTC(), ReceivedTime: received.UTC(),
	}
	if err := b.Validate(); err != nil {
		return Message{}, fmt.Errorf("decode block: %w", err)
	}
	return Message{Block: &b, ChainID: d.ChainID}, nil
}
