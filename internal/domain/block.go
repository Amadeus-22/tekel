package domain

import (
	"fmt"
	"time"
)

// StreamKind says what a stream carries. A feed declares it once; the pipeline
// reads it when it registers the feed's streams.
type StreamKind string

// Stream kinds.
const (
	// StreamTrades carries trades: every check runs and trades become LEAN bars.
	StreamTrades StreamKind = "trades"
	// StreamBlocks carries chain blocks: only the sequence, time and connection
	// checks apply (no price, no bars).
	StreamBlocks StreamKind = "blocks"
)

// Block is one block of a chain whose blocks carry a sequential nonce (height).
type Block struct {
	Chain string
	// Network names the chain instance, e.g. "mainnet"; it is the stream's symbol.
	Network string
	// Nonce is the block height; consecutive blocks differ by one.
	Nonce int64
	Hash  string
	// TxCount is the number of transactions in the block.
	TxCount int
	// ChainTime is the block timestamp set by its producer.
	ChainTime time.Time
	// ReceivedTime is when tickguard read the block off the socket.
	ReceivedTime time.Time
}

// Key returns the stream this block belongs to.
func (b Block) Key() StreamKey { return StreamKey{Exchange: b.Chain, Symbol: b.Network} }

// Validate checks the invariants every decoded block must satisfy.
func (b Block) Validate() error {
	switch {
	case b.Chain == "":
		return fmt.Errorf("%w: missing chain", ErrInvalidBlock)
	case b.Network == "":
		return fmt.Errorf("%w: missing network", ErrInvalidBlock)
	case b.Nonce < 0:
		return fmt.Errorf("%w: negative nonce %d", ErrInvalidBlock, b.Nonce)
	case b.Hash == "":
		return fmt.Errorf("%w: missing hash", ErrInvalidBlock)
	case b.TxCount < 0:
		return fmt.Errorf("%w: negative transaction count %d", ErrInvalidBlock, b.TxCount)
	case b.ChainTime.IsZero():
		return fmt.Errorf("%w: missing chain time", ErrInvalidBlock)
	case b.ReceivedTime.IsZero():
		return fmt.Errorf("%w: missing received time", ErrInvalidBlock)
	}
	return nil
}
