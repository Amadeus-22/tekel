package domain

import "errors"

// Sentinel errors. Callers match them with errors.Is.
var (
	ErrInvalidDecimal = errors.New("invalid decimal")
	ErrInvalidSide    = errors.New("invalid side")
	ErrInvalidTrade   = errors.New("invalid trade")
	ErrInvalidKind    = errors.New("invalid incident kind")
	// ErrLateTrade means a trade belongs to a minute whose bar was already emitted.
	ErrLateTrade = errors.New("trade arrived after its bar was emitted")
)
