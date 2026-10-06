package natscontext

import "errors"

var (
	// ErrNotDistributed is returned when a package-level function (WithValue,
	// WithValueFunc, Load) is called with a context.Context that was not
	// created by New or derived from one.
	ErrNotDistributed = errors.New("natscontext: context was not created by natscontext.New")

	// ErrMaxRetriesExceeded is returned when a CAS loop could not commit its
	// write after exhausting the configured number of retries.
	ErrMaxRetriesExceeded = errors.New("natscontext: max retries exceeded while committing value")

	// ErrInvalidKey is returned when a key cannot be encoded into a valid
	// NATS KV key.
	ErrInvalidKey = errors.New("natscontext: key could not be encoded")
)
