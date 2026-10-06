package natscontext

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// Store holds the NATS KV bucket and configuration shared by all Contexts
// derived from a single call to New.
type Store struct {
	kv         jetstream.KeyValue
	codec      Codec
	maxRetries int
	backoff    func(attempt int) time.Duration
}

// Option configures a Store at construction time, via New.
type Option func(*Store)

// WithCodec overrides the default JSONCodec used to encode/decode values.
func WithCodec(c Codec) Option {
	return func(s *Store) { s.codec = c }
}

// WithMaxRetries overrides the default number of CAS retries (10) attempted
// by WithValue and WithValueFunc before giving up with
// ErrMaxRetriesExceeded.
func WithMaxRetries(n int) Option {
	return func(s *Store) { s.maxRetries = n }
}

// WithBackoff overrides the default backoff used between CAS retry attempts.
func WithBackoff(fn func(attempt int) time.Duration) Option {
	return func(s *Store) { s.backoff = fn }
}

// encodeKey derives a NATS KV-safe key for the given namespace and
// NatsContextValue key, hashing the key so that arbitrary key contents
// (which may include characters not permitted in a NATS KV key) always
// produce a valid key.
func (s *Store) encodeKey(namespace string, key NatsContextValue) (string, error) {
	sum := sha256.Sum256([]byte(key))
	return namespace + "." + hex.EncodeToString(sum[:]), nil
}

type mutateFunc func(data []byte, exists bool) ([]byte, error)

// cas performs an optimistic read-modify-write against natsKey: it reads the
// current entry (if any), passes its raw bytes to mutate, and attempts to
// commit the returned bytes using Create (if the key did not exist) or
// Update with the observed revision (if it did). If another writer commits a
// change concurrently, the whole cycle is retried against the new revision,
// up to s.maxRetries times.
func (s *Store) cas(ctx context.Context, natsKey string, mutate mutateFunc) error {
	maxRetries := s.maxRetries
	if maxRetries <= 0 {
		maxRetries = 1
	}
	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(s.backoff(attempt)):
			}
		}

		entry, getErr := s.kv.Get(ctx, natsKey)
		switch {
		case errors.Is(getErr, jetstream.ErrKeyNotFound):
			data, err := mutate(nil, false)
			if err != nil {
				return err
			}
			if _, err := s.kv.Create(ctx, natsKey, data); err != nil {
				if errors.Is(err, jetstream.ErrKeyExists) {
					lastErr = err
					continue
				}
				return err
			}
			return nil
		case getErr != nil:
			return getErr
		default:
			data, err := mutate(entry.Value(), true)
			if err != nil {
				return err
			}
			if _, err := s.kv.Update(ctx, natsKey, data, entry.Revision()); err != nil {
				if errors.Is(err, jetstream.ErrKeyRevisionMismatch) || errors.Is(err, jetstream.ErrKeyExists) {
					lastErr = err
					continue
				}
				return err
			}
			return nil
		}
	}
	if lastErr != nil {
		return fmt.Errorf("%w: %v", ErrMaxRetriesExceeded, lastErr)
	}
	return ErrMaxRetriesExceeded
}
