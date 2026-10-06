// Package natscontext provides an implementation of context.Context whose
// values (set via WithValue) are stored in a NATS JetStream key/value
// bucket rather than in local process memory. This allows independent
// processes/nodes to coordinate through a shared, named context: a value
// set on one node becomes visible to any other node holding a context with
// the same namespace, as soon as the write commits.
//
// Only keys of type NatsContextValue are backed by the KV bucket. Value
// checks whether a given key is a NatsContextValue first; if it is not, the
// lookup is delegated directly to the parent context chain without ever
// consulting the bucket. This keeps ordinary, process-local context values
// (set via the standard library's context.WithValue) cheap and unaffected,
// while reserving the distributed behavior for keys explicitly typed as
// NatsContextValue.
//
// Writes go through an optimistic compare-and-swap loop against the KV
// entry's revision, retrying on conflict until they commit. This linearizes
// concurrent writers around the bucket's revision history rather than
// silently losing updates, giving strongly serializable semantics for
// coordination built on top of it (e.g. counters via WithValueFunc, or
// leader-election-style "set if absent" via WithValue + Create semantics).
package natscontext

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"regexp"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

type ctxKey struct{}

var infoKey ctxKey

type contextInfo struct {
	store     *Store
	namespace string
}

var namespacePattern = regexp.MustCompile(`^[A-Za-z0-9_=\-.]+$`)

// Context is a context.Context whose values, for keys of type
// NatsContextValue, are backed by a NATS JetStream KV bucket. Value lookups
// for such keys always query the bucket live, so that all nodes sharing the
// same namespace observe a consistent view of values written through
// WithValue/WithValueFunc. Lookups for any other key type are resolved
// through the parent context chain, exactly as with a plain context.Context.
type Context struct {
	context.Context
	info *contextInfo
}

var _ context.Context = (*Context)(nil)

// New creates a root Context backed by kv. All values set through
// WithValue/WithValueFunc on this context, or on any context derived from
// it (including via context.WithCancel/WithTimeout), are namespaced under
// id, allowing a single bucket to be shared by multiple independent
// distributed contexts.
//
// id must contain only characters permitted in a NATS KV key: alphanumeric
// characters, dashes, underscores, equals signs, and dots.
func New(parent context.Context, kv jetstream.KeyValue, id string, opts ...Option) (*Context, error) {
	if kv == nil {
		return nil, errors.New("natscontext: kv must not be nil")
	}
	if !namespacePattern.MatchString(id) {
		return nil, fmt.Errorf("%w: id %q contains characters not permitted in a NATS KV key", ErrInvalidKey, id)
	}

	store := &Store{
		kv:         kv,
		codec:      JSONCodec{},
		maxRetries: 10,
		backoff:    defaultBackoff,
	}
	for _, opt := range opts {
		opt(store)
	}

	info := &contextInfo{store: store, namespace: id}
	base := context.WithValue(parent, infoKey, info)
	return &Context{Context: base, info: info}, nil
}

// Value looks up key, first checking whether key is of type NatsContextValue.
// If it is, Value performs a live Get against the NATS KV bucket (so that
// writes made by other nodes sharing this context's namespace are
// observed), falling back to the parent context chain on miss. If key is not
// a NatsContextValue, the KV bucket is never consulted and the lookup is
// delegated directly to the parent context chain, which covers both
// ordinary context.WithValue entries and this context's own distributed
// namespace metadata.
func (c *Context) Value(key any) any {
	nck, ok := key.(NatsContextValue)
	if !ok {
		return c.Context.Value(key)
	}
	natsKey, err := c.info.store.encodeKey(c.info.namespace, nck)
	if err == nil {
		if entry, getErr := c.info.store.kv.Get(context.Background(), natsKey); getErr == nil {
			var v any
			if derr := c.info.store.codec.Decode(entry.Value(), &v); derr == nil {
				return v
			}
		}
	}
	return c.Context.Value(key)
}

func infoFromContext(ctx context.Context) (*contextInfo, bool) {
	info, ok := ctx.Value(infoKey).(*contextInfo)
	return info, ok
}

// NatsContextValue is the key type recognized by Context.Value as backed by
// the distributed NATS KV bucket. Only lookups for keys of this type ever
// reach the KV bucket; any other key type is resolved purely through the
// parent context chain. The package-level WithValue, WithValueFunc, and Load
// functions require keys of this type for the same reason: a value stored
// under a key of another type would never be observed by Context.Value.
type NatsContextValue string

func (v NatsContextValue) String() string {
	return string(v)
}

// WithValue sets key to value in the shared KV bucket backing ctx. The write
// is performed as a compare-and-swap against the key's current revision,
// retrying on conflict until it commits, so that concurrent writers from
// different nodes are linearized rather than one silently clobbering
// another. It returns a new context.Context reflecting the write; ctx itself
// is left unmodified.
//
// key must be of type NatsContextValue, since Context.Value only consults
// the KV bucket for keys of that type.
//
// ctx must have been created by New, or be derived from such a context,
// otherwise ErrNotDistributed is returned.
func WithValue(ctx context.Context, key NatsContextValue, value any) (context.Context, error) {
	info, ok := infoFromContext(ctx)
	if !ok {
		return nil, ErrNotDistributed
	}
	natsKey, err := info.store.encodeKey(info.namespace, key)
	if err != nil {
		return nil, err
	}
	data, err := info.store.codec.Encode(value)
	if err != nil {
		return nil, err
	}
	err = info.store.cas(ctx, natsKey, func([]byte, bool) ([]byte, error) {
		return data, nil
	})
	if err != nil {
		return nil, err
	}
	return &Context{Context: ctx, info: info}, nil
}

// WithValueFunc performs a read-modify-write of key: it fetches the value
// currently stored for key (exists reports whether one is present) and
// passes it to fn, then stores the value fn returns. If a concurrent writer
// commits a change to key before this write lands, the entire
// read-modify-write cycle is retried against the new revision. This gives
// strongly serializable semantics for operations that depend on the
// previous value, such as counters or accumulating sets.
//
// key must be of type NatsContextValue, since Context.Value only consults
// the KV bucket for keys of that type.
//
// ctx must have been created by New, or be derived from such a context,
// otherwise ErrNotDistributed is returned.
func WithValueFunc(ctx context.Context, key NatsContextValue, fn func(old any, exists bool) (any, error)) (context.Context, error) {
	info, ok := infoFromContext(ctx)
	if !ok {
		return nil, ErrNotDistributed
	}
	natsKey, err := info.store.encodeKey(info.namespace, key)
	if err != nil {
		return nil, err
	}

	var fnErr error
	err = info.store.cas(ctx, natsKey, func(data []byte, exists bool) ([]byte, error) {
		var old any
		if exists {
			if derr := info.store.codec.Decode(data, &old); derr != nil {
				return nil, derr
			}
		}
		newVal, ferr := fn(old, exists)
		if ferr != nil {
			fnErr = ferr
			return nil, ferr
		}
		return info.store.codec.Encode(newVal)
	})
	if fnErr != nil {
		return nil, fnErr
	}
	if err != nil {
		return nil, err
	}
	return &Context{Context: ctx, info: info}, nil
}

// Load fetches the value and KV revision currently stored for key directly
// from the bucket, without falling back to the parent context chain. found
// reports whether key currently has a value.
//
// key must be of type NatsContextValue, since Context.Value only consults
// the KV bucket for keys of that type.
//
// ctx must have been created by New, or be derived from such a context,
// otherwise ErrNotDistributed is returned.
func Load(ctx context.Context, key NatsContextValue) (value any, revision uint64, found bool, err error) {
	info, ok := infoFromContext(ctx)
	if !ok {
		return nil, 0, false, ErrNotDistributed
	}
	natsKey, err := info.store.encodeKey(info.namespace, key)
	if err != nil {
		return nil, 0, false, err
	}
	entry, err := info.store.kv.Get(ctx, natsKey)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return nil, 0, false, nil
	}
	if err != nil {
		return nil, 0, false, err
	}
	var v any
	if derr := info.store.codec.Decode(entry.Value(), &v); derr != nil {
		return nil, 0, false, derr
	}
	return v, entry.Revision(), true, nil
}

func defaultBackoff(attempt int) time.Duration {
	d := time.Duration(attempt+1) * 5 * time.Millisecond
	if d > 200*time.Millisecond {
		d = 200 * time.Millisecond
	}
	return d + time.Duration(rand.Intn(5))*time.Millisecond
}
