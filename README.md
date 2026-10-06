# natscontext

A `context.Context` implementation whose values are stored in a NATS
JetStream key/value bucket instead of local process memory, so that
independent processes/nodes can coordinate through a shared, named context.

## How it works

- `New(parent, kv, id)` creates a root `*Context` namespaced by `id` within
  the given `jetstream.KeyValue` bucket. Multiple independent contexts can
  share the same bucket by using different `id`s.
- Only keys of type `natscontext.NatsContextValue` are backed by the KV
  bucket. `Value(key)` first checks whether `key` is a `NatsContextValue`;
  if it is, it performs a live `Get` against the KV bucket (falling back to
  the parent context chain on miss), so every node sharing the same
  namespace observes the same values as soon as a write commits. If `key` is
  any other type, the KV bucket is never consulted and the lookup is
  delegated straight to the parent context chain, exactly like a plain
  `context.Context`.
- `WithValue(ctx, key, value)` writes to the bucket using the key's current
  revision as a compare-and-swap precondition (`Create` if absent, `Update`
  with the observed revision otherwise), retrying on conflict until the
  write commits. This linearizes concurrent writers around NATS KV's
  revision history rather than silently dropping updates. `key` must be a
  `NatsContextValue`.
- `WithValueFunc(ctx, key, fn)` does the same, but as a read-modify-write:
  `fn` receives the current value and is retried, along with the read, on
  conflict. Use this for values that depend on their previous value, such as
  counters or accumulating sets. `key` must be a `NatsContextValue`.
- `Load(ctx, key)` reads a key's value and revision directly, without
  falling back to the parent chain. `key` must be a `NatsContextValue`.

Because the namespace/store metadata travels through the context via a
private key, contexts derived with `context.WithCancel`/`WithTimeout`/etc.
still work with the package-level `WithValue`/`WithValueFunc`/`Load`
functions.

## Example

```go
nc, _ := nats.Connect(nats.DefaultURL)
js, _ := jetstream.New(nc)
kv, _ := js.CreateKeyValue(context.Background(), jetstream.KeyValueConfig{
    Bucket:  "COORDINATION",
    History: 10, // keep revision history for CAS retries
})

ctx, err := natscontext.New(context.Background(), kv, "job-42")

const statusKey natscontext.NatsContextValue = "status"
ctx, err = natscontext.WithValue(ctx, statusKey, "running")

// On another node, sharing the same bucket and id "job-42":
ctx.Value(statusKey) // => "running"

// Read-modify-write, e.g. a distributed counter:
const countKey natscontext.NatsContextValue = "count"
ctx, err = natscontext.WithValueFunc(ctx, countKey, func(old any, exists bool) (any, error) {
    if !exists {
        return float64(1), nil
    }
    return old.(float64) + 1, nil
})

// Keys of any other type bypass the KV bucket entirely and behave like a
// plain context.WithValue:
type localKey struct{}
ctx = context.WithValue(ctx, localKey{}, "only visible in this process")
```

## Notes

- The default `JSONCodec` decodes into generic types (`map[string]any`,
  `float64`, etc.), since concrete type information isn't preserved across
  the wire. Supply a custom `Codec` (e.g. gob with registered types) if
  typed round-tripping matters.
- The KV bucket should be created with `History` greater than 1 so that
  `Update`'s revision check has something to compare against under
  contention (a `History` of 1 still works, but increases the chance of
  retries under heavy concurrent writes to the same key).
- `id` and encoded keys must satisfy NATS KV's key character rules;
  `New` validates `id` eagerly.
